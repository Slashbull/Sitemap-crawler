#!/usr/bin/env python3
"""
Keyword Sitemap Crawler — Smart Root‑Aware Edition
==================================================
• Dropdown for **Piceapp** (direct sitemap crawl) or **Jamku** (GSTIN‑rewritten URLs).  
• Handles **1 M+ pages** with fully‑async aiohttp.  
• **Root‑aware keyword mapping**: any 4‑digit numeric “master” (e.g. `0813`) captures all 8‑digit codes that start with it (e.g. `08131000`).  
• Outputs **matched_root** column so you can aggregate hits at the HS‑chapter level.  
• Regex chunking prevents huge‑pattern errors.  
• CSV always downloadable.
"""

from __future__ import annotations

import asyncio, gzip, logging, re, time
from collections import defaultdict
from io import BytesIO
from typing import Dict, List, Sequence, Set, Tuple

import aiohttp, async_timeout
import pandas as pd
import streamlit as st
from bs4 import BeautifulSoup
from lxml import etree
from more_itertools import chunked

# ───────────────────────── Config ─────────────────────────
DEFAULT_TIMEOUT      = 20
DEFAULT_CONCURRENCY  = 50
DEFAULT_BATCH_SIZE   = 1000
REGEX_CHUNK_SIZE     = 1000
GSTIN_PATTERN        = re.compile(r"[0-9]{2}[A-Z0-9]{10}[0-9A-Z]{3}", re.IGNORECASE)

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
LOGGER = logging.getLogger("keyword-crawler")

# ──────────── Keyword preprocessing (root mapping) ────────────

def build_keyword_mapping(raw_keywords: Sequence[str]) -> Tuple[List[str], Dict[str, str]]:
    """Detect masters (≤4 digit numbers) and map every keyword to its root."""
    cleaned = sorted({k.strip() for k in raw_keywords if k.strip()})
    roots   = {k for k in cleaned if k.isdigit() and len(k) <= 4}

    mapping: Dict[str, str] = {}
    flat: List[str] = []

    for kw in cleaned:
        if kw.isdigit() and len(kw) == 4:
            # Treat as master but still match it exactly
            mapping[kw] = kw
            flat.append(kw)
            # Add regex for its 8‑digit family
            family_regex = fr"\b{kw}\d{{4}}\b"  # 0813xxxx
            mapping[family_regex] = kw  # map regex token -> root
            flat.append(family_regex)
        else:
            # Long code: derive root as first 4 digits if present in roots
            root = kw[:4] if kw[:4] in roots else kw
            mapping[kw] = root
            flat.append(kw)
    return flat, mapping

# ──────────── Regex chunking ────────────

def compile_patterns(tokens: Sequence[str]) -> List[re.Pattern]:
    patterns = []
    for i in range(0, len(tokens), REGEX_CHUNK_SIZE):
        chunk = tokens[i:i + REGEX_CHUNK_SIZE]
        # Tokens may already contain \b / \d – keep raw
        pattern_src = "|".join(chunk)
        patterns.append(re.compile(pattern_src, re.IGNORECASE))
    return patterns

# ──────────── Async fetch ────────────
async def fetch(session: aiohttp.ClientSession, url: str) -> bytes:
    try:
        async with async_timeout.timeout(DEFAULT_TIMEOUT):
            async with session.get(url, ssl=False) as resp:
                resp.raise_for_status()
                return await resp.read()
    except Exception as exc:
        LOGGER.warning("Fetch failed %s → %s", url, exc)
        raise

# ──────────── Sitemap discovery ────────────
async def _locs_from_sitemap(session: aiohttp.ClientSession, url: str) -> List[str]:
    try:
        raw = await fetch(session, url)
        if url.endswith(".gz"):
            raw = gzip.decompress(raw)
        tree = etree.parse(BytesIO(raw))
        root_name = etree.QName(tree.getroot()).localname
        return [loc.text.strip() for loc in tree.findall(".//{*}loc")] if root_name in {"urlset", "sitemapindex"} else []
    except Exception as exc:
        LOGGER.warning("Parse failed %s → %s", url, exc)
        return []

async def discover_urls(session: aiohttp.ClientSession, sitemaps: List[str]) -> List[str]:
    queue, seen, pages = list(dict.fromkeys(sitemaps)), set(), []
    prog = st.progress(0.0, text="Discovering URLs…")
    done = 0
    while queue:
        sm = queue.pop(0)
        if sm in seen:
            continue
        seen.add(sm)
        locs = await _locs_from_sitemap(session, sm)
        if locs and locs[0].lower().endswith((".xml", ".gz")):
            queue.extend(u for u in locs if u not in seen)
        else:
            pages.extend(locs)
        done += 1
        prog.progress(done / (done + len(queue) + 1e-9))
    prog.empty()
    return list(dict.fromkeys(pages))

# ──────────── Jamku URL builder ────────────

def to_jamku_urls(pice_urls: List[str]) -> List[str]:
    jamku_urls = []
    for url in pice_urls:
        m = GSTIN_PATTERN.search(url)
        if m:
            jamku_urls.append(f"https://gst.jamku.app/gstin/{m.group(0).lower()}")
    return list(dict.fromkeys(jamku_urls))

# ──────────── HTML keyword scan ────────────
async def _page_scan(session: aiohttp.ClientSession, url: str, patterns: List[re.Pattern], sem: asyncio.Semaphore) -> Tuple[str, str, str] | None:
    async with sem:
        try:
            html = (await fetch(session, url)).decode("utf-8", errors="ignore")
        except Exception:
            return None
        for pat in patterns:
            m = pat.search(html)
            if m:
                title = ""
                try:
                    title = (BeautifulSoup(html, "html.parser").title.string or "").strip()
                except Exception:
                    pass
                return url, m.group(0), title
        return None

# ──────────── Crawl orchestrator ────────────
async def crawl(sitemaps: List[str], raw_keywords: List[str], mode: str, concurrency: int, batch_size: int) -> pd.DataFrame:
    flat_keywords, root_map = build_keyword_mapping(raw_keywords)
    patterns = compile_patterns(flat_keywords)

    sem = asyncio.Semaphore(concurrency)
    timeout = aiohttp.ClientTimeout(total=DEFAULT_TIMEOUT)
    connector = aiohttp.TCPConnector(limit=concurrency)
    results: List[Tuple[str, str, str]] = []

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        base_pages = await discover_urls(session, sitemaps)
        pages = base_pages if mode == "Piceapp" else to_jamku_urls(base_pages)

        st.info(f"🌐 Total pages queued: {len(pages):,}")
        scanned = 0
        prog = st.progress(0.0, text="Scanning pages…")
        start = time.perf_counter()

        for batch in chunked(pages, batch_size):
            tasks = [_page_scan(session, url, patterns, sem) for url in batch]
            for res in await asyncio.gather(*tasks, return_exceptions=True):
                scanned += 1
                if isinstance(res, tuple):
                    results.append(res)
            prog.progress(min(1.0, scanned / len(pages)))

        prog.empty()
        st.success(f"✅ Finished in {time.perf_counter() - start:.1f}s – {len(results):,} matches")

    df = pd.DataFrame(results, columns=["url", "matched_keyword", "page_title"])
    df["matched_root"] = df["matched_keyword"].apply(lambda k: root_map.get(k, k))
    return df[["url", "matched_keyword", "matched_root", "page_title"]]

# ──────────── Streamlit UI ────────────
st.set_page_config(page_title="Keyword Sitemap Crawler", layout="wide")
st.title("🔍 Keyword Sitemap Crawler (Root‑Aware)")

with st.form("crawl_form"):
    mode         = st.selectbox("Mode", ["Piceapp", "Jamku"], index=0)
    sitemaps_txt = st.text_area("Sitemap URL(s)", height=120)
    keyword_file = st.file_uploader("Keyword list (TXT)", type=["txt"])
    col1, col2 = st.columns(2)
    concurrency = col1.slider("Concurrency", 10, 500, DEFAULT_CONCURRENCY, 10)
    batch_size  = col2.slider("Batch size", 200, 5000, DEFAULT_BATCH_SIZE, 200)
    submitted   = st.form_submit_button("🚀 Crawl")

if submitted:
    if not sitemaps_txt or not keyword_file:
        st.error("Please provide both sitemap URLs and a keyword file.")
        st.stop()

    sitemaps = [l.strip() for l in sitemaps_txt.splitlines() if l.strip()]
    raw_kw   = keyword_file.read().decode("utf-8", errors="ignore").splitlines()

    st.write("Loaded", len(raw_kw), "keywords (including masters & children)")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        df = loop.run_until_complete(crawl(sitemaps, raw_kw, mode, concurrency, batch_size))
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()

    st.download
