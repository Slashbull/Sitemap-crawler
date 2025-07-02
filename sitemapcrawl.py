#!/usr/bin/env python3
"""
Keyword Sitemap Crawler — Smart Root‑Aware Edition + Turnover Extractor
=======================================================================
• Dropdown for **Piceapp** (sitemap crawl) or **Jamku** (GSTIN‑rewritten URLs).
• Handles **1 M+ pages** async with aiohttp.
• Root‑aware keyword mapping: `0813` captures all `0813xxxx`.
• Extracts "Slab" & "Turnover" text near matches from web page.
• Outputs **matched_root**, **slab**, and **turnover** in CSV.
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
import altair as alt

# ─────────── Config ───────────
DEFAULT_TIMEOUT = 20
DEFAULT_CONCURRENCY = 50
DEFAULT_BATCH_SIZE = 1000
REGEX_CHUNK_SIZE = 1000
GSTIN_PATTERN = re.compile(r"[0-9]{2}[A-Z0-9]{10}[0-9A-Z]{3}", re.IGNORECASE)

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
LOGGER = logging.getLogger("keyword-crawler")

# ─────────── Keyword mapping (root aware) ───────────
def build_keyword_mapping(raw_keywords: Sequence[str]) -> Tuple[List[str], Dict[str, str]]:
    cleaned = sorted({k.strip() for k in raw_keywords if k.strip()})
    roots = {k for k in cleaned if k.isdigit() and len(k) <= 4}
    mapping: Dict[str, str] = {}
    flat: List[str] = []
    for kw in cleaned:
        if kw.isdigit() and len(kw) == 4:
            mapping[kw] = kw
            flat.append(kw)
            family_regex = fr"\\b{kw}\\d{{4}}\\b"
            mapping[family_regex] = kw
            flat.append(family_regex)
        else:
            root = kw[:4] if kw[:4] in roots else kw
            mapping[kw] = root
            flat.append(kw)
    return flat, mapping

# ─────────── Regex compiler ───────────
def compile_patterns(tokens: Sequence[str]) -> List[re.Pattern]:
    return [re.compile("|".join(tokens[i:i + REGEX_CHUNK_SIZE]), re.IGNORECASE)
            for i in range(0, len(tokens), REGEX_CHUNK_SIZE)]

# ─────────── HTTP fetch with retries ───────────
async def fetch(session: aiohttp.ClientSession, url: str, retries=3, backoff=1.5) -> bytes:
    for i in range(retries):
        try:
            async with async_timeout.timeout(DEFAULT_TIMEOUT):
                async with session.get(url, ssl=False) as resp:
                    resp.raise_for_status()
                    return await resp.read()
        except Exception as exc:
            LOGGER.warning(f"[Retry {i+1}] {url} failed → {exc}")
            await asyncio.sleep(backoff * (2 ** i))
    raise Exception(f"Failed after {retries} retries → {url}")

# ─────────── Sitemap discovery ───────────
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

# ─────────── Jamku links ───────────
def to_jamku_urls(pice_urls: List[str]) -> List[str]:
    return [f"https://gst.jamku.app/gstin/{m.group(0).lower()}"
            for url in pice_urls if (m := GSTIN_PATTERN.search(url))]

# ─────────── Scan page content ───────────
def extract_slab_and_turnover(html: str) -> Tuple[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    slab_match = re.search(r"slab[^:\n]*[:\-–]\s*([\w\₹ ,%]*)", text, re.IGNORECASE)
    turnover_match = re.search(r"turnover[^:\n]*[:\-–]\s*([\w\₹ ,%]*)", text, re.IGNORECASE)
    slab = slab_match.group(1).strip() if slab_match else ""
    turnover = turnover_match.group(1).strip() if turnover_match else ""
    return slab, turnover

async def _page_scan(session: aiohttp.ClientSession, url: str, patterns: List[re.Pattern], sem: asyncio.Semaphore) -> Tuple[str, str, str, str, str] | None:
    async with sem:
        try:
            html = (await fetch(session, url)).decode("utf-8", errors="ignore")
        except Exception:
            return None
        m = next((pat.search(html) for pat in patterns if pat.search(html)), None)
        if m:
            soup = BeautifulSoup(html, "html.parser")
            title = (soup.title.string or "").strip() if soup.title else ""
            slab, turnover = extract_slab_and_turnover(html)
            return url, m.group(0), title, slab, turnover
        return None

# ─────────── Orchestrator ───────────
async def crawl(sitemaps: List[str], raw_keywords: List[str], mode: str, concurrency: int, batch_size: int) -> pd.DataFrame:
    flat_keywords, root_map = build_keyword_mapping(raw_keywords)
    patterns = compile_patterns(flat_keywords)
    sem = asyncio.Semaphore(concurrency)
    timeout = aiohttp.ClientTimeout(total=DEFAULT_TIMEOUT)
    connector = aiohttp.TCPConnector(limit=concurrency, force_close=True)
    results = []

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

    df = pd.DataFrame(results, columns=["url", "matched_keyword", "page_title", "slab", "turnover"])
    df["matched_root"] = df["matched_keyword"].apply(lambda k: root_map.get(k, k))
    return df[["url", "matched_keyword", "matched_root", "page_title", "slab", "turnover"]]

# ─────────── UI ───────────
st.set_page_config(page_title="Keyword Sitemap Crawler", layout="wide")
st.title("🔍 Keyword Sitemap Crawler + Turnover Extractor")

with st.form("crawl_form"):
    mode = st.selectbox("Mode", ["Piceapp", "Jamku"], index=0)
    sitemaps_txt = st.text_area("Sitemap URL(s)", height=120)
    keyword_file = st.file_uploader("Keyword list (TXT)", type=["txt"])
    col1, col2 = st.columns(2)
    concurrency = col1.slider("Concurrency", 10, 500, DEFAULT_CONCURRENCY, 10)
    batch_size = col2.slider("Batch size", 200, 5000, DEFAULT_BATCH_SIZE, 200)
    submitted = st.form_submit_button("🚀 Crawl")

if submitted:
    if not sitemaps_txt or not keyword_file:
        st.error("Please provide both sitemap URLs and a keyword file.")
        st.stop()

    sitemaps = [l.strip() for l in sitemaps_txt.splitlines() if l.strip()]
    raw_kw = keyword_file.read().decode("utf-8", errors="ignore").splitlines()
    st.write("Loaded", len(raw_kw), "keywords (including masters & children)")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        df = loop.run_until_complete(crawl(sitemaps, raw_kw, mode, concurrency, batch_size))
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    st.download_button("📥 Download CSV", csv_bytes, "matched_urls.csv", "text/csv")

    if df.empty:
        st.warning("⚠️ No keyword matches found.")
    else:
        st.dataframe(df, use_container_width=True)
        count_by_root = df["matched_root"].value_counts().reset_index()
        count_by_root.columns = ["matched_root", "count"]
        st.altair_chart(
            alt.Chart(count_by_root).mark_bar().encode(
                x="matched_root:N", y="count:Q", tooltip=["matched_root", "count"]
            ).properties(height=400),
            use_container_width=True
        )
