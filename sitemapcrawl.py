#!/usr/bin/env python3
"""
Keyword Sitemap Crawler — All‑Time Best Edition
==============================================
* Async crawler for **Piceapp** sitemap or **Jamku** GSTIN mode.
* Smart HS‑code keyword matching with root‑family detection (e.g. 0813 ⇒ 0813xxxx).
* Fuzzy extraction of “Slab” and “Turnover” text.
* DNS‑cached connector, exponential‑backoff retries.
* Streamlit UI + Altair chart summarising matches.
"""

from __future__ import annotations
import asyncio, gzip, logging, re, time
from io import BytesIO
from typing import Dict, List, Sequence, Tuple

import aiohttp, async_timeout
import pandas as pd
import streamlit as st
from bs4 import BeautifulSoup
from lxml import etree
from more_itertools import chunked
import altair as alt

# ─────────── Config ───────────
DEFAULT_TIMEOUT      = 20
DEFAULT_CONCURRENCY  = 50
DEFAULT_BATCH_SIZE   = 1000
REGEX_CHUNK_SIZE     = 1000
DNS_CACHE_TTL        = 300
GSTIN_PATTERN        = re.compile(r"[0-9]{2}[A-Z0-9]{10}[0-9A-Z]{3}", re.IGNORECASE)

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
LOGGER = logging.getLogger("crawler")

# ─────────── Keyword utils ───────────
def validate_keywords(keywords: Sequence[str]) -> List[str]:
    return [k for k in keywords if re.fullmatch(r"[\w\- ]{2,}", k.strip())]

def build_keyword_mapping(raw: Sequence[str]) -> Tuple[List[str], Dict[str, str]]:
    clean  = sorted({k.strip() for k in validate_keywords(raw)})
    roots  = {k for k in clean if k.isdigit() and len(k) <= 4}
    flat:  List[str] = []
    mapping: Dict[str, str] = {}
    for kw in clean:
        if kw.isdigit() and len(kw) == 4:            # master root, e.g. 0813
            mapping[kw] = kw
            flat.append(kw)
            regex = fr"\b{kw}\d{{4}}\b"              # matches 0813xxxx
            mapping[regex] = kw
            flat.append(regex)
        else:                                        # full 6‑/8‑digit codes
            root = kw[:4] if kw[:4] in roots else kw
            mapping[kw] = root
            flat.append(kw)
    return flat, mapping

def compile_patterns(tokens: Sequence[str]) -> List[re.Pattern]:
    return [
        re.compile("|".join(tokens[i:i + REGEX_CHUNK_SIZE]), re.IGNORECASE)
        for i in range(0, len(tokens), REGEX_CHUNK_SIZE)
    ]

# ─────────── Async helpers ───────────
async def fetch(session: aiohttp.ClientSession, url: str, retries=3, backoff=1.5) -> bytes:
    for i in range(retries):
        try:
            async with async_timeout.timeout(DEFAULT_TIMEOUT):
                async with session.get(url, ssl=False) as resp:
                    resp.raise_for_status()
                    return await resp.read()
        except Exception as exc:
            LOGGER.warning("[Retry %s/%s] %s → %s", i + 1, retries, url, exc)
            await asyncio.sleep(backoff * (2 ** i))
    raise RuntimeError(f"Failed after {retries} retries: {url}")

# ─────────── Sitemap discovery ───────────
async def _locs_from_sitemap(session: aiohttp.ClientSession, url: str) -> List[str]:
    try:
        raw = await fetch(session, url)
        if url.endswith(".gz"):
            raw = gzip.decompress(raw)
        tree = etree.parse(BytesIO(raw))
        root = etree.QName(tree.getroot()).localname
        return [loc.text.strip() for loc in tree.findall(".//{*}loc")] if root in {"urlset", "sitemapindex"} else []
    except Exception as exc:
        LOGGER.warning("Parse failed %s → %s", url, exc)
        return []

async def discover_urls(session: aiohttp.ClientSession, sitemaps: List[str]) -> List[str]:
    queue, seen, pages = list(dict.fromkeys(sitemaps)), set(), []
    prog = st.progress(0.0, text="🔍 Discovering URLs …")
    done = 0
    while queue:
        sm = queue.pop(0)
        if sm in seen:
            continue
        seen.add(sm)
        locs = await _locs_from_sitemap(session, sm)
        if locs and locs[0].lower().endswith((".xml", ".gz")):
            queue.extend(u for u in locs if u not in seen)  # recurse into sitemap‑index
        else:
            pages.extend(locs)
        done += 1
        prog.progress(done / (done + len(queue) + 1e-9))
    prog.empty()
    return list(dict.fromkeys(pages))

# ─────────── Jamku mode helper ───────────
def to_jamku_urls(pice_urls: List[str]) -> List[str]:
    return [f"https://gst.jamku.app/gstin/{m.group(0).lower()}" for u in pice_urls if (m := GSTIN_PATTERN.search(u))]

# ─────────── Slab & Turnover extraction ───────────
def extract_slab_turnover(html: str) -> Tuple[str, str]:
    text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
    slab  = re.search(r"slab[^:\n]*[:\-–]\s*([\w₹ ,%]*)", text, re.I)
    turn  = re.search(r"turnover[^:\n]*[:\-–]\s*([\w₹ ,%]*)", text, re.I)
    return slab.group(1).strip() if slab else "", turn.group(1).strip() if turn else ""

async def _scan_page(session: aiohttp.ClientSession, url: str, pats: List[re.Pattern], sem: asyncio.Semaphore):
    async with sem:
        try:
            html = (await fetch(session, url)).decode("utf-8", errors="ignore")
        except Exception:
            return None
        match = next((m for p in pats if (m := p.search(html))), None)
        if match:
            soup = BeautifulSoup(html, "html.parser")
            title = (soup.title.string or "").strip() if soup.title else ""
            slab, turn = extract_slab_turnover(html)
            return url, match.group(0), title, slab, turn
        return None

# ─────────── Crawl orchestrator ───────────
async def crawl(sitemaps: List[str], raw_kw: List[str], mode: str,
                concurrency: int, batch_size: int) -> pd.DataFrame:

    tokens, root_map = build_keyword_mapping(raw_kw)
    patterns = compile_patterns(tokens)

    sem = asyncio.Semaphore(concurrency)
    conn = aiohttp.TCPConnector(
        limit=concurrency,
        limit_per_host=max(1, concurrency // 2),
        ttl_dns_cache=DNS_CACHE_TTL
    )
    timeout = aiohttp.ClientTimeout(total=DEFAULT_TIMEOUT)

    results: List[Tuple[str, str, str, str, str]] = []
    async with aiohttp.ClientSession(timeout=timeout, connector=conn) as session:
        base_pages = await discover_urls(session, sitemaps)
        pages      = base_pages if mode == "Piceapp" else to_jamku_urls(base_pages)
        st.info(f"🌐 Pages queued: {len(pages):,}")

        start, scanned = time.perf_counter(), 0
        prog = st.progress(0.0, text="🔎 Scanning …")

        for batch in chunked(pages, batch_size):
            tasks = [_scan_page(session, u, patterns, sem) for u in batch]
            for res in await asyncio.gather(*tasks, return_exceptions=True):
                scanned += 1
                if isinstance(res, tuple):
                    results.append(res)
            prog.progress(scanned / len(pages))

        prog.empty()
        st.success(f"✅ Completed in {time.perf_counter() - start:.1f}s — {len(results):,} matches")

    df = pd.DataFrame(results, columns=["url", "matched_keyword", "page_title", "slab", "turnover"])
    df["matched_root"] = df["matched_keyword"].map(root_map).fillna(df["matched_keyword"])
    return df[["url", "matched_keyword", "matched_root", "page_title", "slab", "turnover"]]

# ─────────── Streamlit UI ───────────
st.set_page_config(page_title="Keyword Sitemap Crawler", layout="wide")
st.title("🔍 Keyword Sitemap Crawler — All‑Time Best 🌟")

with st.form("crawl_form"):
    mode            = st.selectbox("Site mode", ["Piceapp", "Jamku"], index=0)
    sitemaps_txt    = st.text_area("Sitemap URL(s) (one per line)", height=120)
    keyword_file    = st.file_uploader("Keyword list (TXT)", type=["txt"])
    cols            = st.columns(2)
    concurrency     = cols[0].slider("Concurrency", 10, 500, DEFAULT_CONCURRENCY, 10)
    batch_size      = cols[1].slider("Batch size", 200, 5000, DEFAULT_BATCH_SIZE, 200)
    submitted       = st.form_submit_button("🚀 Start Crawling")

if submitted:
    if not sitemaps_txt or not keyword_file:
        st.error("❌ Both sitemap and keyword file are required.")
        st.stop()

    sitemaps = [l.strip() for l in sitemaps_txt.splitlines() if l.strip()]
    raw_kw   = keyword_file.read().decode("utf-8", errors="ignore").splitlines()
    st.write(f"📥 Loaded {len(raw_kw)} keywords")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        df = loop.run_until_complete(
            crawl(sitemaps, raw_kw, mode, concurrency, batch_size)
        )
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()

    if df.empty:
        st.warning("⚠️ No matches found.")
    else:
        st.dataframe(df, use_container_width=True)
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("📦 Download CSV", csv, "matches.csv", "text/csv")

        chart = (
            alt.Chart(
                df["matched_root"].value_counts()
                  .reset_index(names=["matched_root", "count"])
            )
            .mark_bar()
            .encode(
                x="matched_root:N", y="count:Q",
                tooltip=["matched_root", "count"]
            )
            .properties(title="Keyword Match Count by Root", height=400)
        )

        st.altair_chart(chart, use_container_width=True)
