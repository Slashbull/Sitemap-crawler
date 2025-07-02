#!/usr/bin/env python3
"""
Streamlit App: Keyword Sitemap Crawler (Jamku & Piceapp Mode)
=============================================================
- Supports dropdown to choose between Piceapp (sitemap crawl) or Jamku (GST-based URL generation).
- Designed for massive scale (1 million+ URLs).
- Fully asynchronous with keyword matching.
"""

import asyncio, gzip, logging, re, time
from io import BytesIO
from typing import List, Sequence, Set, Tuple

import aiohttp, async_timeout
import pandas as pd
import streamlit as st
from bs4 import BeautifulSoup
from lxml import etree
from more_itertools import chunked

# ──────────────── Config ────────────────
DEFAULT_TIMEOUT = 20
DEFAULT_CONCURRENCY = 50
DEFAULT_BATCH_SIZE = 1000
REGEX_CHUNK_SIZE = 1000

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
LOGGER = logging.getLogger("keyword-crawler")

# ───────────── Regex Chunking ─────────────
def compile_patterns(keywords: Sequence[str], chunk_size: int = REGEX_CHUNK_SIZE) -> List[re.Pattern]:
    cleaned = [k.strip() for k in keywords if k.strip()]
    return [re.compile("|".join(map(re.escape, cleaned[i:i+chunk_size])), re.IGNORECASE)
            for i in range(0, len(cleaned), chunk_size)]

# ───────────── HTTP Fetch ─────────────
async def fetch(session: aiohttp.ClientSession, url: str) -> bytes:
    try:
        async with async_timeout.timeout(DEFAULT_TIMEOUT):
            async with session.get(url, ssl=False) as resp:
                resp.raise_for_status()
                return await resp.read()
    except Exception as exc:
        LOGGER.warning("Fetch failed %s → %s", url, exc)
        raise

# ───────────── Sitemap Crawl (Piceapp) ─────────────
async def _locs_from_sitemap(session: aiohttp.ClientSession, url: str) -> List[str]:
    try:
        raw = await fetch(session, url)
        if url.endswith(".gz"):
            raw = gzip.decompress(raw)
        tree = etree.parse(BytesIO(raw))
        root = etree.QName(tree.getroot()).localname
        return [loc.text.strip() for loc in tree.findall(".//{*}loc")] if root in {"sitemapindex", "urlset"} else []
    except Exception as exc:
        LOGGER.warning("Parse failed %s → %s", url, exc)
        return []

async def discover_urls(session: aiohttp.ClientSession, sitemaps: List[str]) -> List[str]:
    queue, seen, pages = list(dict.fromkeys(sitemaps)), set(), []
    prog = st.progress(0.0, text="🔍 Discovering URLs …")
    processed = 0

    while queue:
        sitemap = queue.pop(0)
        if sitemap in seen: continue
        seen.add(sitemap)
        children = await _locs_from_sitemap(session, sitemap)
        if children and children[0].lower().endswith((".xml", ".gz")):
            queue.extend(u for u in children if u not in seen)
        else:
            pages.extend(children)
        processed += 1
        prog.progress(processed / (processed + len(queue) + 1e-9))
    prog.empty()
    return list(dict.fromkeys(pages))

# ───────────── Jamku Mode URL Generation ─────────────
def extract_gstin_from_pice_urls(pice_urls: List[str]) -> List[str]:
    gstins = [u.rstrip("/").split("-")[-1].lower() for u in pice_urls if "-" in u and len(u.split("-")[-1]) == 15]
    return [f"https://gst.jamku.app/gstin/{g}" for g in gstins]

# ───────────── Keyword Match in HTML ─────────────
async def _page_contains_keyword(session: aiohttp.ClientSession, url: str, patterns: List[re.Pattern], sem: asyncio.Semaphore) -> Tuple[str, str, str] | None:
    async with sem:
        try:
            html = (await fetch(session, url)).decode("utf-8", errors="ignore")
        except Exception:
            return None
        for pattern in patterns:
            m = pattern.search(html)
            if m:
                soup = BeautifulSoup(html, "html.parser")
                title = (soup.title.string or "").strip() if soup.title else ""
                return url, m.group(0), title
        return None

# ───────────── Full Crawl Orchestration ─────────────
async def crawl(sitemaps: List[str], keywords: List[str], mode: str, concurrency: int, batch_size: int) -> pd.DataFrame:
    patterns = compile_patterns(keywords)
    sem = asyncio.Semaphore(concurrency)
    timeout = aiohttp.ClientTimeout(total=DEFAULT_TIMEOUT)
    connector = aiohttp.TCPConnector(limit=concurrency)
    results: List[Tuple[str, str, str]] = []

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        if mode == "Piceapp":
            pages = await discover_urls(session, sitemaps)
        else:  # Jamku mode
            raw_urls = await discover_urls(session, sitemaps)
            pages = extract_gstin_from_pice_urls(raw_urls)

        st.info(f"🌐 Scanning {len(pages):,} pages")
        scanned = 0
        prog = st.progress(0.0, text="🔍 Scanning pages …")
        start = time.perf_counter()

        for batch in chunked(pages, batch_size):
            tasks = [_page_contains_keyword(session, url, patterns, sem) for url in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            for hit in batch_results:
                scanned += 1
                if isinstance(hit, tuple):
                    results.append(hit)
            prog.progress(min(1.0, scanned / len(pages)))

        prog.empty()
        st.success(f"✅ Done in {time.perf_counter() - start:.1f}s with {len(results):,} matches")
    return pd.DataFrame(results, columns=["url", "matched_keyword", "page_title"])

# ───────────── Streamlit UI ─────────────
st.set_page_config(page_title="Keyword Sitemap Crawler", layout="wide")
st.title("🔍 Keyword Sitemap Crawler – Jamku + Piceapp")
st.caption("Massive keyword matcher. Choose your source.")

with st.form("crawl_form"):
    mode = st.selectbox("Choose Mode", ["Piceapp", "Jamku"], index=0)
    sitemap_input = st.text_area("🌐 Enter Sitemap URL(s)", height=100)
    keyword_file = st.file_uploader("📄 Upload Keyword List (TXT)", type=["txt"])
    concurrency = st.slider("⚙️ Concurrency", 10, 500, DEFAULT_CONCURRENCY, 10)
    batch_size = st.slider("📦 Batch Size", 200, 5000, DEFAULT_BATCH_SIZE, 200)
    submitted = st.form_submit_button("🚀 Start Crawling")

if submitted:
    if not sitemap_input or not keyword_file:
        st.error("⚠️ Please provide sitemap(s) and a keyword file.")
        st.stop()

    sitemaps = [u.strip() for u in sitemap_input.strip().splitlines() if u.strip()]
    keywords = [k.strip() for k in keyword_file.read().decode("utf-8").splitlines() if k.strip()]

    st.write("**🔑 Keywords Loaded:**", ", ".join(keywords[:20]) + (" …" if len(keywords) > 20 else ""))

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        df = loop.run_until_complete(crawl(sitemaps, keywords, mode, concurrency, batch_size))
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    st.download_button("📥 Download CSV", csv_bytes, "matched_urls.csv", "text/csv")

    if df.empty:
        st.warning("⚠️ No keyword matches found.")
    else:
        st.dataframe(df, use_container_width=True)
