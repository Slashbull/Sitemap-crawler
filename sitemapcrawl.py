#!/usr/bin/env python3
"""
Keyword Sitemap Crawler – Streamlit App
======================================
All‑async, fault‑tolerant crawler that scans **millions of URLs** for any of your
keywords (HS‑codes, SKUs, phrases …) and outputs a CSV.

Built for Streamlit Community Cloud, but will happily run anywhere.

Main upgrades over the previous version
--------------------------------------
* **Scales to 1 000 000+ URLs** – memory‑light: only matched pages are kept.
* **Regex‑chunking** – avoids the "pattern too large" error with thousands of
  keywords by splitting them into manageable 1 000‑keyword groups.
* **100 % task completion** using `asyncio.gather(return_exceptions=True)`.
* **CSV button always shown** – even if no matches were found (empty file
  included).
* **Progress bars** tuned for huge datasets (no int‑overflow, fast updates).
* **Config sliders** widened (concurrency up to 500, batch size up to 5 000).
* **Clean loop shutdown** – zero "Task was destroyed" warnings.

Install requirements
--------------------
```bash
pip install streamlit aiohttp async-timeout lxml beautifulsoup4 tqdm pandas more_itertools tenacity
```
If you deploy to Streamlit Cloud, put those lines in `requirements.txt`.
"""
from __future__ import annotations

import asyncio
import gzip
import logging
import re
import time
from io import BytesIO
from typing import Iterable, List, Sequence, Set, Tuple

import aiohttp
import async_timeout
import pandas as pd
import streamlit as st
from bs4 import BeautifulSoup
from lxml import etree
from more_itertools import chunked

# ────────────────────────────── Config & Logging ──────────────────────────────
DEFAULT_TIMEOUT = 20            # seconds per HTTP request (tweak if needed)
DEFAULT_CONCURRENCY = 50        # default max simultaneous requests (slider up to 500)
DEFAULT_BATCH_SIZE = 1000       # URLs processed in one async batch (slider up to 5 000)
REGEX_CHUNK_SIZE = 1000         # max keywords per compiled regex – prevents HUGE patterns

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
LOGGER = logging.getLogger("keyword‑crawler")

# ────────────────────────────── Utility: regex chunker ─────────────────────────

def compile_patterns(keywords: Sequence[str], chunk_size: int = REGEX_CHUNK_SIZE) -> List[re.Pattern]:
    """Return a list of regex patterns, each containing ≤ *chunk_size* keywords."""
    cleaned = [k for k in keywords if k]
    patterns: List[re.Pattern] = []
    for i in range(0, len(cleaned), chunk_size):
        group = "|".join(map(re.escape, cleaned[i : i + chunk_size]))
        patterns.append(re.compile(group, re.IGNORECASE))
    return patterns

# ────────────────────────────── Async HTTP Fetch ──────────────────────────────
async def fetch(session: aiohttp.ClientSession, url: str) -> bytes:
    """GET *url* returning raw bytes, with DEFAULT_TIMEOUT and SSL verification disabled."""
    try:
        async with async_timeout.timeout(DEFAULT_TIMEOUT):
            async with session.get(url, ssl=False) as resp:
                resp.raise_for_status()
                return await resp.read()
    except Exception as exc:
        LOGGER.warning("Fetch failed %s → %s", url, exc)
        raise

# ───────────── Recursive discovery of <loc> children in sitemap(‑index) ─────────
async def _locs_from_sitemap(session: aiohttp.ClientSession, url: str) -> List[str]:
    """Return every <loc> text inside *url* (handles .xml and .gz)."""
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

async def discover_urls(session: aiohttp.ClientSession, start_sitemaps: Iterable[str]) -> List[str]:
    """Breadth‑first crawl of *start_sitemaps* until every child URL is found."""
    queue: List[str] = list(dict.fromkeys(start_sitemaps))
    seen: Set[str] = set()
    pages: List[str] = []

    prog = st.progress(0.0, text="🔍 Discovering URLs …")
    processed = 0

    while queue:
        sitemap = queue.pop(0)
        if sitemap in seen:
            continue
        seen.add(sitemap)
        child_locs = await _locs_from_sitemap(session, sitemap)
        if child_locs and child_locs[0].lower().endswith((".xml", ".gz")):
            queue.extend(u for u in child_locs if u not in seen)
        else:
            pages.extend(child_locs)
        processed += 1
        prog.progress(processed / (processed + len(queue) + 1e-9))
    prog.empty()
    return list(dict.fromkeys(pages))  # de‑duplicate, preserve order

# ─────────────── Keyword scan (HTML contains any of the patterns) ──────────────
async def _page_contains_keyword(
    session: aiohttp.ClientSession,
    url: str,
    patterns: Sequence[re.Pattern],
    sem: asyncio.Semaphore,
) -> Tuple[str, str, str] | None:
    async with sem:
        try:
            html = (await fetch(session, url)).decode("utf-8", errors="ignore")
        except Exception:
            return None

        for pattern in patterns:  # loop through pattern chunks, break fast on first match
            m = pattern.search(html)
            if m:
                soup = BeautifulSoup(html, "html.parser")
                title = (soup.title.string or "").strip() if soup.title else ""
                return url, m.group(0), title
        return None

async def crawl(
    sitemaps: List[str],
    keywords: List[str],
    concurrency: int,
    batch_size: int,
    domain_filter: str | None = None,
):
    """Discover URLs → scan in batches → return DataFrame of matches."""
    patterns = compile_patterns(keywords)
    sem = asyncio.Semaphore(concurrency)
    timeout = aiohttp.ClientTimeout(total=DEFAULT_TIMEOUT)
    connector = aiohttp.TCPConnector(limit=concurrency)
    results: list[tuple[str, str, str]] = []

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        pages = await discover_urls(session, sitemaps)
        if domain_filter:
            pages = [u for u in pages if domain_filter in u]
        st.info(f"🌐 Discovered {len(pages):,} page URLs to scan")

        scanned = 0
        prog = st.progress(0.0, text="🔍 Scanning pages …")
        start = time.perf_counter()

        for page_batch in chunked(pages, batch_size):
            tasks = [
                _page_contains_keyword(session, url, patterns, sem) for url in page_batch
            ]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            # Update counters & collect matches
            for hit in batch_results:
                scanned += 1
                if isinstance(hit, tuple):
                    results.append(hit)
            prog.progress(min(1.0, scanned / len(pages)))

        prog.empty()
        st.success(f"✅ Completed in {time.perf_counter() - start:.1f}s – {len(results):,} matches")

    return pd.DataFrame(results, columns=["url", "matched_keyword", "page_title"])

# ────────────────────────────── Streamlit UI ──────────────────────────────
st.set_page_config(page_title="Keyword Sitemap Crawler", layout="wide")
st.title("🔍 Keyword Sitemap Crawler – Million‑URL Edition")
st.caption("Find every page containing your keywords – massively parallel, no crashes.")

with st.form("crawl_form"):
    col1, col2 = st.columns(2)
    with col1:
        sitemap_input = st.text_area("🌐 Sitemap URLs (one per line)")
        domain_filter = st.text_input("🔗 Optional domain filter (e.g. example.com)")
    with col2:
        keyword_file = st.file_uploader("📄 Upload Keyword List (one per line)", type=["txt"])
        concurrency = st.slider("⚙️ Concurrent HTTP Requests", 10, 500, DEFAULT_CONCURRENCY, 10)
        batch_size = st.slider("📦 Batch size", 200, 5000, DEFAULT_BATCH_SIZE, 200)
    submitted = st.form_submit_button("🚀 Start Crawling")

if submitted:
    if not sitemap_input or not keyword_file:
        st.error("Please provide at least one sitemap URL and a keyword file.")
        st.stop()

    sitemaps = [u.strip() for u in sitemap_input.splitlines() if u.strip()]
    raw_keywords = keyword_file.read().decode("utf-8").splitlines()
    keywords = [k.strip() for k in raw_keywords if k.strip()]

    st.write(
        "**🔑 Keywords uploaded:** ",
        ", ".join(keywords[:20]) + (" …" if len(keywords) > 20 else ""),
    )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        df = loop.run_until_complete(
            crawl(sitemaps, keywords, concurrency, batch_size, domain_filter or None)
        )
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()

    # ────────────────────── Results & CSV download ──────────────────────
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    st.download_button("📥 Download CSV", csv_bytes, "matched_urls.csv", "text/csv")

    if df.empty:
        st.warning("No matches found!")
    else:
        st.dataframe(df, use_container_width=True)
