#!/usr/bin/env python3
"""
Keyword Sitemap Crawler – Piceapp / Jamku Edition
================================================
Streamlit app that can either:
1. Crawl **Piceapp** sitemaps directly and scan every page for keywords.
2. Take the **same Piceapp sitemap**, extract the GSTIN from each URL, build the
   corresponding **Jamku** URL (``https://gst.jamku.app/gstin/<GSTIN>``) and scan
   *those* pages for the keywords.

Highlights
----------
* Handles **1 M+ pages** with asyncio, aiohttp and batching.
* Works on Streamlit Cloud free tier.
* **Dropdown** lets the user pick *Piceapp* or *Jamku* mode.
* "Master" 4‑digit HS codes (e.g. ``0813``) automatically expand to their full
  8‑digit family (e.g. ``0813\d{4}``).
* Regex chunking prevents the "pattern too large" error when thousands of
  keywords are supplied.
* CSV always downloadable – even if no matches are found (empty file).

Install
-------
```bash
pip install streamlit aiohttp async-timeout lxml beautifulsoup4 tqdm pandas more_itertools tenacity
```
Add those lines to ``requirements.txt`` if deploying to Streamlit Cloud.
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
DEFAULT_TIMEOUT = 20            # seconds per HTTP request
DEFAULT_CONCURRENCY = 50        # default concurrency (slider up to 500)
DEFAULT_BATCH_SIZE = 1000       # URLs processed per async batch (slider up to 5 000)
REGEX_CHUNK_SIZE = 1000         # max keywords per compiled regex
GSTIN_RE = re.compile(r"[0-9]{2}[A-Z0-9]{10}[0-9A-Z]{3}", re.IGNORECASE)  # 15‑char GSTIN

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
LOGGER = logging.getLogger("keyword‑crawler")

# ────────────────────────────── Pattern helper ───────────────────────────────

def expand_master_keywords(keywords: Sequence[str]) -> List[str]:
    """Return list with 4‑digit numeric masters expanded to 8‑digit regex family."""
    expanded: List[str] = []
    for kw in keywords:
        kw = kw.strip()
        if not kw:
            continue
        if kw.isdigit() and len(kw) == 4:  # master code e.g. 0813
            expanded.append(fr"\b{kw}\d{{4}}\b")  # 0813xxxx
        expanded.append(kw)
    return expanded


def compile_patterns(keywords: Sequence[str], *, chunk_size: int = REGEX_CHUNK_SIZE) -> List[re.Pattern]:
    """Split *keywords* into chunks and compile each to a regex."""
    clean = expand_master_keywords(keywords)
    patterns: List[re.Pattern] = []
    for i in range(0, len(clean), chunk_size):
        group = "|".join(map(re.escape, clean[i : i + chunk_size]))
        # If we already injected explicit regex (\b0813\d{4}\b) don't escape
        group = group.replace(r"\\b", r"\b").replace(r"\\d", r"\d")
        patterns.append(re.compile(group, re.IGNORECASE))
    return patterns

# ────────────────────────────── Async HTTP Fetch ──────────────────────────────
async def fetch(session: aiohttp.ClientSession, url: str) -> bytes:
    """GET *url* with timeout and SSL disabled."""
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
    return list(dict.fromkeys(pages))

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
        for pattern in patterns:
            m = pattern.search(html)
            if m:
                soup = BeautifulSoup(html, "html.parser")
                title = (soup.title.string or "").strip() if soup.title else ""
                return url, m.group(0), title
        return None

async def crawl(
    sitemaps: List[str],
    keywords: List[str],
    mode: str,  # "Piceapp" or "Jamku"
    concurrency: int,
    batch_size: int,
    domain_filter: str | None = None,
):
    patterns = compile_patterns(keywords)
    sem = asyncio.Semaphore(concurrency)
    timeout = aiohttp.ClientTimeout(total=DEFAULT_TIMEOUT)
    connector = aiohttp.TCPConnector(limit=concurrency)

    results: list[tuple[str, str, str]] = []

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        pages = await discover_urls(session, sitemaps)

        # Mode‑specific URL transform
        if mode == "Jamku":
            new_pages: List[str] = []
            for p in pages:
                m = GSTIN_RE.search(p)
                if m:
                    gstin = m.group(0).upper()
                    new_pages.append(f"https://gst.jamku.app/gstin/{gstin}")
            pages = list(dict.fromkeys(new_pages))  # deduplicate
            st.info(f"🔗 Transformed to {len(pages):,} Jamku URLs")

        if domain_filter:
            pages = [u for u in pages if domain_filter in u]
        st.info(f"🌐 Total pages to scan: {len(pages):,}")

        scanned = 0
        prog = st.progress(0.0, text="🔍 Scanning pages …")
        start = time.perf_counter()

        for batch in chunked(pages, batch_size):
            tasks = [_page_contains_keyword(session, url, patterns, sem) for url in batch]
            for hit in await asyncio.gather(*tasks, return_exceptions=True):
                scanned += 1
                if isinstance(hit, tuple):
                    results.append(hit)
            prog.progress(min(1.0, scanned / len(pages)))

        prog.empty()
        st.success(f"✅ Completed in {time.perf_counter() - start:.1f}s – {len(results):,} matches")

    return pd.DataFrame(results, columns=["url", "matched_keyword", "page_title"])

# ────────────────────────────── Streamlit UI ────────────────────────────────
st.set_page_config(page_title="Keyword Sitemap Crawler", layout="wide")
st.title("🔍 Keyword Sitemap Crawler – Piceapp / Jamku Edition")
st.caption("Crawl Piceapp sitemaps directly or jump to matching Jamku GSTIN pages.")

with st.form("crawl_form"):
    col1, col2 = st.columns(2)
    with col1:
        mode = st.selectbox("Site mode", ["Piceapp", "Jamku"], index=0)
        sitemap_input = st.text_area("🌐 Sitemap URLs (one per line)")
        domain_filter = st.text_input("🔗 Optional domain filter")
    with col2:
        keyword_file = st.file_uploader("📄 Upload Keyword List (one per line)", type=["txt"])
        concurrency = st.slider("⚙️ Concurrent HTTP Requests", 10, 500, DEFAULT_CONCURRENCY, 10)
        batch_size = st.slider("📦 Batch size", 200, 5000, DEFAULT_BATCH_SIZE, 200)
    submitted = st.form_submit
