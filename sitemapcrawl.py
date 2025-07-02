#!/usr/bin/env python3
"""
Keyword Sitemap Crawler – All‑Time Best Edition (Piceapp‑first → Jamku fallback)
================================================================================
* **Two‑phase scan**
  1. Crawl & scan Piceapp URLs (high‑speed, async).
  2. Retry every *failed* Piceapp URL as a Jamku GSTIN URL (rate‑limited to **1 request/sec**).
* **Root‑aware HS‑code keyword matching**: “0813” ⇒ 0813xxxx…
* **Async** (`aiohttp`, `asyncio`) with exponential‑backoff retries & DNS cache.
* **Streamlit UI** with live progress + Altair bar chart of matches.
* **CSV export** – columns: `url`, `matched_keyword`, `matched_root`, `page_title`.

© 2025 — Designed for 1 M+ pages.
"""

from __future__ import annotations

import asyncio
import gzip
import logging
import re
import time
from io import BytesIO
from typing import Dict, List, Sequence, Tuple

import aiohttp
import async_timeout
import pandas as pd
import streamlit as st
from bs4 import BeautifulSoup
from lxml import etree
from more_itertools import chunked
import altair as alt

# ─────────── Config ───────────
DEFAULT_TIMEOUT: int = 20          # seconds per request
DEFAULT_CONCURRENCY: int = 50      # max parallel fetches for Piceapp phase
DEFAULT_BATCH_SIZE: int = 1_000    # pages per gather() batch
REGEX_CHUNK_SIZE: int = 1_000      # tokens per compiled regex (avoids "too‑large" regex)
DNS_CACHE_TTL: int = 300           # seconds
GSTIN_RE = re.compile(r"[0-9]{2}[A-Z0-9]{10}[0-9A-Z]{3}", re.I)

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
LOG = logging.getLogger("kws-crawler")

# ─────────── Keyword helpers ───────────

def _validate_keywords(raw: Sequence[str]) -> List[str]:
    """Drop empty/too‑short tokens & weird chars."""
    return [k.strip() for k in raw if re.fullmatch(r"[\w\- ]{2,}", k.strip())]


def build_keyword_mapping(raw: Sequence[str]) -> Tuple[List[str], Dict[str, str]]:
    """Return **flat regex tokens** + **root‑lookup** map."""
    cleaned = sorted({k for k in _validate_keywords(raw)})
    roots = {k for k in cleaned if k.isdigit() and len(k) <= 4}  # 4‑digit masters

    flat: List[str] = []
    mapping: Dict[str, str] = {}

    for kw in cleaned:
        if kw.isdigit() and len(kw) == 4:
            # 4‑digit root itself – e.g. 0813
            mapping[kw] = kw
            flat.append(kw)
            # Any 6/8‑digit children (e.g. 08131000)
            family_regex = fr"\b{kw}\d{{4}}\b"
            mapping[family_regex] = kw
            flat.append(family_regex)
        else:
            root = kw[:4] if kw[:4] in roots else kw
            mapping[kw] = root
            flat.append(kw)
    return flat, mapping


def compile_patterns(tokens: Sequence[str]) -> List[re.Pattern]:
    return [
        re.compile("|".join(tokens[i : i + REGEX_CHUNK_SIZE]), re.I)
        for i in range(0, len(tokens), REGEX_CHUNK_SIZE)
    ]

# ─────────── HTTP fetch with retries ───────────

async def fetch(session: aiohttp.ClientSession, url: str, retries: int = 3, backoff: float = 1.7) -> bytes:
    for attempt in range(1, retries + 1):
        try:
            async with async_timeout.timeout(DEFAULT_TIMEOUT):
                async with session.get(url, ssl=False) as resp:
                    resp.raise_for_status()
                    return await resp.read()
        except Exception as exc:
            LOG.warning("[Retry %s/%s] %s – %s", attempt, retries, url, exc)
            if attempt == retries:
                raise
            await asyncio.sleep(backoff ** attempt)

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
        LOG.warning("Parse failed %s – %s", url, exc)
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
            queue.extend(u for u in locs if u not in seen)  # recurse
        else:
            pages.extend(locs)
        done += 1
        prog.progress(done / (done + len(queue) + 1e-9))
    prog.empty()
    return list(dict.fromkeys(pages))

# ─────────── GSTIN URL helpers ───────────

def to_jamku_url(pice_url: str) -> str | None:
    if m := GSTIN_RE.search(pice_url):
        return f"https://gst.jamku.app/gstin/{m.group(0).lower()}"
    return None

# ─────────── Page scan ───────────

async def _scan_page(session: aiohttp.ClientSession, url: str, patterns: List[re.Pattern], sem: asyncio.Semaphore) -> Tuple[str, str, str] | None:
    """Return (url, matched_keyword, title) or None on no‑match; raises for HTTP errors."""
    async with sem:
        html = (await fetch(session, url)).decode("utf-8", "ignore")
        # find first pattern hit
        match = next((m for p in patterns if (m := p.search(html))), None)
        if match:
            soup = BeautifulSoup(html, "html.parser")
            title = (soup.title.string or "").strip() if soup.title else ""
            return url, match.group(0), title
        return None

# ─────────── Main crawl routine ───────────

async def crawl_and_retry(
    sitemaps: List[str],
    raw_keywords: List[str],
    concurrency: int,
    batch_size: int,
) -> pd.DataFrame:
    """Phase‑1 Piceapp scan → retry failed as Jamku (1 req/sec)."""

    tokens, root_map = build_keyword_mapping(raw_keywords)
    patterns = compile_patterns(tokens)

    # Set up shared client + DNS cache
    conn = aiohttp.TCPConnector(limit=concurrency, ttl_dns_cache=DNS_CACHE_TTL)
    timeout = aiohttp.ClientTimeout(total=DEFAULT_TIMEOUT)

    results: List[Tuple[str, str, str]] = []
    failed_pice_urls: List[str] = []

    async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
        # 1️⃣ Discover URLs from sitemap(s)
        base_pages = await discover_urls(session, sitemaps)
        st.info(f"🌐 Total Piceapp pages discovered: {len(base_pages):,}")

        # 2️⃣ High‑speed Piceapp scan (async‑gather)
        sem = asyncio.Semaphore(concurrency)
        scanned = 0
        prog = st.progress(0.0, text="⚡ Scanning Piceapp …")
        start = time.perf_counter()

        for batch in chunked(base_pages, batch_size):
            tasks = [_scan_page(session, u, patterns, sem) for u in batch]
            for res in await asyncio.gather(*tasks, return_exceptions=True):
                scanned += 1
                if isinstance(res, tuple):
                    results.append(res)
                elif isinstance(res, Exception):
                    # Fetch/HTTP failure – queue for Jamku retry
                    failed_pice_urls.append(res.args[0] if res.args else "")
            prog.progress(min(1.0, scanned / len(base_pages)))
        prog.empty()
        st.success(f"✅ Piceapp phase completed in {time.perf_counter() - start:.1f}s")

        # 3️⃣ Prepare Jamku retry list
        jamku_urls = [u for u in {to_jamku_url(p) for p in failed_pice_urls} if u]
        if not jamku_urls:
            st.info("🎉 No failed URLs to retry on Jamku.")
            return _to_df(results, root_map)

        st.info(f"♻️ Retrying {len(jamku_urls):,} failed URLs via Jamku (1 req/sec)")

        # 4️⃣ Rate‑limited Jamku scan (sequential)
        sem_jamku = asyncio.Semaphore(1)
        prog_j = st.progress(0.0, text="🔄 Scanning Jamku …")
        for idx, url in enumerate(jamku_urls, start=1):
            try:
                res = await _scan_page(session, url, patterns, sem_jamku)
                if res:
                    results.append(res)
            except Exception as exc:
                LOG.warning("Jamku fetch failed %s – %s", url, exc)
            prog_j.progress(idx / len(jamku_urls))
            if idx < len(jamku_urls):
                await asyncio.sleep(1)  # 1 request/second
        prog_j.empty()

    return _to_df(results, root_map)

# ─────────── Helpers ───────────

def _to_df(records: List[Tuple[str, str, str]], root_map: Dict[str, str]) -> pd.DataFrame:
    df = pd.DataFrame(records, columns=["url", "matched_keyword", "page_title"])
    df["matched_root"] = df["matched_keyword"].map(root_map).fillna(df["matched_keyword"])
    return df[["url", "matched_keyword", "matched_root", "page_title"]]

# ─────────── Streamlit UI ───────────

st.set_page_config(page_title="Keyword Sitemap Crawler", layout="wide")
st.title("🔍 Keyword Sitemap Crawler — All‑Time Best 🚀")

with st.form("crawl_form"):
    sitemaps_input = st.text_area("Sitemap URL(s) (one per line)", height=120)
    kw_file = st.file_uploader("Keyword list (TXT)", type=["txt"])
    col1, col2 = st.columns(2)
    concurrency = col1.slider("Concurrency (Piceapp phase)", 10, 500, DEFAULT_CONCURRENCY, 10)
    batch_size = col2.slider("Batch size", 200, 5_000, DEFAULT_BATCH_SIZE, 200)
    submitted = st.form_submit_button("🚀 Run Crawl")

if submitted:
    if not sitemaps_input or not kw_file:
        st.error("❌ Provide both sitemap URLs and a keyword list.")
        st.stop()

    sitemap_list = [u.strip() for u in sitemaps_input.splitlines() if u.strip()]
    raw_kw = kw_file.read().decode("utf-8", "ignore").splitlines()
    st.write(f"📄 {len(raw_kw):,} keywords loaded.")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        df_out = loop.run_until_complete(crawl_and_retry(sitemap_list, raw_kw, concurrency, batch_size))
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()

    # — Results
    if df_out.empty:
        st.warning("⚠️ No keyword matches found.")
    else:
        st.dataframe(df_out, use_container_width=True)
        csv_bytes = df_out.to_csv(index=False).encode("utf-8")
        st.download_button("📥 Download CSV", csv_bytes, "keyword_matches.csv", "text/csv")

        # Altair summary chart
        chart_data = df_out["matched_root"].value_counts().reset_index(names=["matched_root", "count"])
        bar = (
            alt.Chart(chart_data)
            .mark_bar()
            .encode(x="matched_root:N", y="count:Q", tooltip=["matched_root", "count"])
            .properties(title="Match count by root", height=400)
        )
        st.altair_chart(bar, use_container_width=True)
