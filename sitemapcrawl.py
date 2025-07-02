# 📦 Install required packages before running:
# pip install streamlit aiohttp async-timeout lxml tqdm tenacity beautifulsoup4

import asyncio
import aiohttp
import async_timeout
import gzip
import re
import logging
from io import BytesIO
from pathlib import Path
from lxml import etree
from bs4 import BeautifulSoup
from typing import List, Tuple
import streamlit as st
from tempfile import NamedTemporaryFile
import csv

st.set_page_config(page_title="Keyword Sitemap Crawler", layout="wide")
st.title("🔍 Keyword Sitemap Crawler")

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger("crawler")

DEFAULT_TIMEOUT = 15

# ────────────────────────────── Async Fetch ──────────────────────────────
async def fetch(session: aiohttp.ClientSession, url: str) -> bytes:
    async with async_timeout.timeout(DEFAULT_TIMEOUT):
        async with session.get(url, ssl=False) as resp:
            resp.raise_for_status()
            return await resp.read()

# ────────────────────────────── Sitemap Parsing ──────────────────────────────
async def _urls_from_sitemap(session: aiohttp.ClientSession, url: str) -> List[str]:
    try:
        raw = await fetch(session, url)
        if url.endswith(".gz"):
            raw = gzip.decompress(raw)
        tree = etree.parse(BytesIO(raw))
        root_tag = etree.QName(tree.getroot()).localname
        if root_tag == "urlset":
            return [loc.text for loc in tree.findall(".//{*}loc")]
        if root_tag == "sitemapindex":
            return [loc.text for loc in tree.findall(".//{*}loc")]
        return []
    except Exception as e:
        LOGGER.warning(f"Failed to parse {url}: {e}")
        return []

async def iter_sitemap_urls(session: aiohttp.ClientSession, sitemap_urls: List[str]) -> List[str]:
    all_urls = []
    seen = set()
    to_visit = list(sitemap_urls)

    while to_visit:
        sitemap = to_visit.pop()
        if sitemap in seen:
            continue
        seen.add(sitemap)
        urls = await _urls_from_sitemap(session, sitemap)
        if urls and not urls[0].lower().endswith((".xml", ".gz")):
            all_urls.extend(urls)
        else:
            to_visit.extend(urls)
    return all_urls

# ────────────────────────────── Keyword Matching ──────────────────────────────
async def _page_contains_keyword(session, url, pattern, sem):
    async with sem:
        try:
            html_bytes = await fetch(session, url)
            text = html_bytes.decode("utf-8", errors="ignore")
            match = pattern.search(text)
            if match:
                soup = BeautifulSoup(text, "html.parser")
                title = soup.title.string.strip() if soup.title else ""
                return url, match.group(0), title
        except Exception:
            return None
    return None

async def crawl_sitemaps(sitemap_urls, keywords, concurrency):
    pattern = re.compile(r"|".join(map(re.escape, keywords)), re.IGNORECASE)
    connector = aiohttp.TCPConnector(limit=concurrency)
    timeout = aiohttp.ClientTimeout(total=None)

    results = []
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        urls = await iter_sitemap_urls(session, sitemap_urls)
        st.info(f"Total URLs discovered: {len(urls)}")

        sem = asyncio.Semaphore(concurrency)
        tasks = [_page_contains_keyword(session, url, pattern, sem) for url in urls]

        for fut in asyncio.as_completed(tasks):
            result = await fut
            if result:
                results.append(result)
    return results

# ────────────────────────────── UI ──────────────────────────────
with st.form("crawl_form"):
    sitemap_input = st.text_area("Enter sitemap URLs (one per line)")
    keyword_file = st.file_uploader("Upload Keyword List (one per line)", type=["txt"])
    concurrency = st.slider("Concurrent HTTP Requests", 5, 100, 25)
    submitted = st.form_submit_button("Start Crawling")

if submitted and sitemap_input and keyword_file:
    sitemap_urls = [u.strip() for u in sitemap_input.strip().splitlines() if u.strip()]
    keywords = [line.strip() for line in keyword_file.read().decode("utf-8").splitlines() if line.strip()]

    with st.spinner("Crawling in progress..."):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        matches = loop.run_until_complete(crawl_sitemaps(sitemap_urls, keywords, concurrency))

    if matches:
        st.success(f"✅ Found {len(matches)} matching pages!")
        st.dataframe(matches, use_container_width=True)

        # Export to CSV
        with NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            writer = csv.writer(tmp)
            writer.writerow(["url", "matched_keyword", "page_title"])
            writer.writerows(matches)
            tmp_path = tmp.name

        with open(tmp_path, "rb") as f:
            st.download_button("📥 Download Results", f, file_name="matched_urls.csv")
    else:
        st.warning("No matches found!")
