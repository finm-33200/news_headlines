"""
Pull free newswire headlines from PR Newswire, GlobeNewswire, and Newswire.ca.

Scrapes press releases via sitemap crawling. Raw headlines are saved to a
Hive-partitioned data lake:

    newswire_headlines/source={key}/year=YYYY/month=MM/day=DD/data.parquet

Sources:
  PR Newswire     — gzipped monthly sitemaps (2010–present, some gaps).
                    Wayback Machine fallback for corrupted/missing months.
  GlobeNewswire   — plain monthly sitemaps on sitemaps.globenewswire.com
                    (Apr 2023–present, ~10K articles/month). Titles are
                    embedded in the sitemap XML — fast-path extraction
                    with no per-page HTTP.
  Newswire.ca     — gzipped monthly sitemaps (same format as PR Newswire,
                    2011–present, ~2.6K releases/month). No news:title in
                    sitemap; headlines extracted from URL slugs (fast-path).

S&P 500 filtering is done downstream in analysis notebooks.

Two modes:
  Default    — crawl a single sample month (default: SAMPLE_MONTH).
  --full     — long-running crawl (days/weeks), 2010 to present.

Both modes write to the same output directory with the same daily
partitioning. Completed days are skipped on re-run. Safe to Ctrl+C.

Note: Business Wire was previously attempted but removed — its sitemap
consistently times out.

"""

import argparse
import calendar
import gc
import gzip
import logging
import re
import signal
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from urllib.parse import unquote

import polars as pl
import requests
from bs4 import BeautifulSoup
from lxml import etree

from pull_gdelt_headlines import SAMPLE_MONTH, _generate_month_ranges
from settings import config

DATA_DIR = Path(config("DATA_DIR"))
NEWSWIRE_RAW_DIR = DATA_DIR / "newswire_headlines"
FULL_START_DATE = "2010-01-01"

REQUEST_DELAY = 0.5  # seconds between page requests
CONCURRENT_WORKERS = 8  # parallel page-fetch workers (fallback path)
SITEMAP_DELAY = 0.5  # seconds between sitemap requests
MAX_RETRIES = 3

HEADERS = {
    "User-Agent": "FINM33200-CourseProject/1.0 (University of Chicago; academic research)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------

_session = None
_thread_local = threading.local()


def _get_session():
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update(HEADERS)
    return _session


def _get_thread_session():
    """Return a per-thread requests.Session (for concurrent workers)."""
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        s.headers.update(HEADERS)
        _thread_local.session = s
    return _thread_local.session


def _reset_session():
    global _session
    if _session is not None:
        _session.close()
    _session = None


def _fetch(url, delay=REQUEST_DELAY, session=None):
    """Fetch a URL with rate limiting, retries, and exponential backoff.

    Returns the Response on success, or None on 404 / permanent failure.
    Pass a thread-local session for use from worker threads.
    """
    if session is None:
        session = _get_session()
    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(delay)
            resp = session.get(url, timeout=30)
            if resp.status_code == 200:
                return resp
            elif resp.status_code == 404:
                logger.debug(f"404: {url}")
                return None
            elif resp.status_code == 429:
                wait = (2**attempt) * 10
                logger.warning(f"429 rate limited, waiting {wait}s: {url}")
                time.sleep(wait)
            else:
                logger.warning(f"HTTP {resp.status_code}: {url}")
                if resp.status_code >= 500:
                    time.sleep(2**attempt)
                else:
                    return None
        except requests.RequestException as e:
            logger.warning(f"Request error (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
            time.sleep(2**attempt)
    logger.error(f"Failed after {MAX_RETRIES} retries: {url}")
    return None


# ---------------------------------------------------------------------------
# Sitemap XML parsing
# ---------------------------------------------------------------------------

SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
NEWS_NS = {"news": "http://www.google.com/schemas/sitemap-news/0.9"}

_XML_PARSER = etree.XMLParser(recover=True)


def _xml_root(xml_input):
    """Parse XML (str or bytes) into an lxml root element, tolerating minor corruption.

    Accepts either ``str`` (decoded text) or ``bytes`` (raw response content).
    Uses lxml's recovery parser to tolerate null bytes and minor corruption.
    Returns None if parsing fails entirely.
    """
    if isinstance(xml_input, str):
        xml_bytes = xml_input.encode("utf-8", errors="replace")
    else:
        xml_bytes = xml_input
    # Strip UTF-8 BOM if present
    if xml_bytes.startswith(b"\xef\xbb\xbf"):
        xml_bytes = xml_bytes[3:]
    # Strip null bytes
    xml_bytes = xml_bytes.replace(b"\x00", b"")
    try:
        return etree.fromstring(xml_bytes, _XML_PARSER)
    except Exception as e:
        logger.warning(f"XML parse failed even with recovery: {e}")
        return None


def _parse_sitemap_index(xml_text):
    """Parse a sitemap index XML -> list of child sitemap URLs."""
    root = _xml_root(xml_text)
    if root is None:
        logger.warning("Failed to parse sitemap index XML")
        return []
    urls = []
    # Try with namespace first, then without
    for sitemap in root.findall("sm:sitemap", SITEMAP_NS):
        loc = sitemap.find("sm:loc", SITEMAP_NS)
        if loc is not None and loc.text:
            urls.append(loc.text.strip())
    if not urls:
        for sitemap in root.findall("sitemap"):
            loc = sitemap.find("loc")
            if loc is not None and loc.text:
                urls.append(loc.text.strip())
    return urls


def _parse_sitemap_urls(xml_text, url_filter=None):
    """Parse a sitemap XML -> list of page URLs, optionally filtered."""
    root = _xml_root(xml_text)
    if root is None:
        logger.warning("Failed to parse sitemap XML")
        return []
    urls = []
    for url_elem in root.findall("sm:url", SITEMAP_NS):
        loc = url_elem.find("sm:loc", SITEMAP_NS)
        if loc is not None and loc.text:
            url = loc.text.strip()
            if url_filter is None or url_filter(url):
                urls.append(url)
    if not urls:
        for url_elem in root.findall("url"):
            loc = url_elem.find("loc")
            if loc is not None and loc.text:
                url = loc.text.strip()
                if url_filter is None or url_filter(url):
                    urls.append(url)
    return urls


def _parse_sitemap_news_entries(xml_bytes):
    """Parse a Google News sitemap -> list of {headline, source_url, date} dicts.

    Returns an empty list if no <news:title> elements are found (i.e., the
    sitemap is a plain URL list without news metadata).
    """
    root = _xml_root(xml_bytes)
    if root is None:
        return []
    url_elems = root.findall("sm:url", SITEMAP_NS)
    if not url_elems:
        url_elems = root.findall("url")
    entries = []
    for url_elem in url_elems:
        loc = url_elem.find("sm:loc", SITEMAP_NS)
        if loc is None:
            loc = url_elem.find("loc")
        if loc is None or not loc.text:
            continue
        url = loc.text.strip()
        if "/news-release" not in url.lower():
            continue
        title = None
        pub_date = None
        news_elem = url_elem.find("news:news", NEWS_NS)
        if news_elem is not None:
            title_elem = news_elem.find("news:title", NEWS_NS)
            if title_elem is not None and title_elem.text:
                title = title_elem.text.strip()
            date_elem = news_elem.find("news:publication_date", NEWS_NS)
            if date_elem is not None and date_elem.text:
                pub_date = date_elem.text[:10]
        if not pub_date:
            lastmod = url_elem.find("sm:lastmod", SITEMAP_NS)
            if lastmod is None:
                lastmod = url_elem.find("lastmod")
            if lastmod is not None and lastmod.text:
                pub_date = lastmod.text[:10]
        if title and pub_date:
            entries.append({"headline": title, "source_url": url, "date": pub_date})
    return entries


# ---------------------------------------------------------------------------
# Scraper classes
# ---------------------------------------------------------------------------


_WAYBACK_TIMESTAMPS = {
    # Discovered via src/discover_wayback_timestamps.py on 2026-03-28.
    # 79 new timestamps for the 2012-07 through 2019-12 gap, plus 9 existing.
    # 11 months have no Wayback archive: 2012-07..09, 2015-03..04, 2016-10,
    # 2018-02, 2018-06, 2019-02, 2019-10..11.
    "2012-10": "20250801115512",
    "2012-11": "20260102151437",
    "2012-12": "20260101172855",
    "2013-01": "20260102151215",
    "2013-02": "20260101173112",
    "2013-03": "20260102151242",
    "2013-04": "20260101173130",
    "2013-05": "20260102195915",
    "2013-06": "20260102151431",
    "2013-07": "20260102151315",
    "2013-08": "20260101172918",
    "2013-09": "20260102151409",
    "2013-10": "20260102151158",
    "2013-11": "20260102190551",
    "2013-12": "20260101173135",
    "2014-01": "20260102151232",
    "2014-02": "20260101173125",
    "2014-03": "20260103025302",
    "2014-04": "20260101172908",
    "2014-05": "20260102151433",
    "2014-06": "20260102151316",
    "2014-07": "20260102151358",
    "2014-08": "20260101172939",
    "2014-09": "20260103003912",
    "2014-10": "20260102151408",
    "2014-11": "20260102151424",
    "2014-12": "20260101173054",
    "2015-01": "20260102151450",
    "2015-02": "20260101173018",
    "2015-05": "20260102151626",
    "2015-06": "20260102151339",
    "2015-07": "20260102151419",
    "2015-08": "20260101173215",
    "2015-09": "20260102151203",
    "2015-10": "20260102151419",
    "2015-11": "20260102151208",
    "2015-12": "20260101173150",
    "2016-01": "20260102151207",
    "2016-02": "20260101172936",
    "2016-03": "20260102151343",
    "2016-04": "20260101173104",
    "2016-05": "20260102151518",
    "2016-06": "20260102151351",
    "2016-07": "20260102151310",
    "2016-08": "20260101173140",
    "2016-09": "20260102151357",
    "2016-11": "20260102151315",
    "2016-12": "20260101173121",
    "2017-01": "20260102151152",
    "2017-02": "20260101173117",
    "2017-03": "20260102151354",
    "2017-04": "20260101173157",
    "2017-05": "20260102151410",
    "2017-06": "20260102151313",
    "2017-07": "20260102151242",
    "2017-08": "20260101172946",
    "2017-09": "20260102151347",
    "2017-10": "20260102151324",
    "2017-11": "20260102151418",
    "2017-12": "20260101173000",
    "2018-01": "20260102151426",
    "2018-03": "20260102151402",
    "2018-04": "20260101172925",
    "2018-05": "20260102151337",
    "2018-07": "20260102151405",
    "2018-08": "20260102151505",
    "2018-09": "20260102151218",
    "2018-10": "20260102151341",
    "2018-11": "20260102151319",
    "2018-12": "20260102151259",
    "2019-01": "20260102151151",
    "2019-03": "20260102151223",
    "2019-04": "20260102151503",
    "2019-05": "20260102151413",
    "2019-06": "20260102151701",
    "2019-07": "20260102151314",
    "2019-08": "20260102151309",
    "2019-09": "20260102151447",
    "2019-12": "20260102151413",
    "2021-03": "20250402102233",
    "2021-09": "20250402095524",
    "2021-10": "20240101020755",
    "2021-11": "20260102151312",
    "2022-06": "20250218160245",
    "2022-07": "20240502014628",
    "2022-08": "20240501203810",
    "2023-01": "20240101074937",
    "2023-10": "20240101014022",
}


class PRNewswireScraper:
    """Scraper for PR Newswire (prnewswire.com)."""

    NAME = "PR Newswire"
    SOURCE_KEY = "prnewswire"
    SITEMAP_INDEX_URL = "https://www.prnewswire.com/sitemap-gz.xml"

    _MONTH_ABBR = {
        1: "Jan",
        2: "Feb",
        3: "Mar",
        4: "Apr",
        5: "May",
        6: "Jun",
        7: "Jul",
        8: "Aug",
        9: "Sep",
        10: "Oct",
        11: "Nov",
        12: "Dec",
    }

    def date_from_url(self, url):
        """PR Newswire URLs don't contain dates."""
        return None

    def press_release_url_filter(self, url):
        """Return whether a sitemap URL is a press release page."""
        return "/news-release" in url.lower()

    def _fetch_sitemap_xml(self, year, month):
        """Fetch and decompress the gzipped monthly sitemap.

        Tries the live PR Newswire URL first.  If decompression fails
        (known-corrupted months), falls back to an archived copy from
        the Wayback Machine.  Returns decompressed XML bytes, or None.
        """
        month_str = f"{year:04d}-{month:02d}"
        abbr = self._MONTH_ABBR.get(month)
        if abbr is None:
            return None

        gz_url = f"https://www.prnewswire.com/Sitemap_Index_{abbr}_{year}.xml.gz"
        logger.info(f"{self.NAME}: fetching {gz_url}")
        resp = _fetch(gz_url, delay=SITEMAP_DELAY)

        if resp is not None:
            try:
                return gzip.decompress(resp.content)
            except Exception as e:
                logger.warning(f"{self.NAME}: decompress failed for {month_str}: {e}")

        # Fallback: try the Wayback Machine
        ts = _WAYBACK_TIMESTAMPS.get(month_str)
        if ts is None:
            logger.warning(f"{self.NAME}: no Wayback timestamp for {month_str}")
            return None

        wb_url = f"https://web.archive.org/web/{ts}id_/{gz_url}"
        logger.info(f"{self.NAME}: trying Wayback Machine: {wb_url}")
        resp = _fetch(wb_url, delay=SITEMAP_DELAY)
        if resp is None:
            logger.warning(f"{self.NAME}: Wayback fetch failed for {month_str}")
            return None

        try:
            xml_bytes = gzip.decompress(resp.content)
            logger.info(f"{self.NAME}: recovered {month_str} from Wayback Machine")
            return xml_bytes
        except Exception as e:
            logger.warning(
                f"{self.NAME}: Wayback decompress failed for {month_str}: {e}"
            )
            return None

    def _parse_headline_from_soup(self, soup, url):
        headline = None
        for selector in ["h1.release-header__title", "h1"]:
            tag = soup.select_one(selector)
            if tag and tag.get_text(strip=True):
                headline = tag.get_text(strip=True)
                break
        if not headline:
            return None
        pub_date = None
        for attr in [{"name": "date"}, {"property": "article:published_time"}]:
            meta = soup.find("meta", attrs=attr)
            if meta and meta.get("content"):
                pub_date = meta["content"][:10]
                break
        return {"headline": headline, "source_url": url, "date": pub_date}

    def sitemap_urls_for_month(self, year, month):
        """Get press release URLs from gzipped monthly sitemaps.

        PR Newswire hosts compressed sitemaps at e.g.
        ``Sitemap_Index_Jan_2025.xml.gz``, listed in ``sitemap-gz.xml``.
        Falls back to the Wayback Machine for corrupted archives.
        """
        month_str = f"{year:04d}-{month:02d}"
        xml_bytes = self._fetch_sitemap_xml(year, month)
        if xml_bytes is None:
            logger.warning(f"{self.NAME}: no sitemap available for {month_str}")
            return []

        urls = _parse_sitemap_urls(
            xml_bytes,
            url_filter=self.press_release_url_filter,
        )
        logger.info(f"{self.NAME}: {len(urls)} URLs for {month_str}")
        return urls

    def sitemap_entries_for_month(self, year, month):
        """Try to extract headlines directly from the gzipped monthly sitemap.

        Returns a list of {headline, source_url, date} dicts if the sitemap
        contains Google News metadata (<news:title>), or None if no metadata
        is found and callers should fall back to per-page fetches.
        Falls back to the Wayback Machine for corrupted archives.
        """
        month_str = f"{year:04d}-{month:02d}"
        xml_bytes = self._fetch_sitemap_xml(year, month)
        if xml_bytes is None:
            return None
        entries = _parse_sitemap_news_entries(xml_bytes)
        if entries:
            logger.info(
                f"{self.NAME}: {len(entries)} entries with sitemap metadata for {month_str}"
            )
            return entries
        logger.info(
            f"{self.NAME}: no <news:title> in sitemap for {month_str}; "
            f"falling back to per-page fetches"
        )
        return None


class GlobeNewswireScraper:
    """Scraper for GlobeNewswire (globenewswire.com).

    GlobeNewswire hosts a comprehensive sitemap index on a separate subdomain:
        https://sitemaps.globenewswire.com/news-en.xml     (sitemap index)
        https://sitemaps.globenewswire.com/news/en/YYYY-MM.xml  (monthly)
        https://sitemaps.globenewswire.com/news/en/latest.xml   (rolling recent)

    Monthly sitemaps are plain XML (not gzipped) and include Google News
    metadata: title, publication date, keywords, and stock tickers.
    This means headlines can be extracted directly from the sitemap XML
    without fetching individual article pages (fast-path).

    Coverage: Apr 2023 to present (~10K articles/month).
    """

    NAME = "GlobeNewswire"
    SOURCE_KEY = "globenewswire"
    SITEMAP_INDEX_URL = "https://sitemaps.globenewswire.com/news-en.xml"
    EARLIEST_MONTH = (2023, 4)  # Earliest month with a sitemap on the live index

    _available_months_cache = None

    def available_months(self):
        """Discover which monthly sitemaps exist by parsing the sitemap index.

        Queries ``news-en.xml`` once and caches the result for the process
        lifetime. Returns a set of (year, month) tuples.
        """
        if self._available_months_cache is not None:
            return self._available_months_cache

        logger.info(f"{self.NAME}: fetching sitemap index {self.SITEMAP_INDEX_URL}")
        resp = _fetch(self.SITEMAP_INDEX_URL, delay=SITEMAP_DELAY)
        if resp is None:
            logger.warning(f"{self.NAME}: could not fetch sitemap index")
            self._available_months_cache = set()
            return self._available_months_cache

        child_urls = _parse_sitemap_index(resp.content)
        months = set()
        for url in child_urls:
            # Pattern: .../news/en/YYYY-MM.xml  (skip latest.xml)
            if "latest.xml" in url:
                continue
            # Extract YYYY-MM from the URL
            m = re.search(r"/(\d{4})-(\d{2})\.xml", url)
            if m:
                months.add((int(m.group(1)), int(m.group(2))))
        self._available_months_cache = months
        logger.info(f"{self.NAME}: sitemap index has {len(months)} monthly sitemaps")
        return months

    def date_from_url(self, url):
        """Extract date from GlobeNewswire URL.

        URL format: .../news-release/YYYY/MM/DD/{id}/0/en/{slug}.html
        """
        try:
            idx = url.index("/news-release/")
            parts = url[idx:].split("/")
            # parts: ['', 'news-release', 'YYYY', 'MM', 'DD', ...]
            if len(parts) >= 5:
                return f"{parts[2]}-{parts[3]}-{parts[4]}"
        except (ValueError, IndexError):
            pass
        return None

    def press_release_url_filter(self, url):
        """Return whether a sitemap URL is a press release page."""
        return "/news-release/" in url.lower()

    def _fetch_sitemap_xml(self, year, month):
        """Fetch the monthly sitemap XML (plain, not gzipped).

        Returns XML bytes, or None if the month is not available.
        """
        month_str = f"{year:04d}-{month:02d}"
        url = f"https://sitemaps.globenewswire.com/news/en/{month_str}.xml"
        logger.info(f"{self.NAME}: fetching {url}")
        resp = _fetch(url, delay=SITEMAP_DELAY)
        if resp is None:
            logger.warning(f"{self.NAME}: no sitemap available for {month_str}")
            return None
        return resp.content

    def _parse_headline_from_soup(self, soup, url):
        """Parse headline from a GlobeNewswire article page (fallback path).

        Rarely needed since GlobeNewswire sitemaps always include news:title.
        """
        headline = None
        for selector in ["h1.article-headline", "h1.main-title", "h1"]:
            tag = soup.select_one(selector)
            if tag and tag.get_text(strip=True):
                headline = tag.get_text(strip=True)
                break
        if not headline:
            return None
        pub_date = self.date_from_url(url)
        if not pub_date:
            for attr in [{"name": "date"}, {"property": "article:published_time"}]:
                meta = soup.find("meta", attrs=attr)
                if meta and meta.get("content"):
                    pub_date = meta["content"][:10]
                    break
        return {"headline": headline, "source_url": url, "date": pub_date}

    def sitemap_urls_for_month(self, year, month):
        """Get press release URLs from monthly sitemap."""
        month_str = f"{year:04d}-{month:02d}"
        xml_bytes = self._fetch_sitemap_xml(year, month)
        if xml_bytes is None:
            logger.warning(f"{self.NAME}: no sitemap available for {month_str}")
            return []
        urls = _parse_sitemap_urls(
            xml_bytes,
            url_filter=self.press_release_url_filter,
        )
        logger.info(f"{self.NAME}: {len(urls)} URLs for {month_str}")
        return urls

    def sitemap_entries_for_month(self, year, month):
        """Extract headlines directly from monthly sitemap metadata.

        GlobeNewswire sitemaps always include news:title, so the fast-path
        should always succeed. Returns None only if the sitemap is unavailable.
        """
        month_str = f"{year:04d}-{month:02d}"
        xml_bytes = self._fetch_sitemap_xml(year, month)
        if xml_bytes is None:
            return None
        entries = _parse_sitemap_news_entries(xml_bytes)
        if entries:
            logger.info(
                f"{self.NAME}: {len(entries)} entries with sitemap metadata for {month_str}"
            )
            return entries
        logger.info(
            f"{self.NAME}: no <news:title> in sitemap for {month_str}; "
            f"falling back to per-page fetches"
        )
        return None


class NewswireCaScraper:
    """Scraper for Newswire.ca / Cision (newswire.ca).

    Newswire.ca hosts gzipped monthly sitemaps with the same naming convention
    as PR Newswire:
        https://www.newswire.ca/sitemap-gz.xml              (sitemap index)
        https://www.newswire.ca/Sitemap_Index_Mon_YYYY.xml.gz  (monthly gz)

    The sitemaps include ``<lastmod>`` timestamps but **no** Google News
    metadata (``news:title``).  To avoid ~465K per-page HTTP fetches, this
    scraper extracts approximate headlines from the URL slug:

        /news-releases/some-headline-text-123456789.html
        -> "Some Headline Text"

    Slug-derived headlines preserve all content words, which is sufficient
    for TF-IDF crosswalk matching against RavenPack.

    Coverage: ~2,600 releases/month, 2011–present (179 monthly sitemaps).
    """

    NAME = "Newswire.ca"
    SOURCE_KEY = "newswireca"
    SITEMAP_INDEX_URL = "https://www.newswire.ca/sitemap-gz.xml"
    EARLIEST_MONTH = (2011, 1)  # Earliest month with a validated live sitemap

    _MONTH_ABBR = {
        1: "Jan",
        2: "Feb",
        3: "Mar",
        4: "Apr",
        5: "May",
        6: "Jun",
        7: "Jul",
        8: "Aug",
        9: "Sep",
        10: "Oct",
        11: "Nov",
        12: "Dec",
    }

    def date_from_url(self, url):
        """Newswire.ca URLs don't contain dates."""
        return None

    def press_release_url_filter(self, url):
        """Return whether a sitemap URL is a press release page."""
        return "/news-releases/" in url.lower()

    @staticmethod
    def _headline_from_slug(url):
        """Extract an approximate headline from a Newswire.ca URL slug.

        URL format:
            .../news-releases/{slug}-{numeric_id}.html

        Returns a title-cased string with the numeric suffix removed,
        or None if the URL doesn't match the expected pattern.
        """
        # Find the slug portion after /news-releases/
        idx = url.lower().find("/news-releases/")
        if idx < 0:
            return None
        slug = url[idx + len("/news-releases/") :]
        slug = slug.split("?", 1)[0].split("#", 1)[0]
        # Strip .html extension
        if slug.endswith(".html"):
            slug = slug[:-5]
        # Remove trailing numeric ID (e.g., -829648686)
        slug = re.sub(r"-\d{6,}$", "", slug)
        if not slug:
            return None
        slug = unquote(slug)
        # Convert hyphens to spaces and title-case
        return slug.replace("-", " ").strip().title()

    def _fetch_sitemap_xml(self, year, month):
        """Fetch and decompress the gzipped monthly sitemap.

        Same gz format as PR Newswire.  Returns decompressed XML bytes,
        or None on failure.
        """
        month_str = f"{year:04d}-{month:02d}"
        abbr = self._MONTH_ABBR.get(month)
        if abbr is None:
            return None

        gz_url = f"https://www.newswire.ca/Sitemap_Index_{abbr}_{year}.xml.gz"
        logger.info(f"{self.NAME}: fetching {gz_url}")
        resp = _fetch(gz_url, delay=SITEMAP_DELAY)

        if resp is not None:
            try:
                return gzip.decompress(resp.content)
            except Exception as e:
                logger.warning(f"{self.NAME}: decompress failed for {month_str}: {e}")

        logger.warning(f"{self.NAME}: no sitemap available for {month_str}")
        return None

    def _parse_headline_from_soup(self, soup, url):
        """Parse headline from a Newswire.ca article page (slow-path fallback)."""
        headline = None
        for selector in ["h1.release-header__title", "h1"]:
            tag = soup.select_one(selector)
            if tag and tag.get_text(strip=True):
                headline = tag.get_text(strip=True)
                break
        if not headline:
            return None
        pub_date = None
        for attr in [{"name": "date"}, {"property": "article:published_time"}]:
            meta = soup.find("meta", attrs=attr)
            if meta and meta.get("content"):
                pub_date = meta["content"][:10]
                break
        return {"headline": headline, "source_url": url, "date": pub_date}

    def parse_entries_from_xml(self, xml_bytes):
        """Extract entries from sitemap using URL slugs + lastmod dates.

        Newswire.ca sitemaps lack ``news:title``, so this method extracts
        approximate headlines from URL slugs and dates from ``<lastmod>``.
        Returns a list of {headline, source_url, date} dicts.
        """
        root = _xml_root(xml_bytes)
        if root is None:
            return []
        url_elems = root.findall("sm:url", SITEMAP_NS)
        if not url_elems:
            url_elems = root.findall("url")
        entries = []
        for url_elem in url_elems:
            loc = url_elem.find("sm:loc", SITEMAP_NS)
            if loc is None:
                loc = url_elem.find("loc")
            if loc is None or not loc.text:
                continue
            url = loc.text.strip()
            if "/news-releases/" not in url.lower():
                continue
            headline = self._headline_from_slug(url)
            if not headline:
                continue
            # Date from lastmod
            pub_date = None
            lastmod = url_elem.find("sm:lastmod", SITEMAP_NS)
            if lastmod is None:
                lastmod = url_elem.find("lastmod")
            if lastmod is not None and lastmod.text:
                pub_date = lastmod.text[:10]
            if pub_date:
                entries.append(
                    {"headline": headline, "source_url": url, "date": pub_date}
                )
        return entries

    def sitemap_urls_for_month(self, year, month):
        """Get press release URLs from gzipped monthly sitemaps."""
        month_str = f"{year:04d}-{month:02d}"
        xml_bytes = self._fetch_sitemap_xml(year, month)
        if xml_bytes is None:
            logger.warning(f"{self.NAME}: no sitemap available for {month_str}")
            return []
        urls = _parse_sitemap_urls(
            xml_bytes,
            url_filter=self.press_release_url_filter,
        )
        logger.info(f"{self.NAME}: {len(urls)} URLs for {month_str}")
        return urls

    def sitemap_entries_for_month(self, year, month):
        """Extract entries from monthly sitemap using slug extraction.

        Uses URL slug + lastmod fast path.  Returns None only if the
        sitemap is unavailable.
        """
        month_str = f"{year:04d}-{month:02d}"
        xml_bytes = self._fetch_sitemap_xml(year, month)
        if xml_bytes is None:
            return None
        entries = self.parse_entries_from_xml(xml_bytes)
        if entries:
            logger.info(
                f"{self.NAME}: {len(entries)} entries via slug extraction for {month_str}"
            )
            return entries
        logger.info(
            f"{self.NAME}: slug extraction yielded 0 entries for {month_str}; "
            f"falling back to per-page fetches"
        )
        return None


ALL_SCRAPERS = [PRNewswireScraper(), GlobeNewswireScraper(), NewswireCaScraper()]


# ---------------------------------------------------------------------------
# Hive-partitioned output helpers
# ---------------------------------------------------------------------------

_RAW_SCHEMA = {"headline": pl.Utf8, "source_url": pl.Utf8, "date": pl.Utf8}


def _day_parquet_path(base_dir, source_key, year, month, day):
    """Return Hive-partitioned path for a single day's data."""
    return (
        base_dir
        / f"source={source_key}"
        / f"year={year:04d}"
        / f"month={month:02d}"
        / f"day={day:02d}"
        / "data.parquet"
    )


def _completed_days(base_dir, source_key, year, month):
    """Return set of day ints that already have a parquet on disk."""
    month_dir = (
        base_dir / f"source={source_key}" / f"year={year:04d}" / f"month={month:02d}"
    )
    if not month_dir.exists():
        return set()
    days = set()
    for day_dir in month_dir.iterdir():
        if day_dir.is_dir() and day_dir.name.startswith("day="):
            parquet = day_dir / "data.parquet"
            if parquet.exists():
                try:
                    days.add(int(day_dir.name.split("=")[1]))
                except ValueError:
                    pass
    return days


def _save_day_parquet(headlines, base_dir, source_key, year, month, day):
    """Write a list of headline dicts as a single day parquet."""
    out_path = _day_parquet_path(base_dir, source_key, year, month, day)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame(headlines, schema=_RAW_SCHEMA)
    df.write_parquet(out_path)
    return out_path


def _month_complete_marker(base_dir, source_key, year, month):
    """Path for a marker file indicating a month has been fully crawled."""
    return (
        base_dir
        / f"source={source_key}"
        / f"year={year:04d}"
        / f"month={month:02d}"
        / ".complete"
    )


def _mark_month_complete(base_dir, source_key, year, month):
    """Write a marker file after a month is fully crawled."""
    marker = _month_complete_marker(base_dir, source_key, year, month)
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(f"crawled {year:04d}-{month:02d}\n")


def _is_month_complete(base_dir, source_key, year, month):
    """Check whether a month has already been fully crawled."""
    return _month_complete_marker(base_dir, source_key, year, month).exists()


# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------


class _GracefulShutdown:
    """Handle SIGINT/SIGTERM for clean exit during long crawls."""

    def __init__(self):
        self.shutdown_requested = False
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, signum, frame):
        sig_name = signal.Signals(signum).name
        if self.shutdown_requested:
            logger.warning(f"Second {sig_name} received, forcing exit")
            sys.exit(1)
        logger.info(
            f"{sig_name} received — finishing current scraper, then exiting. "
            f"Press Ctrl+C again to force quit."
        )
        self.shutdown_requested = True

    @property
    def should_stop(self):
        return self.shutdown_requested


# ---------------------------------------------------------------------------
# Full crawl (--full mode)
# ---------------------------------------------------------------------------


def _fetch_headlines_by_day(scraper, urls, done_days, shutdown):
    """Fetch page headlines concurrently and bucket by day-of-month.

    Uses a ThreadPoolExecutor so multiple pages are in-flight at once.
    Returns (headlines_by_day, pages_fetched). headlines_by_day is a
    defaultdict(list) mapping day int to list of headline dicts.
    """
    headlines_by_day = defaultdict(list)
    pages_fetched = 0
    lock = threading.Lock()
    total = len(urls)

    def _worker(url):
        session = _get_thread_session()
        resp = _fetch(url, delay=REQUEST_DELAY, session=session)
        if resp is None:
            return None
        soup = BeautifulSoup(resp.text, "lxml")
        return scraper._parse_headline_from_soup(soup, url)

    with ThreadPoolExecutor(max_workers=CONCURRENT_WORKERS) as pool:
        futures = {}
        for url in urls:
            if shutdown.should_stop:
                break
            futures[pool.submit(_worker, url)] = url

        for i, future in enumerate(as_completed(futures)):
            if shutdown.should_stop:
                logger.info(f"  {scraper.NAME}: interrupted at URL {i}/{total}")
                break
            result = future.result()
            pages_fetched += 1
            if result is not None and result["date"]:
                day = int(result["date"][8:10])
                with lock:
                    if day not in done_days:
                        headlines_by_day[day].append(result)
            if pages_fetched % 100 == 0:
                logger.info(
                    f"  {scraper.NAME}: {pages_fetched}/{total} pages "
                    f"({pages_fetched / total * 100:.0f}%)"
                )

    return headlines_by_day, pages_fetched


def _expected_days_in_month(year, month):
    """Number of days expected on disk for a fully-crawled month.

    For past months: calendar day count (28-31, leap-year aware).
    For the current month: today's day number (future days haven't happened).
    """
    total_days = calendar.monthrange(year, month)[1]
    today = date.today()
    if year == today.year and month == today.month:
        return today.day
    return total_days


def _crawl_scraper_for_month(scraper, year, month, output_dir, shutdown):
    """Crawl one scraper for one month. Save raw headlines partitioned by day.

    Fast path: if the scraper provides sitemap_entries_for_month(), extracts
    headlines directly from sitemap XML metadata (no per-page HTTP fetches).
    Slow path (fallback): fetches each press-release page concurrently.

    Skips days that already have a parquet on disk.
    Returns (pages_fetched, headlines_saved) counts, or None if interrupted.
    """
    source_key = scraper.SOURCE_KEY
    month_str = f"{year:04d}-{month:02d}"

    # If this month was already fully crawled (marker file exists), skip entirely
    if _is_month_complete(output_dir, source_key, year, month):
        done_days = _completed_days(output_dir, source_key, year, month)
        logger.info(
            f"  {scraper.NAME}: {month_str} already crawled "
            f"({len(done_days)} days on disk), skipping"
        )
        return (0, 0)

    done_days = _completed_days(output_dir, source_key, year, month)

    # If every expected day is already on disk, skip entirely
    expected_days = _expected_days_in_month(year, month)
    if len(done_days) >= expected_days:
        logger.info(
            f"  {scraper.NAME}: {month_str} complete "
            f"({len(done_days)}/{expected_days} days on disk), skipping"
        )
        _mark_month_complete(output_dir, source_key, year, month)
        return (0, 0)

    # Fetch the sitemap XML once (avoids double HTTP request)
    xml_bytes = scraper._fetch_sitemap_xml(year, month)
    if xml_bytes is None:
        logger.warning(f"{scraper.NAME}: no sitemap available for {month_str}")
        _mark_month_complete(output_dir, source_key, year, month)
        return (0, 0)

    # --- Fast path: headlines from sitemap metadata (no page fetches) ---
    # Prefer scraper-specific extraction (e.g. slug-based) if available,
    # otherwise fall back to standard Google News metadata parsing.
    if hasattr(scraper, "parse_entries_from_xml"):
        entries = scraper.parse_entries_from_xml(xml_bytes)
    else:
        entries = _parse_sitemap_news_entries(xml_bytes)
    if entries:
        logger.info(
            f"{scraper.NAME}: {len(entries)} entries with sitemap metadata for {month_str}"
        )
        month_prefix = f"{year:04d}-{month:02d}"
        valid = [
            e
            for e in entries
            if e.get("date", "").startswith(month_prefix)
            and int(e["date"][8:10]) not in done_days
        ]
        headlines_by_day = defaultdict(list)
        for e in valid:
            headlines_by_day[int(e["date"][8:10])].append(e)
        headlines_saved = 0
        for day, day_headlines in sorted(headlines_by_day.items()):
            _save_day_parquet(day_headlines, output_dir, source_key, year, month, day)
            headlines_saved += len(day_headlines)
        _mark_month_complete(output_dir, source_key, year, month)
        logger.info(
            f"  {scraper.NAME}: {month_str} done via sitemap metadata "
            f"({headlines_saved} headlines, 0 page fetches)"
        )
        return (0, headlines_saved)

    # --- Slow path: fetch each press-release page concurrently ---
    logger.info(
        f"{scraper.NAME}: no <news:title> in sitemap for {month_str}; "
        f"falling back to per-page fetches"
    )
    url_filter = getattr(
        scraper,
        "press_release_url_filter",
        lambda u: "/news-release" in u.lower(),
    )
    urls = _parse_sitemap_urls(xml_bytes, url_filter=url_filter)
    logger.info(f"{scraper.NAME}: {len(urls)} URLs for {month_str}")
    if not urls:
        _mark_month_complete(output_dir, source_key, year, month)
        return (0, 0)

    # For scrapers with date-in-URL: pre-filter to skip URLs for completed days
    can_prefilter = urls and scraper.date_from_url(urls[0]) is not None
    if can_prefilter and done_days:
        before = len(urls)
        urls = [u for u in urls if _day_int_from_url(scraper, u) not in done_days]
        logger.info(
            f"  {scraper.NAME}: skipped {before - len(urls)} URLs "
            f"for {len(done_days)} completed days, {len(urls)} remaining"
        )

    if not urls:
        logger.info(f"  {scraper.NAME}: all days complete for {month_str}")
        return (0, 0)

    headlines_by_day, pages_fetched = _fetch_headlines_by_day(
        scraper, urls, done_days, shutdown
    )

    # Save each day's headlines
    headlines_saved = 0
    for day, day_headlines in sorted(headlines_by_day.items()):
        if day in done_days:
            continue
        _save_day_parquet(day_headlines, output_dir, source_key, year, month, day)
        headlines_saved += len(day_headlines)

    if shutdown.should_stop:
        return None

    _mark_month_complete(output_dir, source_key, year, month)
    return (pages_fetched, headlines_saved)


def _day_int_from_url(scraper, url):
    """Extract day-of-month int from a URL, or -1 if not possible."""
    d = scraper.date_from_url(url)
    if d and len(d) >= 10:
        try:
            return int(d[8:10])
        except (ValueError, IndexError):
            pass
    return -1


def _setup_file_logging():
    """Add a file handler for long-running crawl logging."""
    log_path = DATA_DIR / "newswire_crawl.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(log_path, mode="a")
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s [%(funcName)s] %(message)s")
    )
    logging.getLogger().addHandler(file_handler)
    return log_path


def pull_newswire_full(
    start_date=FULL_START_DATE,
    end_date=None,
    data_dir=DATA_DIR,
    output_dir=None,
):
    """Crawl sitemaps month-by-month, extract ALL raw headlines, save by day.

    Output is Hive-partitioned:
        output_dir/source={key}/year=YYYY/month=MM/day=DD/data.parquet

    Completed days are skipped on re-run. Safe to Ctrl+C and resume.
    """
    if end_date is None:
        end_date = date.today().strftime("%Y-%m-%d")
    if output_dir is None:
        output_dir = NEWSWIRE_RAW_DIR
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    log_path = _setup_file_logging()
    shutdown = _GracefulShutdown()

    month_ranges = list(_generate_month_ranges(start_date, end_date))
    total_months = len(month_ranges)

    logger.info(
        f"Full newswire crawl: {start_date} to {end_date} ({total_months} months)"
    )
    logger.info(f"Scrapers: {', '.join(s.NAME for s in ALL_SCRAPERS)}")
    logger.info(f"Output: {output_dir}")
    logger.info(f"Log file: {log_path}")
    logger.info("Completed days are skipped on re-run. Ctrl+C to stop safely.\n")

    completed_months = 0
    total_pages = 0
    total_headlines = 0

    for i, (m_start, m_end) in enumerate(month_ranges, 1):
        year = int(m_start[:4])
        month = int(m_start[5:7])
        month_str = f"{year:04d}-{month:02d}"

        logger.info(f"[{i}/{total_months}] {month_str}")

        month_pages = 0
        month_headlines = 0

        for scraper in ALL_SCRAPERS:
            if shutdown.should_stop:
                break

            # Skip months before the scraper's earliest coverage
            earliest = getattr(scraper, "EARLIEST_MONTH", None)
            if earliest and (year, month) < earliest:
                continue

            result = _crawl_scraper_for_month(
                scraper, year, month, output_dir, shutdown
            )
            if result is None:
                # Interrupted
                break
            pages, headlines = result
            month_pages += pages
            month_headlines += headlines

        if shutdown.should_stop:
            logger.info(
                f"\nShutdown after [{i}/{total_months}] {month_str}. "
                f"Re-run to resume — completed days will be skipped."
            )
            break

        total_pages += month_pages
        total_headlines += month_headlines
        completed_months += 1

        if month_pages > 0:
            logger.info(
                f"[{i}/{total_months}] {month_str}: "
                f"{month_headlines:,} headlines from {month_pages:,} pages"
            )

        # Periodic maintenance
        if completed_months % 5 == 0:
            _reset_session()
            gc.collect()

    logger.info(
        f"\nCrawl {'interrupted' if shutdown.should_stop else 'finished'}. "
        f"Months processed: {completed_months}/{total_months}. "
        f"Total: {total_headlines:,} headlines from {total_pages:,} pages."
    )


# ---------------------------------------------------------------------------
# Sample month (default mode)
# ---------------------------------------------------------------------------


def pull_newswire_sample_month(month=None, data_dir=DATA_DIR):
    """Crawl sitemaps for a single month, saving raw daily-partitioned parquets.

    This is the same pipeline as --full, just scoped to one month.
    Output goes to the same newswire_headlines/ directory.

    Parameters
    ----------
    month : str, optional
        'YYYY-MM' month to pull.  Defaults to SAMPLE_MONTH from the GDELT pipeline.
    data_dir : Path
        Root data directory.
    """
    if month is None:
        month = SAMPLE_MONTH
    year = int(month[:4])
    mo = int(month[5:7])

    # Use the first day of the month as start, first day of next month as end
    start_date = f"{year:04d}-{mo:02d}-01"
    if mo == 12:
        end_date = f"{year + 1:04d}-01-01"
    else:
        end_date = f"{year:04d}-{mo + 1:02d}-01"

    pull_newswire_full(
        start_date=start_date,
        end_date=end_date,
        data_dir=data_dir,
    )


# ---------------------------------------------------------------------------
# Convenience loaders
# ---------------------------------------------------------------------------


def load_newswire_headlines(data_dir=DATA_DIR):
    """Lazy-scan the raw Hive-partitioned newswire headlines directory.

    Returns a LazyFrame with columns: headline, source_url, date,
    plus Hive partition columns: source, year, month, day.
    """
    return pl.scan_parquet(
        Path(data_dir) / "newswire_headlines" / "**" / "*.parquet",
        hive_partitioning=True,
    )


# ---------------------------------------------------------------------------
# Status reporting
# ---------------------------------------------------------------------------


def _print_status(output_dir=None):
    """Print crawl progress by counting completed day-parquets on disk."""
    if output_dir is None:
        output_dir = NEWSWIRE_RAW_DIR
    output_dir = Path(output_dir)

    if not output_dir.exists():
        print(f"No data found at {output_dir}")
        return

    print(f"Crawl output: {output_dir}\n")

    for source_dir in sorted(output_dir.iterdir()):
        if not source_dir.is_dir() or not source_dir.name.startswith("source="):
            continue
        source_name = source_dir.name.split("=", 1)[1]

        day_count = 0
        months = set()
        for parquet in source_dir.rglob("data.parquet"):
            day_count += 1
            # Extract year/month from path
            parts = parquet.relative_to(source_dir).parts
            ym = "/".join(parts[:2])  # year=YYYY/month=MM
            months.add(ym)

        print(f"  {source_name}: {day_count} days across {len(months)} months")

    # Also check log file
    log_path = DATA_DIR / "newswire_crawl.log"
    if log_path.exists():
        size_mb = log_path.stat().st_size / (1024 * 1024)
        print(f"\n  Log file: {log_path} ({size_mb:.1f} MB)")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pull free newswire headlines via sitemap crawling.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python pull_free_newswires.py                           # sample month\n"
            "  python pull_free_newswires.py --full                    # 2020 to today\n"
            "  python pull_free_newswires.py --full --start 2023-01-01 # from 2023\n"
            "  python pull_free_newswires.py --status                  # show progress\n"
        ),
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help=(
            "Long-running crawl of all newswires (2020 to present). "
            "Saves raw headlines. Resumable — safe to Ctrl+C."
        ),
    )
    parser.add_argument(
        "--month",
        type=str,
        default=None,
        help=f"Pull a single YYYY-MM month (default: {SAMPLE_MONTH}).",
    )
    parser.add_argument(
        "--start",
        type=str,
        default=FULL_START_DATE,
        help=f"Start date for --full mode (default: {FULL_START_DATE}).",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="End date for --full mode (default: today).",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Print crawl progress and exit.",
    )
    args = parser.parse_args()

    if args.status:
        _print_status()
    elif args.full:
        pull_newswire_full(start_date=args.start, end_date=args.end)
    else:
        pull_newswire_sample_month(month=args.month or SAMPLE_MONTH)
