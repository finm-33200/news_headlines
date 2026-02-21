"""
Pull free newswire headlines from PR Newswire, Business Wire, and GlobeNewswire.

Scrapes press releases from three major free wire services and filters to
S&P 500 companies using substring matching against normalized company names.

Two modes:
  Default  — pull recent headlines via RSS feeds (fast, for development)
  --full   — crawl sitemaps month-by-month (slow, days for full history)

Prerequisites:
- sp500_names_lookup.parquet already built (run pull_sp500_constituents.py first)
"""

import argparse
import logging
import re
import time
from datetime import date, datetime
from pathlib import Path
from xml.etree import ElementTree

import polars as pl
import requests
from bs4 import BeautifulSoup

try:
    import feedparser
except ImportError:
    feedparser = None

from pull_gdelt_sp500_headlines import _generate_month_ranges, _month_filename
from pull_sp500_constituents import load_sp500_names_lookup, normalize_company_name
from settings import config

DATA_DIR = Path(config("DATA_DIR"))
NEWSWIRE_SP500_DIR = DATA_DIR / "newswire_sp500_headlines"

REQUEST_DELAY = 1.0  # seconds between page requests
SITEMAP_DELAY = 0.5  # seconds between sitemap requests
MAX_RETRIES = 3

HEADERS = {
    "User-Agent": "FINM33200-CourseProject/1.0 (University of Chicago; academic research)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------

_session = None


def _get_session():
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update(HEADERS)
    return _session


def _fetch(url, delay=REQUEST_DELAY):
    """Fetch a URL with rate limiting, retries, and exponential backoff.

    Returns the Response on success, or None on 404 / permanent failure.
    """
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
                wait = (2 ** attempt) * 10
                logger.warning(f"429 rate limited, waiting {wait}s: {url}")
                time.sleep(wait)
            else:
                logger.warning(f"HTTP {resp.status_code}: {url}")
                if resp.status_code >= 500:
                    time.sleep(2 ** attempt)
                else:
                    return None
        except requests.RequestException as e:
            logger.warning(
                f"Request error (attempt {attempt + 1}/{MAX_RETRIES}): {e}"
            )
            time.sleep(2 ** attempt)
    logger.error(f"Failed after {MAX_RETRIES} retries: {url}")
    return None


# ---------------------------------------------------------------------------
# Sitemap XML parsing
# ---------------------------------------------------------------------------

SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


def _parse_sitemap_index(xml_text):
    """Parse a sitemap index XML → list of child sitemap URLs."""
    try:
        root = ElementTree.fromstring(xml_text)
    except ElementTree.ParseError as e:
        logger.warning(f"Failed to parse sitemap index XML: {e}")
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
    """Parse a sitemap XML → list of page URLs, optionally filtered."""
    try:
        root = ElementTree.fromstring(xml_text)
    except ElementTree.ParseError as e:
        logger.warning(f"Failed to parse sitemap XML: {e}")
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


# ---------------------------------------------------------------------------
# RSS date parsing helper
# ---------------------------------------------------------------------------

_RSS_DATE_FORMATS = [
    "%a, %d %b %Y %H:%M:%S %Z",
    "%a, %d %b %Y %H:%M:%S %z",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S",
]


def _parse_rss_date(date_str):
    """Best-effort parse of an RSS date string → 'YYYY-MM-DD' or None."""
    if not date_str:
        return None
    for fmt in _RSS_DATE_FORMATS:
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    # Fallback: grab first 10 chars if they look date-like
    if len(date_str) >= 10 and date_str[4] == "-":
        return date_str[:10]
    return None


# ---------------------------------------------------------------------------
# Scraper classes
# ---------------------------------------------------------------------------


class PRNewswireScraper:
    """Scraper for PR Newswire (prnewswire.com)."""

    NAME = "PR Newswire"
    RSS_URL = "https://www.prnewswire.com/rss/news-releases-list.rss"
    SITEMAP_INDEX_URL = "https://www.prnewswire.com/sitemap-news-releases-index.xml"

    def fetch_rss_headlines(self):
        """Fetch recent headlines from RSS feed."""
        if feedparser is None:
            logger.warning("feedparser not installed, skipping RSS for PR Newswire")
            return []
        resp = _fetch(self.RSS_URL, delay=0.5)
        if resp is None:
            logger.warning(f"Failed to fetch RSS from {self.NAME}")
            return []
        feed = feedparser.parse(resp.text)
        results = []
        for entry in feed.entries:
            headline = entry.get("title", "").strip()
            link = entry.get("link", "").strip()
            published = entry.get("published", "")
            if headline and link:
                results.append({
                    "headline": headline,
                    "source_url": link,
                    "source_name": self.NAME,
                    "date": _parse_rss_date(published),
                })
        logger.info(f"{self.NAME} RSS: {len(results)} headlines")
        return results

    def fetch_page_headline(self, url):
        """Fetch one press release page → {headline, source_url, source_name, date}."""
        resp = _fetch(url)
        if resp is None:
            return None
        soup = BeautifulSoup(resp.text, "lxml")
        headline = None
        for selector in ["h1.release-header__title", "h1"]:
            tag = soup.select_one(selector)
            if tag and tag.get_text(strip=True):
                headline = tag.get_text(strip=True)
                break
        if not headline:
            return None
        pub_date = None
        for attr in [
            {"name": "date"},
            {"property": "article:published_time"},
        ]:
            meta = soup.find("meta", attrs=attr)
            if meta and meta.get("content"):
                pub_date = meta["content"][:10]
                break
        return {
            "headline": headline,
            "source_url": url,
            "source_name": self.NAME,
            "date": pub_date,
        }

    def sitemap_urls_for_month(self, year, month):
        """Get press release URLs from sitemaps for a given month."""
        resp = _fetch(self.SITEMAP_INDEX_URL, delay=SITEMAP_DELAY)
        if resp is None:
            logger.warning(f"{self.NAME}: failed to fetch sitemap index")
            return []
        child_sitemaps = _parse_sitemap_index(resp.text)
        month_str = f"{year:04d}-{month:02d}"
        month_str_alt = f"{year:04d}{month:02d}"
        relevant = [
            u for u in child_sitemaps if month_str in u or month_str_alt in u
        ]
        if not relevant:
            logger.info(
                f"{self.NAME}: no sitemaps matched {month_str}, "
                f"trying all {len(child_sitemaps)} child sitemaps"
            )
            relevant = child_sitemaps
        month_prefix = f"/{year:04d}/{month:02d}/"
        urls = []
        for sitemap_url in relevant:
            resp = _fetch(sitemap_url, delay=SITEMAP_DELAY)
            if resp is None:
                continue
            page_urls = _parse_sitemap_urls(
                resp.text,
                url_filter=lambda u, _mp=month_prefix: (
                    "/news-release" in u.lower() and _mp in u
                ),
            )
            urls.extend(page_urls)
        logger.info(f"{self.NAME}: {len(urls)} URLs for {month_str}")
        return urls


class BusinessWireScraper:
    """Scraper for Business Wire (businesswire.com)."""

    NAME = "Business Wire"
    RSS_URL = "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeEFpRWQ=="
    SITEMAP_INDEX_URL = "https://www.businesswire.com/sitemap-index.xml"

    def fetch_rss_headlines(self):
        if feedparser is None:
            logger.warning("feedparser not installed, skipping RSS for Business Wire")
            return []
        resp = _fetch(self.RSS_URL, delay=0.5)
        if resp is None:
            logger.warning(f"Failed to fetch RSS from {self.NAME}")
            return []
        feed = feedparser.parse(resp.text)
        results = []
        for entry in feed.entries:
            headline = entry.get("title", "").strip()
            link = entry.get("link", "").strip()
            published = entry.get("published", "")
            if headline and link:
                results.append({
                    "headline": headline,
                    "source_url": link,
                    "source_name": self.NAME,
                    "date": _parse_rss_date(published),
                })
        logger.info(f"{self.NAME} RSS: {len(results)} headlines")
        return results

    def fetch_page_headline(self, url):
        resp = _fetch(url)
        if resp is None:
            return None
        soup = BeautifulSoup(resp.text, "lxml")
        headline = None
        for selector in ["h1.epi-fontLg", "h1.bw-release-title", "h1"]:
            tag = soup.select_one(selector)
            if tag and tag.get_text(strip=True):
                headline = tag.get_text(strip=True)
                break
        if not headline:
            return None
        pub_date = None
        meta = soup.find("meta", attrs={"property": "article:published_time"})
        if meta and meta.get("content"):
            pub_date = meta["content"][:10]
        if not pub_date:
            time_tag = soup.find("time")
            if time_tag and time_tag.get("datetime"):
                pub_date = time_tag["datetime"][:10]
        return {
            "headline": headline,
            "source_url": url,
            "source_name": self.NAME,
            "date": pub_date,
        }

    def sitemap_urls_for_month(self, year, month):
        resp = _fetch(self.SITEMAP_INDEX_URL, delay=SITEMAP_DELAY)
        if resp is None:
            logger.warning(f"{self.NAME}: failed to fetch sitemap index")
            return []
        child_sitemaps = _parse_sitemap_index(resp.text)
        month_str = f"{year:04d}-{month:02d}"
        month_str_alt = f"{year:04d}{month:02d}"
        relevant = [
            u for u in child_sitemaps if month_str in u or month_str_alt in u
        ]
        if not relevant:
            relevant = child_sitemaps
        month_prefix = f"/{year:04d}/{month:02d}/"
        date_prefix = f"{year:04d}{month:02d}"
        urls = []
        for sitemap_url in relevant:
            resp = _fetch(sitemap_url, delay=SITEMAP_DELAY)
            if resp is None:
                continue
            page_urls = _parse_sitemap_urls(
                resp.text,
                url_filter=lambda u, _mp=month_prefix, _dp=date_prefix: (
                    (_mp in u or _dp in u) and "news" in u.lower()
                ),
            )
            urls.extend(page_urls)
        logger.info(f"{self.NAME}: {len(urls)} URLs for {month_str}")
        return urls


class GlobeNewswireScraper:
    """Scraper for GlobeNewswire (globenewswire.com)."""

    NAME = "GlobeNewswire"
    RSS_URL = "https://www.globenewswire.com/rss/list"
    SITEMAP_INDEX_URL = "https://www.globenewswire.com/sitemap-index.xml"

    def fetch_rss_headlines(self):
        if feedparser is None:
            logger.warning("feedparser not installed, skipping RSS for GlobeNewswire")
            return []
        resp = _fetch(self.RSS_URL, delay=0.5)
        if resp is None:
            logger.warning(f"Failed to fetch RSS from {self.NAME}")
            return []
        feed = feedparser.parse(resp.text)
        results = []
        for entry in feed.entries:
            headline = entry.get("title", "").strip()
            link = entry.get("link", "").strip()
            published = entry.get("published", "")
            if headline and link:
                results.append({
                    "headline": headline,
                    "source_url": link,
                    "source_name": self.NAME,
                    "date": _parse_rss_date(published),
                })
        logger.info(f"{self.NAME} RSS: {len(results)} headlines")
        return results

    def fetch_page_headline(self, url):
        resp = _fetch(url)
        if resp is None:
            return None
        soup = BeautifulSoup(resp.text, "lxml")
        headline = None
        for selector in ["h1.article-headline", "h1.main-title", "h1"]:
            tag = soup.select_one(selector)
            if tag and tag.get_text(strip=True):
                headline = tag.get_text(strip=True)
                break
        if not headline:
            return None
        # Try extracting date from URL: /news-release/YYYY/MM/DD/...
        pub_date = None
        url_match = re.search(r"/news-release/(\d{4}/\d{2}/\d{2})/", url)
        if url_match:
            pub_date = url_match.group(1).replace("/", "-")
        if not pub_date:
            meta = soup.find("meta", attrs={"property": "article:published_time"})
            if meta and meta.get("content"):
                pub_date = meta["content"][:10]
        return {
            "headline": headline,
            "source_url": url,
            "source_name": self.NAME,
            "date": pub_date,
        }

    def sitemap_urls_for_month(self, year, month):
        resp = _fetch(self.SITEMAP_INDEX_URL, delay=SITEMAP_DELAY)
        if resp is None:
            logger.warning(f"{self.NAME}: failed to fetch sitemap index")
            return []
        child_sitemaps = _parse_sitemap_index(resp.text)
        month_str = f"{year:04d}-{month:02d}"
        month_str_alt = f"{year:04d}{month:02d}"
        relevant = [
            u for u in child_sitemaps if month_str in u or month_str_alt in u
        ]
        if not relevant:
            relevant = child_sitemaps
        month_prefix = f"/{year:04d}/{month:02d}/"
        urls = []
        for sitemap_url in relevant:
            resp = _fetch(sitemap_url, delay=SITEMAP_DELAY)
            if resp is None:
                continue
            page_urls = _parse_sitemap_urls(
                resp.text,
                url_filter=lambda u, _mp=month_prefix: (
                    "/news-release/" in u and _mp in u
                ),
            )
            urls.extend(page_urls)
        logger.info(f"{self.NAME}: {len(urls)} URLs for {month_str}")
        return urls


ALL_SCRAPERS = [PRNewswireScraper(), BusinessWireScraper(), GlobeNewswireScraper()]


# ---------------------------------------------------------------------------
# S&P 500 filtering
# ---------------------------------------------------------------------------


def _filter_to_sp500(df: pl.DataFrame, data_dir=DATA_DIR) -> pl.DataFrame:
    """Filter headlines to those mentioning S&P 500 companies.

    Normalizes each headline using the same function applied to company names,
    then checks for substring matches against comnam_norm from the lookup table.
    """
    lookup = pl.from_pandas(load_sp500_names_lookup(data_dir))

    # Build regex alternation from normalized company names, longest first
    names = lookup["comnam_norm"].unique().to_list()
    names = sorted([n for n in names if n], key=len, reverse=True)
    escaped = [re.escape(n) for n in names]
    pattern = "(" + "|".join(escaped) + ")"

    # Normalize headlines
    df = df.with_columns(
        pl.col("headline")
        .map_elements(normalize_company_name, return_dtype=pl.Utf8)
        .alias("headline_norm")
    )

    # Filter to headlines containing any S&P 500 company name
    matched = df.filter(pl.col("headline_norm").str.contains(pattern))

    if len(matched) == 0:
        return pl.DataFrame(
            schema={
                "date": pl.Utf8,
                "headline": pl.Utf8,
                "source_url": pl.Utf8,
                "source_name": pl.Utf8,
                "matched_company": pl.Utf8,
                "permno": pl.Float64,
                "ticker": pl.Utf8,
            }
        )

    # Extract the first matched company name
    matched = matched.with_columns(
        pl.col("headline_norm")
        .str.extract(pattern, group_index=1)
        .alias("matched_norm")
    )

    # Join to get company details (deduplicate lookup to avoid fan-out)
    lookup_dedup = lookup.select(
        "comnam_norm", "comnam", "permno", "ticker"
    ).unique(subset=["comnam_norm"], keep="first")

    result = matched.join(
        lookup_dedup,
        left_on="matched_norm",
        right_on="comnam_norm",
        how="left",
    ).rename({"comnam": "matched_company"})

    result = result.drop("headline_norm", "matched_norm")

    return result


# ---------------------------------------------------------------------------
# Main pull functions
# ---------------------------------------------------------------------------


def pull_newswire_sample(data_dir=DATA_DIR):
    """Fetch recent headlines via RSS from all 3 wire services, filter to S&P 500.

    Returns a Polars DataFrame with S&P 500–matched headlines.
    """
    all_headlines = []
    for scraper in ALL_SCRAPERS:
        headlines = scraper.fetch_rss_headlines()
        all_headlines.extend(headlines)

    if not all_headlines:
        logger.warning("No headlines fetched from any RSS feed")
        return pl.DataFrame()

    df = pl.DataFrame(all_headlines)
    print(f"Total RSS headlines: {len(df):,}")

    df = df.unique(subset=["headline"])
    print(f"After dedup: {len(df):,}")

    filtered = _filter_to_sp500(df, data_dir)
    print(f"S&P 500 matches: {len(filtered):,}")

    return filtered


def pull_newswire_full(
    start_date="2020-01-01",
    end_date=None,
    data_dir=DATA_DIR,
    output_dir=None,
):
    """Crawl sitemaps month-by-month, extract headlines, filter to S&P 500.

    Each month is written as a separate parquet file. Months already on disk
    are skipped for resumability.
    """
    if end_date is None:
        end_date = date.today().strftime("%Y-%m-%d")
    if output_dir is None:
        output_dir = NEWSWIRE_SP500_DIR
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    month_ranges = list(_generate_month_ranges(start_date, end_date))
    total_months = len(month_ranges)

    print(f"Full newswire pull: {start_date} to {end_date} ({total_months} months)")
    print("Completed months are skipped on re-run.")
    print(f"Output directory: {output_dir}\n")

    for i, (m_start, m_end) in enumerate(month_ranges, 1):
        out_path = output_dir / _month_filename(m_start)
        if out_path.exists():
            print(f"  [{i}/{total_months}] {m_start[:7]}: already exists, skipping")
            continue

        year = int(m_start[:4])
        month = int(m_start[5:7])

        print(f"  [{i}/{total_months}] {m_start[:7]}: fetching sitemap URLs...")
        all_headlines = []
        for scraper in ALL_SCRAPERS:
            urls = scraper.sitemap_urls_for_month(year, month)
            for j, url in enumerate(urls):
                if (j + 1) % 100 == 0:
                    print(f"    {scraper.NAME}: {j + 1}/{len(urls)} pages...")
                result = scraper.fetch_page_headline(url)
                if result is not None:
                    all_headlines.append(result)

        if not all_headlines:
            empty_df = pl.DataFrame(
                schema={
                    "date": pl.Utf8,
                    "headline": pl.Utf8,
                    "source_url": pl.Utf8,
                    "source_name": pl.Utf8,
                    "matched_company": pl.Utf8,
                    "permno": pl.Float64,
                    "ticker": pl.Utf8,
                }
            )
            empty_df.write_parquet(out_path)
            print(f"  [{i}/{total_months}] {m_start[:7]}: 0 headlines")
            continue

        df = pl.DataFrame(all_headlines).unique(subset=["headline"])
        filtered = _filter_to_sp500(df, data_dir)
        filtered.write_parquet(out_path)
        print(
            f"  [{i}/{total_months}] {m_start[:7]}: "
            f"{len(filtered):,} S&P 500 headlines (from {len(df):,} total)"
        )

    print(f"\nDone. Monthly parquets are in {output_dir}")


# ---------------------------------------------------------------------------
# Convenience loaders
# ---------------------------------------------------------------------------


def load_newswire_sp500_headlines(data_dir=DATA_DIR):
    """Lazy-scan the full partitioned newswire S&P 500 headlines directory."""
    return pl.scan_parquet(
        Path(data_dir) / "newswire_sp500_headlines" / "*.parquet"
    )


def load_newswire_sp500_headlines_sample(data_dir=DATA_DIR):
    """Load the newswire S&P 500 headlines sample."""
    return pl.read_parquet(
        Path(data_dir) / "newswire_sp500_headlines_sample.parquet"
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pull free newswire headlines filtered to S&P 500 companies."
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help=(
            "Pull full dataset via sitemap crawling (2020 to present) instead "
            "of a recent RSS sample. Very slow but resumable."
        ),
    )
    args = parser.parse_args()

    if args.full:
        pull_newswire_full()
    else:
        df = pull_newswire_sample()
        path = DATA_DIR / "newswire_sp500_headlines_sample.parquet"
        df.write_parquet(path)
        print(f"Saved to {path}")
