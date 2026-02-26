"""
Pull free newswire headlines from PR Newswire, Business Wire, and GlobeNewswire.

Scrapes press releases from three major free wire services via sitemap
crawling. Raw headlines are saved to a Hive-partitioned data lake:

    newswire_headlines/source={key}/year=YYYY/month=MM/day=DD/data.parquet

S&P 500 filtering is done downstream in analysis notebooks.

Two modes:
  Default    — crawl a single sample month (default: SAMPLE_MONTH).
  --full     — long-running crawl (days/weeks), 2020 to present.

Both modes write to the same output directory with the same daily
partitioning. Completed days are skipped on re-run. Safe to Ctrl+C.

"""

import argparse
import gc
import gzip
import logging
import re
import signal
import sys
import time
from collections import defaultdict
from datetime import date
from pathlib import Path

import polars as pl
import requests
from bs4 import BeautifulSoup
from lxml import etree

from pull_gdelt_sp500_headlines import SAMPLE_MONTH, _generate_month_ranges
from settings import config

DATA_DIR = Path(config("DATA_DIR"))
NEWSWIRE_RAW_DIR = DATA_DIR / "newswire_headlines"
FULL_START_DATE = "2020-01-01"

REQUEST_DELAY = 1.0  # seconds between page requests
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


def _get_session():
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update(HEADERS)
    return _session


def _reset_session():
    global _session
    if _session is not None:
        _session.close()
    _session = None


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
    """Parse a sitemap index XML → list of child sitemap URLs."""
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
    """Parse a sitemap XML → list of page URLs, optionally filtered."""
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


# ---------------------------------------------------------------------------
# Scraper classes
# ---------------------------------------------------------------------------


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
            "date": pub_date,
        }

    def sitemap_urls_for_month(self, year, month):
        """Get press release URLs from gzipped monthly sitemaps.

        PR Newswire hosts compressed sitemaps at e.g.
        ``Sitemap_Index_Jan_2025.xml.gz``, listed in ``sitemap-gz.xml``.
        """
        month_str = f"{year:04d}-{month:02d}"
        abbr = self._MONTH_ABBR.get(month)
        if abbr is None:
            logger.warning(f"{self.NAME}: invalid month {month}")
            return []

        # Direct URL for the gzipped monthly sitemap
        gz_url = f"https://www.prnewswire.com/Sitemap_Index_{abbr}_{year}.xml.gz"
        logger.info(f"{self.NAME}: fetching {gz_url}")

        resp = _fetch(gz_url, delay=SITEMAP_DELAY)
        if resp is None:
            logger.warning(f"{self.NAME}: failed to fetch gz sitemap for {month_str}")
            return []

        try:
            xml_bytes = gzip.decompress(resp.content)
        except Exception as e:
            logger.warning(f"{self.NAME}: failed to decompress gz sitemap: {e}")
            return []

        urls = _parse_sitemap_urls(
            xml_bytes,
            url_filter=lambda u: "/news-release" in u.lower(),
        )
        logger.info(f"{self.NAME}: {len(urls)} URLs for {month_str}")
        return urls


class BusinessWireScraper:
    """Scraper for Business Wire (businesswire.com)."""

    NAME = "Business Wire"
    SOURCE_KEY = "businesswire"
    SITEMAP_INDEX_URL = "https://www.businesswire.com/sitemap-index.xml"

    _BW_DATE_RE = re.compile(r"/(\d{8})\d+/en/")

    def date_from_url(self, url):
        """Extract YYYY-MM-DD from Business Wire URL timestamp."""
        m = self._BW_DATE_RE.search(url)
        if m:
            d = m.group(1)
            return f"{d[:4]}-{d[4:6]}-{d[6:8]}"
        return None

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
        pub_date = self.date_from_url(url)
        if not pub_date:
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
            "date": pub_date,
        }

    def sitemap_urls_for_month(self, year, month):
        resp = _fetch(self.SITEMAP_INDEX_URL, delay=SITEMAP_DELAY)
        if resp is None:
            logger.warning(f"{self.NAME}: failed to fetch sitemap index")
            return []
        child_sitemaps = _parse_sitemap_index(resp.content)
        month_str = f"{year:04d}-{month:02d}"
        month_str_alt = f"{year:04d}{month:02d}"
        relevant = [u for u in child_sitemaps if month_str in u or month_str_alt in u]
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
                resp.content,
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
    SOURCE_KEY = "globenewswire"
    SITEMAP_URL = "https://www.globenewswire.com/en/Newsroom/GoogleSitemap"

    _GNW_DATE_RE = re.compile(r"/news-release/(\d{4}/\d{2}/\d{2})/")

    def date_from_url(self, url):
        """Extract YYYY-MM-DD from GlobeNewswire URL path."""
        m = self._GNW_DATE_RE.search(url)
        if m:
            return m.group(1).replace("/", "-")
        return None

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
        pub_date = self.date_from_url(url)
        if not pub_date:
            meta = soup.find("meta", attrs={"property": "article:published_time"})
            if meta and meta.get("content"):
                pub_date = meta["content"][:10]
        return {
            "headline": headline,
            "source_url": url,
            "date": pub_date,
        }

    def sitemap_urls_for_month(self, year, month):
        """Get press release URLs from Google News sitemap.

        Note: GlobeNewswire's sitemap only contains recent articles (~2 days).
        For historical months this will return 0 URLs.
        """
        month_str = f"{year:04d}-{month:02d}"
        resp = _fetch(self.SITEMAP_URL, delay=SITEMAP_DELAY)
        if resp is None:
            logger.warning(f"{self.NAME}: failed to fetch news sitemap")
            return []

        month_prefix = f"/{year:04d}/{month:02d}/"
        urls = _parse_sitemap_urls(
            resp.content,
            url_filter=lambda u, _mp=month_prefix: "/news-release/" in u and _mp in u,
        )
        logger.info(f"{self.NAME}: {len(urls)} URLs for {month_str}")
        if not urls:
            logger.info(
                f"{self.NAME}: Google News sitemap only has recent articles; "
                f"{month_str} may not be available via sitemap"
            )
        return urls


ALL_SCRAPERS = [PRNewswireScraper(), BusinessWireScraper(), GlobeNewswireScraper()]


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
    """Fetch page headlines and bucket by day-of-month.

    Returns (headlines_by_day, pages_fetched). headlines_by_day is a
    defaultdict(list) mapping day int to list of headline dicts.
    """
    headlines_by_day = defaultdict(list)
    pages_fetched = 0

    for i, url in enumerate(urls):
        if shutdown.should_stop:
            logger.info(f"  {scraper.NAME}: interrupted at URL {i}/{len(urls)}")
            break

        result = scraper.fetch_page_headline(url)
        pages_fetched += 1

        if result is not None and result["date"]:
            day = int(result["date"][8:10])
            if day not in done_days:
                headlines_by_day[day].append(result)

        if (i + 1) % 100 == 0:
            logger.info(
                f"  {scraper.NAME}: {i + 1}/{len(urls)} pages "
                f"({(i + 1) / len(urls) * 100:.0f}%)"
            )

    return headlines_by_day, pages_fetched


def _crawl_scraper_for_month(scraper, year, month, output_dir, shutdown):
    """Crawl one scraper for one month. Save raw headlines partitioned by day.

    Skips days that already have a parquet on disk.
    Returns (pages_fetched, headlines_saved) counts, or None if interrupted.
    """
    source_key = scraper.SOURCE_KEY
    month_str = f"{year:04d}-{month:02d}"
    done_days = _completed_days(output_dir, source_key, year, month)

    # Fetch sitemap URLs
    urls = scraper.sitemap_urls_for_month(year, month)
    if not urls:
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

    # For PR Newswire (no date in URL): if all ~31 possible days exist, skip
    if not can_prefilter and len(done_days) >= 28:
        logger.info(
            f"  {scraper.NAME}: {len(done_days)} days already on disk "
            f"for {month_str}, fetching remaining pages to check for new days"
        )

    headlines_by_day, pages_fetched = _fetch_headlines_by_day(
        scraper, urls, done_days, shutdown
    )

    # Save each day's headlines
    headlines_saved = 0
    for day, day_headlines in sorted(headlines_by_day.items()):
        if day in done_days:
            continue
        # Only save if we're NOT interrupted (avoid partial days)
        # Exception: if we were interrupted, still save completed days
        # A day is "complete" if we processed all URLs for it.
        # For simplicity: always save — partial data is still useful,
        # and the user can --reset-month to re-crawl if needed.
        _save_day_parquet(day_headlines, output_dir, source_key, year, month, day)
        headlines_saved += len(day_headlines)

    if shutdown.should_stop:
        return None

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
