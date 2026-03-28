"""
Discover Wayback Machine timestamps for missing PR Newswire monthly sitemaps.

Iterates over months where PR Newswire's live gzipped sitemaps return 404,
queries the Wayback CDX API for archived snapshots, downloads the best
candidate, and validates that it decompresses and contains press-release URLs.

Outputs a Python dict literal suitable for pasting into _WAYBACK_TIMESTAMPS
in pull_free_newswires.py.

Usage:
    # Discover all missing months (2012-07 through 2019-12)
    python src/discover_wayback_timestamps.py

    # Discover a specific range
    python src/discover_wayback_timestamps.py --start 2015-01 --end 2016-01

    # Use a local cache directory for resumability
    python src/discover_wayback_timestamps.py --cache-dir _data/wayback_cache

    # Dry-run: only query CDX, don't download/validate
    python src/discover_wayback_timestamps.py --dry-run
"""

import argparse
import gzip
import json
import logging
import sys
import time
from pathlib import Path

import requests
from lxml import etree

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

CDX_API = "https://web.archive.org/cdx/search/cdx"
CDX_DELAY = 1.0  # seconds between CDX queries
DOWNLOAD_DELAY = 2.0  # seconds between archive downloads
MAX_RETRIES = 3

HEADERS = {
    "User-Agent": "FINM33200-CourseProject/1.0 (University of Chicago; academic research)",
}

MONTH_ABBR = {
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

# Default range: the known gap in PR Newswire on-disk data
DEFAULT_START = "2012-07"
DEFAULT_END = "2019-12"


def _sitemap_gz_url(year, month):
    """Construct the PR Newswire gzipped sitemap URL for a given month."""
    abbr = MONTH_ABBR[month]
    return f"https://www.prnewswire.com/Sitemap_Index_{abbr}_{year}.xml.gz"


def _query_cdx(url, session):
    """Query Wayback CDX API for all archived snapshots of a URL.

    Returns a list of [timestamp, original, statuscode, digest, length]
    sorted newest-first, or an empty list on failure.
    """
    params = {
        "url": url,
        "output": "json",
        "fl": "timestamp,original,statuscode,digest,length",
        "filter": "statuscode:200",
        "sort": "reverse",  # newest first
    }
    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(CDX_DELAY)
            resp = session.get(CDX_API, params=params, timeout=30)
            if resp.status_code == 200:
                rows = json.loads(resp.text)
                if len(rows) > 1:
                    return rows[1:]  # skip header row
                return []
            elif resp.status_code == 429:
                wait = (2**attempt) * 10
                logger.warning(f"CDX 429 rate limited, waiting {wait}s")
                time.sleep(wait)
            else:
                logger.warning(f"CDX HTTP {resp.status_code} for {url}")
                return []
        except requests.RequestException as e:
            logger.warning(f"CDX request error (attempt {attempt + 1}): {e}")
            time.sleep(2**attempt)
    return []


def _download_and_validate(url, timestamp, session, cache_dir=None):
    """Download an archived gz sitemap and validate it.

    Returns True if the archive decompresses to valid XML containing
    /news-release URLs, False otherwise.
    """
    wb_url = f"https://web.archive.org/web/{timestamp}id_/{url}"

    # Check cache first
    if cache_dir:
        cache_path = Path(cache_dir) / f"{timestamp}_{Path(url).name}"
        if cache_path.exists():
            logger.info(f"  Using cached: {cache_path.name}")
            gz_bytes = cache_path.read_bytes()
            return _validate_gz(gz_bytes, url)

    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(DOWNLOAD_DELAY)
            resp = session.get(wb_url, timeout=60)
            if resp.status_code == 200:
                gz_bytes = resp.content
                # Cache the download
                if cache_dir:
                    cache_path = Path(cache_dir) / f"{timestamp}_{Path(url).name}"
                    cache_path.parent.mkdir(parents=True, exist_ok=True)
                    cache_path.write_bytes(gz_bytes)
                return _validate_gz(gz_bytes, url)
            elif resp.status_code == 429:
                wait = (2**attempt) * 10
                logger.warning(f"  Download 429 rate limited, waiting {wait}s")
                time.sleep(wait)
            else:
                logger.warning(f"  Download HTTP {resp.status_code}: {wb_url}")
                return False
        except requests.RequestException as e:
            logger.warning(f"  Download error (attempt {attempt + 1}): {e}")
            time.sleep(2**attempt)
    return False


def _validate_gz(gz_bytes, url):
    """Validate that gz bytes decompress to XML with /news-release URLs."""
    try:
        xml_bytes = gzip.decompress(gz_bytes)
    except Exception as e:
        logger.warning(f"  Decompress failed: {e}")
        return False

    try:
        parser = etree.XMLParser(recover=True)
        root = etree.fromstring(xml_bytes, parser)
    except Exception as e:
        logger.warning(f"  XML parse failed: {e}")
        return False

    # Count /news-release URLs
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    count = 0
    for url_elem in root.findall("sm:url", ns):
        loc = url_elem.find("sm:loc", ns)
        if loc is not None and loc.text and "/news-release" in loc.text.lower():
            count += 1
    if not count:
        # Try without namespace
        for url_elem in root.findall("url"):
            loc = url_elem.find("loc")
            if loc is not None and loc.text and "/news-release" in loc.text.lower():
                count += 1

    if count > 0:
        logger.info(f"  Valid: {count} press-release URLs in decompressed XML")
        return True
    else:
        logger.warning("  Invalid: no /news-release URLs found in XML")
        return False


def _generate_months(start, end):
    """Yield (year, month) tuples from start to end (inclusive).

    start/end are 'YYYY-MM' strings.
    """
    sy, sm = int(start[:4]), int(start[5:7])
    ey, em = int(end[:4]), int(end[5:7])
    y, m = sy, sm
    while (y, m) <= (ey, em):
        yield y, m
        m += 1
        if m > 12:
            m = 1
            y += 1


def discover_timestamps(
    start=DEFAULT_START,
    end=DEFAULT_END,
    cache_dir=None,
    dry_run=False,
    max_candidates=3,
):
    """Discover valid Wayback timestamps for missing PR Newswire sitemaps.

    Parameters
    ----------
    start, end : str
        'YYYY-MM' range (inclusive).
    cache_dir : str or None
        Directory to cache downloaded gz files for resumability.
    dry_run : bool
        If True, only query CDX and report snapshot counts without downloading.
    max_candidates : int
        Maximum number of snapshots to try per month before giving up.

    Returns
    -------
    dict
        Mapping 'YYYY-MM' -> best Wayback timestamp string.
    """
    session = requests.Session()
    session.headers.update(HEADERS)

    months = list(_generate_months(start, end))
    total = len(months)
    found = {}
    no_archive = []
    invalid = []

    logger.info(f"Discovering Wayback timestamps for {total} months: {start} to {end}")
    if dry_run:
        logger.info("DRY RUN: will query CDX only, no downloads")

    for i, (year, month) in enumerate(months, 1):
        month_str = f"{year:04d}-{month:02d}"
        gz_url = _sitemap_gz_url(year, month)
        logger.info(f"[{i}/{total}] {month_str}: {gz_url}")

        snapshots = _query_cdx(gz_url, session)
        if not snapshots:
            logger.info("  No Wayback snapshots found")
            no_archive.append(month_str)
            continue

        logger.info(f"  {len(snapshots)} snapshots found (newest first)")
        if dry_run:
            for s in snapshots[:3]:
                logger.info(f"    {s[0]} (status={s[2]}, size={s[4]})")
            continue

        # Try candidates newest-first until one validates
        validated = False
        for candidate in snapshots[:max_candidates]:
            ts = candidate[0]
            logger.info(f"  Trying timestamp {ts}...")
            if _download_and_validate(gz_url, ts, session, cache_dir):
                found[month_str] = ts
                validated = True
                break

        if not validated:
            logger.warning(f"  No valid archive found for {month_str}")
            invalid.append(month_str)

    session.close()

    # Summary
    logger.info(f"\n{'=' * 60}")
    logger.info(
        f"Results: {len(found)} valid, {len(no_archive)} no archive, "
        f"{len(invalid)} invalid"
    )
    if no_archive:
        logger.info(f"No archive: {', '.join(no_archive)}")
    if invalid:
        logger.info(f"Invalid (archive exists but corrupt): {', '.join(invalid)}")

    return found


def _print_dict_literal(timestamps):
    """Print a Python dict literal for _WAYBACK_TIMESTAMPS."""
    if not timestamps:
        print("\n# No valid timestamps discovered.")
        return

    print("\n# Discovered Wayback timestamps — paste into _WAYBACK_TIMESTAMPS")
    print("# in pull_free_newswires.py:")
    print("{")
    for key in sorted(timestamps.keys()):
        print(f'    "{key}": "{timestamps[key]}",')
    print("}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Discover Wayback timestamps for missing PR Newswire sitemaps.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python src/discover_wayback_timestamps.py\n"
            "  python src/discover_wayback_timestamps.py --start 2015-01 --end 2016-01\n"
            "  python src/discover_wayback_timestamps.py --cache-dir _data/wayback_cache\n"
            "  python src/discover_wayback_timestamps.py --dry-run\n"
        ),
    )
    parser.add_argument(
        "--start",
        type=str,
        default=DEFAULT_START,
        help=f"Start month YYYY-MM (default: {DEFAULT_START})",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=DEFAULT_END,
        help=f"End month YYYY-MM (default: {DEFAULT_END})",
    )
    parser.add_argument(
        "--cache-dir",
        type=str,
        default=None,
        help="Directory to cache downloaded gz files for resumability.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only query CDX and report snapshot counts, don't download.",
    )
    parser.add_argument(
        "--max-candidates",
        type=int,
        default=3,
        help="Max snapshots to try per month before giving up (default: 3).",
    )
    args = parser.parse_args()

    timestamps = discover_timestamps(
        start=args.start,
        end=args.end,
        cache_dir=args.cache_dir,
        dry_run=args.dry_run,
        max_candidates=args.max_candidates,
    )
    _print_dict_literal(timestamps)
    sys.exit(0 if timestamps else 1)
