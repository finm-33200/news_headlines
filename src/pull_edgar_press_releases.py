"""
Pull press-release headlines from SEC EDGAR via the EFTS full-text search API.

Companies file press releases as EX-99.1 exhibits attached to 8-K filings.
This tool queries the EDGAR Full-Text Search API for 8-K filings containing
"press release", then fetches each exhibit to extract the headline.

Coverage: 2001–present, ~150–200 filings/day.

Output:
    newswire_headlines/source=edgar_8k/year=YYYY/month=MM/day=DD/data.parquet

Usage:
    # Pull one day
    python src/pull_edgar_press_releases.py --start 2024-01-02 --end 2024-01-03

    # Pull a month
    python src/pull_edgar_press_releases.py --start 2024-01-01 --end 2024-02-01

    # Read-only validation for one day
    python src/pull_edgar_press_releases.py --validate-date 2024-01-02

    # Show progress
    python src/pull_edgar_press_releases.py --status
"""

import argparse
import logging
import re
import signal
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import requests
from bs4 import BeautifulSoup

from settings import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DATA_DIR = Path(config("DATA_DIR"))
OUTPUT_DIR = DATA_DIR / "newswire_headlines"
SOURCE_KEY = "edgar_8k"

EFTS_URL = "https://efts.sec.gov/LATEST/search-index"
EDGAR_ARCHIVE = "https://www.sec.gov/Archives/edgar/data"

# SEC requires identifying User-Agent with contact info
HEADERS = {
    "User-Agent": "FINM33200-CourseProject/1.0 jbejarano@uchicago.edu (University of Chicago; academic research)",
    "Accept-Encoding": "gzip, deflate",
}

PAGE_SIZE = 100  # EFTS max results per request
REQUEST_DELAY = 0.2  # SEC asks for max 10 req/s
MAX_RETRIES = 3

_shutdown_requested = False


def _handle_signal(signum, frame):
    global _shutdown_requested
    if _shutdown_requested:
        sys.exit(1)
    logger.info("Shutdown requested — finishing current day.")
    _shutdown_requested = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


def _day_parquet_path(year, month, day):
    return (
        OUTPUT_DIR
        / f"source={SOURCE_KEY}"
        / f"year={year:04d}"
        / f"month={month:02d}"
        / f"day={day:02d}"
        / "data.parquet"
    )


def _day_complete(year, month, day):
    return _day_parquet_path(year, month, day).exists()


def _query_efts(date_str, session):
    """Query EFTS for all 8-K press releases filed on a given date.

    Paginates through all results.  Returns a list of dicts with
    {cik, adsh, filename, display_name, file_date}.
    """
    results = []
    offset = 0

    while True:
        params = {
            "q": '"press release"',
            "forms": "8-K",
            "startdt": date_str,
            "enddt": date_str,
            "from": offset,
        }
        for attempt in range(MAX_RETRIES):
            try:
                time.sleep(REQUEST_DELAY)
                resp = session.get(EFTS_URL, params=params, timeout=30)
                if resp.status_code == 200:
                    data = resp.json()
                    hits = data.get("hits", {}).get("hits", [])
                    total = data.get("hits", {}).get("total", {}).get("value", 0)

                    for hit in hits:
                        src = hit.get("_source", {})
                        doc_id = hit.get("_id", "")
                        # _id format: "adsh:filename"
                        parts = doc_id.split(":", 1)
                        if len(parts) != 2:
                            continue
                        adsh, filename = parts
                        ciks = src.get("ciks", [])
                        if not ciks:
                            continue
                        display_names = src.get("display_names", [])
                        results.append(
                            {
                                "cik": ciks[0].lstrip("0"),
                                "adsh": adsh,
                                "filename": filename,
                                "display_name": display_names[0] if display_names else "",
                                "file_date": src.get("file_date", date_str),
                                "file_type": src.get("file_type", ""),
                            }
                        )

                    offset += len(hits)
                    if offset >= total or not hits:
                        return results
                    break  # success, continue pagination
                elif resp.status_code == 429:
                    wait = (2**attempt) * 5
                    logger.warning(f"EFTS 429, waiting {wait}s")
                    time.sleep(wait)
                else:
                    logger.warning(f"EFTS HTTP {resp.status_code}")
                    return results
            except requests.RequestException as e:
                logger.warning(f"EFTS error (attempt {attempt + 1}): {e}")
                time.sleep(2**attempt)
        else:
            logger.error(f"EFTS failed after {MAX_RETRIES} retries")
            break

    return results


_BOILERPLATE_RE = re.compile(
    r"^(Exhibit\s+\d|FOR\s+IMMEDIATE|CONTACT|INVESTOR\s+RELATIONS|MEDIA\s+CONTACT"
    r"|SAFE\s+HARBOR|FORWARD.LOOKING|SOURCE\s*:|\(PRNewswire\)|\(GLOBE\s*NEWSWIRE\)"
    r"|\(BUSINESS\s*WIRE\))",
    re.IGNORECASE,
)


def _clean_headline(text):
    """Normalize whitespace in a headline (collapse newlines/tabs to spaces)."""
    return " ".join(text.split())


def _extract_headline_from_exhibit(html_text):
    """Extract the press-release headline from an EDGAR exhibit.

    Looks for the first substantive bold text or paragraph after
    the "Exhibit 99.1" header, which is typically the headline.
    """
    soup = BeautifulSoup(html_text, "lxml")

    # Strategy 1: First bold/strong text that looks like a headline
    for tag in soup.find_all(["b", "strong"]):
        text = _clean_headline(tag.get_text(strip=True))
        if _BOILERPLATE_RE.search(text):
            continue
        if len(text) >= 15 and len(text) <= 500:
            return text

    # Strategy 2: First h1
    h1 = soup.find("h1")
    if h1:
        text = _clean_headline(h1.get_text(strip=True))
        if text and not _BOILERPLATE_RE.search(text):
            return text

    # Strategy 3: First substantive paragraph (not boilerplate)
    for p in soup.find_all("p"):
        text = _clean_headline(p.get_text(strip=True))
        if len(text) < 20 or len(text) > 300:
            continue
        if _BOILERPLATE_RE.search(text):
            continue
        return text

    return None


def _fetch_exhibit_headline(filing, session):
    """Fetch an EDGAR exhibit and extract the headline.

    Returns a dict {headline, source_url, date} or None.
    """
    cik = filing["cik"]
    adsh = filing["adsh"]
    filename = filing["filename"]
    adsh_stripped = adsh.replace("-", "")

    exhibit_url = f"{EDGAR_ARCHIVE}/{cik}/{adsh_stripped}/{filename}"

    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(REQUEST_DELAY)
            resp = session.get(exhibit_url, timeout=30)
            if resp.status_code == 200:
                headline = _extract_headline_from_exhibit(resp.text)
                if headline:
                    return {
                        "headline": headline,
                        "source_url": exhibit_url,
                        "date": filing["file_date"],
                    }
                return None
            elif resp.status_code == 404:
                return None
            elif resp.status_code == 429:
                wait = (2**attempt) * 5
                time.sleep(wait)
            else:
                return None
        except requests.RequestException as e:
            logger.debug(f"Exhibit fetch error: {e}")
            time.sleep(2**attempt)
    return None


def _summarize_validation(date_str, filings, headlines, failed_exhibits, sample_size=5):
    """Build a read-only validation summary for one EDGAR filing date."""
    ex99_filings = [f for f in filings if "99" in f.get("file_type", "")]
    headline_yield_pct = 0.0
    if ex99_filings:
        headline_yield_pct = 100.0 * len(headlines) / len(ex99_filings)

    sample_headlines = [row["headline"] for row in headlines[:sample_size]]
    failed_urls = []
    for filing in failed_exhibits[:sample_size]:
        cik = filing["cik"]
        adsh = filing["adsh"].replace("-", "")
        filename = filing["filename"]
        failed_urls.append(f"{EDGAR_ARCHIVE}/{cik}/{adsh}/{filename}")

    return {
        "date": date_str,
        "efts_hits": len(filings),
        "ex99_hits": len(ex99_filings),
        "headline_count": len(headlines),
        "failed_exhibit_count": len(failed_exhibits),
        "headline_yield_pct": headline_yield_pct,
        "sample_headlines": sample_headlines,
        "failed_exhibit_urls": failed_urls,
    }


def validate_day(date_str, sample_size=5):
    """Validate EFTS search/extraction coverage for one date without writing output."""
    session = requests.Session()
    session.headers.update(HEADERS)
    try:
        filings = _query_efts(date_str, session)
        ex99_filings = [f for f in filings if "99" in f.get("file_type", "")]
        headlines = []
        failed_exhibits = []

        logger.info(
            f"Validation {date_str}: {len(filings)} EFTS hits, "
            f"{len(ex99_filings)} EX-99 exhibits"
        )

        for i, filing in enumerate(ex99_filings):
            result = _fetch_exhibit_headline(filing, session)
            if result:
                headlines.append(result)
            else:
                failed_exhibits.append(filing)

            if (i + 1) % 50 == 0:
                logger.info(
                    f"  {i + 1}/{len(ex99_filings)} exhibits checked, "
                    f"{len(headlines)} headlines extracted"
                )

        return _summarize_validation(
            date_str,
            filings=filings,
            headlines=headlines,
            failed_exhibits=failed_exhibits,
            sample_size=sample_size,
        )
    finally:
        session.close()


def _pull_day(year, month, day, session):
    """Pull all EDGAR press-release headlines for one day."""
    if _day_complete(year, month, day):
        return 0

    date_str = f"{year:04d}-{month:02d}-{day:02d}"
    filings = _query_efts(date_str, session)

    if not filings:
        # Write empty parquet to mark as done
        out_path = _day_parquet_path(year, month, day)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            schema={"headline": pl.Utf8, "source_url": pl.Utf8, "date": pl.Utf8}
        ).write_parquet(out_path)
        return 0

    # Filter to EX-99 exhibits (actual press releases)
    ex99_filings = [f for f in filings if "99" in f.get("file_type", "")]
    logger.info(
        f"  {date_str}: {len(filings)} EFTS hits, {len(ex99_filings)} EX-99 exhibits"
    )

    headlines = []
    for i, filing in enumerate(ex99_filings):
        if _shutdown_requested:
            return -1
        result = _fetch_exhibit_headline(filing, session)
        if result:
            headlines.append(result)
        if (i + 1) % 50 == 0:
            logger.info(f"    {i + 1}/{len(ex99_filings)} fetched, {len(headlines)} headlines")

    out_path = _day_parquet_path(year, month, day)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame(
        headlines, schema={"headline": pl.Utf8, "source_url": pl.Utf8, "date": pl.Utf8}
    )
    df.write_parquet(out_path)
    return len(headlines)


def pull_edgar(start_date, end_date):
    """Pull EDGAR press-release headlines for a date range."""
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    total_days = (end - start).days

    logger.info(f"Pulling EDGAR press releases: {start_date} to {end_date} ({total_days} days)")

    session = requests.Session()
    session.headers.update(HEADERS)

    total_headlines = 0
    current = start
    day_num = 0

    while current < end:
        if _shutdown_requested:
            break
        day_num += 1
        y, m, d = current.year, current.month, current.day

        if _day_complete(y, m, d):
            current += timedelta(days=1)
            continue

        # Skip weekends (SEC doesn't accept filings)
        if current.weekday() >= 5:
            current += timedelta(days=1)
            continue

        count = _pull_day(y, m, d, session)
        if count < 0:
            break
        if count > 0:
            total_headlines += count
            logger.info(
                f"[{day_num}/{total_days}] {current.isoformat()}: {count} headlines"
            )

        current += timedelta(days=1)

    session.close()
    logger.info(f"\nDone. Total headlines: {total_headlines:,}")


def print_status():
    """Print EDGAR pull progress."""
    edgar_dir = OUTPUT_DIR / f"source={SOURCE_KEY}"
    if not edgar_dir.exists():
        print(f"No EDGAR data at {edgar_dir}")
        return

    parquets = list(edgar_dir.rglob("data.parquet"))
    total = 0
    for p in parquets:
        total += len(pl.read_parquet(p))
    print(f"EDGAR 8-K headlines: {len(parquets)} days, {total:,} headlines")


def print_validation_summary(summary):
    """Render a one-day validation summary to stdout."""
    print(
        f"EDGAR validation for {summary['date']}: "
        f"{summary['efts_hits']} EFTS hits, "
        f"{summary['ex99_hits']} EX-99 exhibits, "
        f"{summary['headline_count']} headlines "
        f"({summary['headline_yield_pct']:.1f}% yield)"
    )
    print(f"Failed exhibits: {summary['failed_exhibit_count']}")
    if summary["sample_headlines"]:
        print("Sample headlines:")
        for headline in summary["sample_headlines"]:
            print(f"  - {headline}")
    if summary["failed_exhibit_urls"]:
        print("Sample failed exhibit URLs:")
        for url in summary["failed_exhibit_urls"]:
            print(f"  - {url}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pull press-release headlines from SEC EDGAR EFTS.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python src/pull_edgar_press_releases.py --start 2024-01-02 --end 2024-01-03\n"
            "  python src/pull_edgar_press_releases.py --start 2024-01-01 --end 2024-02-01\n"
            "  python src/pull_edgar_press_releases.py --status\n"
        ),
    )
    parser.add_argument("--start", type=str, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", type=str, help="End date YYYY-MM-DD (exclusive)")
    parser.add_argument("--status", action="store_true", help="Print progress")
    parser.add_argument(
        "--validate-date",
        type=str,
        help="Read-only validation for one date YYYY-MM-DD (no parquet writes)",
    )
    args = parser.parse_args()

    if args.status:
        print_status()
    elif args.validate_date:
        print_validation_summary(validate_day(args.validate_date))
    elif args.start and args.end:
        pull_edgar(args.start, args.end)
    else:
        parser.print_help()
