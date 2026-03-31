"""
Fetch Business Wire headlines from the Wayback Machine.

Phase 2 of the Business Wire pipeline. Reads URL inventory parquets
produced by ``enumerate_businesswire_urls.py``, fetches each archived
page, extracts the headline from the ``<h1>`` tag, and saves results
in the standard Hive-partitioned format consumed by the crosswalk.

Output:
    newswire_headlines/source=businesswire/year=YYYY/month=MM/day=DD/data.parquet

Rate-limited to ~1 req/s against archive.org.  Resumable — completed
days are skipped on re-run.  Safe to Ctrl+C.

Usage:
    # Fetch headlines for enumerated URLs
    python src/fetch_businesswire_headlines.py --start 2024-01-01 --end 2024-02-01

    # Show progress
    python src/fetch_businesswire_headlines.py --status
"""

import argparse
import logging
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
INVENTORY_DIR = DATA_DIR / "businesswire_url_inventory"
OUTPUT_DIR = DATA_DIR / "newswire_headlines"
SOURCE_KEY = "businesswire"

WAYBACK_DELAY = 1.0  # seconds between Wayback fetches
CONCURRENT_WORKERS = 2  # keep low for archive.org politeness
MAX_RETRIES = 3

HEADERS = {
    "User-Agent": "FINM33200-CourseProject/1.0 (University of Chicago; academic research)",
}

_shutdown_requested = False


def _handle_signal(signum, frame):
    global _shutdown_requested
    if _shutdown_requested:
        logger.warning("Second signal received, forcing exit")
        sys.exit(1)
    logger.info("Shutdown requested — finishing current day, then exiting.")
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


def _iter_completed_dates():
    """Yield completed Business Wire dates from output parquet paths."""
    bw_dir = OUTPUT_DIR / f"source={SOURCE_KEY}"
    if not bw_dir.exists():
        return
    for p in bw_dir.rglob("data.parquet"):
        try:
            parts = p.relative_to(bw_dir).parts
            y = int(parts[0].split("=")[1])
            m = int(parts[1].split("=")[1])
            d = int(parts[2].split("=")[1])
            yield date(y, m, d)
        except Exception:
            continue


def _find_first_incomplete_day(start, end):
    """Return the first date in [start, end) that is not yet complete."""
    current = start
    while current < end:
        if not _day_complete(current.year, current.month, current.day):
            return current
        current += timedelta(days=1)
    return None


def _inventory_path(year, month, day):
    return (
        INVENTORY_DIR
        / f"year={year:04d}"
        / f"month={month:02d}"
        / f"day={day:02d}"
        / "urls.parquet"
    )


def _load_inventory_for_day(year, month, day):
    """Load URL inventory for a day.

    Returns a list of (url, timestamp, headline_from_slug) tuples.
    headline_from_slug is None when the inventory was created before
    slug capture was added, or when the URL had no slug.
    """
    path = _inventory_path(year, month, day)
    if not path.exists():
        return []
    df = pl.read_parquet(path)
    if len(df) == 0:
        return []
    urls = df["source_url"].to_list()
    timestamps = df["wayback_timestamp"].to_list()
    # headline_from_slug column may not exist in older inventory parquets
    if "headline_from_slug" in df.columns:
        slugs = df["headline_from_slug"].to_list()
    else:
        slugs = [None] * len(urls)
    return list(zip(urls, timestamps, slugs))


def _fetch_headline(url, timestamp, session):
    """Fetch an archived Business Wire page and extract the headline.

    Returns a dict {headline, source_url, date} or None.
    """
    # Use id_ to get the original page without Wayback toolbar
    wb_url = f"https://web.archive.org/web/{timestamp}id_/{url}"

    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(WAYBACK_DELAY)
            resp = session.get(wb_url, timeout=30)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "lxml")
                h1 = soup.find("h1")
                if h1 and h1.get_text(strip=True):
                    headline = h1.get_text(strip=True)
                    # Extract date from URL: /home/YYYYMMDDNNNNNN/en
                    date_match = url.split("/home/")[1][:8] if "/home/" in url else None
                    pub_date = (
                        f"{date_match[:4]}-{date_match[4:6]}-{date_match[6:8]}"
                        if date_match
                        else None
                    )
                    return {"headline": headline, "source_url": url, "date": pub_date}
                return None
            elif resp.status_code == 429:
                wait = (2**attempt) * 15
                logger.warning(f"Wayback 429, waiting {wait}s")
                time.sleep(wait)
            elif resp.status_code == 404:
                return None
            else:
                logger.debug(f"Wayback HTTP {resp.status_code}: {wb_url}")
                if resp.status_code >= 500:
                    time.sleep(2**attempt)
                else:
                    return None
        except requests.RequestException as e:
            logger.debug(f"Wayback error (attempt {attempt + 1}): {e}")
            time.sleep(2**attempt)
    return None


def _fetch_day(year, month, day):
    """Fetch all headlines for one day from Wayback. Returns count saved."""
    if _day_complete(year, month, day):
        return 0

    inventory = _load_inventory_for_day(year, month, day)
    if not inventory:
        # Write empty parquet to mark as done
        out_path = _day_parquet_path(year, month, day)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            schema={"headline": pl.Utf8, "source_url": pl.Utf8, "date": pl.Utf8}
        ).write_parquet(out_path)
        return 0

    session = requests.Session()
    session.headers.update(HEADERS)
    headlines = []
    slug_hits = 0

    for i, (url, ts, slug_headline) in enumerate(inventory):
        if _shutdown_requested:
            break

        try:
            # Fast path: use headline from URL slug (no Wayback fetch needed)
            if slug_headline:
                date_match = url.split("/home/")[1][:8] if "/home/" in url else None
                pub_date = (
                    f"{date_match[:4]}-{date_match[4:6]}-{date_match[6:8]}"
                    if date_match
                    else None
                )
                headlines.append(
                    {"headline": slug_headline, "source_url": url, "date": pub_date}
                )
                slug_hits += 1
                continue

            # Slow path: fetch archived page from Wayback
            result = _fetch_headline(url, ts, session)
            if result:
                headlines.append(result)
        except Exception as e:
            logger.warning(f"  Skipping URL {url}: {e}")

        if (i + 1) % 50 == 0:
            logger.info(
                f"  {year}-{month:02d}-{day:02d}: {i + 1}/{len(inventory)} processed, "
                f"{len(headlines)} headlines ({slug_hits} from slugs)"
            )

    session.close()

    if _shutdown_requested:
        return -1  # signal incomplete

    # Save
    out_path = _day_parquet_path(year, month, day)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame(headlines, schema={"headline": pl.Utf8, "source_url": pl.Utf8, "date": pl.Utf8})
    df.write_parquet(out_path)
    return len(headlines)


def fetch_headlines(start_date, end_date):
    """Fetch Business Wire headlines from Wayback for a date range.

    Reads URL inventory, fetches each archived page, extracts headline.
    Saves to newswire_headlines/source=businesswire/... Hive-partitioned.
    Skips completed days. Safe to Ctrl+C and resume.

    Returns
    -------
    tuple[int, date | None]
        (total_headlines_saved, last_processed_date)
    """
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    total_days = (end - start).days

    logger.info(
        f"Fetching Business Wire headlines: {start_date} to {end_date} ({total_days} days)"
    )
    logger.info(f"Inventory: {INVENTORY_DIR}")
    logger.info(f"Output: {OUTPUT_DIR}/source={SOURCE_KEY}/")

    total_headlines = 0
    current = start
    day_num = 0
    last_processed_date = None

    while current < end:
        if _shutdown_requested:
            break
        day_num += 1
        y, m, d = current.year, current.month, current.day

        if _day_complete(y, m, d):
            current += timedelta(days=1)
            continue

        if not _inventory_path(y, m, d).exists():
            logger.debug(f"No inventory for {current.isoformat()}, skipping")
            current += timedelta(days=1)
            continue

        try:
            count = _fetch_day(y, m, d)
        except Exception as e:
            logger.error(
                f"[{day_num}/{total_days}] {current.isoformat()}: unexpected error, "
                f"skipping (will retry on next run): {e}"
            )
            current += timedelta(days=1)
            continue

        if count < 0:
            break  # shutdown

        last_processed_date = current
        if count > 0:
            total_headlines += count
            logger.info(
                f"[{day_num}/{total_days}] {current.isoformat()}: {count} headlines saved"
            )

        current += timedelta(days=1)

    logger.info(f"\nDone. Total headlines saved: {total_headlines:,}")
    return total_headlines, last_processed_date


def continuous_fetch(
    start_date,
    end_date,
    idle_sleep_seconds=60,
    max_loops=None,
    exit_when_complete=False,
    max_idle_rechecks=None,
):
    """Continuously resume BW fetching from the first incomplete day.

    Useful for long-lived background runs where the process may be relaunched
    or where new inventory appears over time.

    Parameters
    ----------
    exit_when_complete : bool
        If True, exit once no incomplete days remain in the requested range.
    max_idle_rechecks : int | None
        Optional cap on how many consecutive "nothing left to do" rechecks are
        allowed before exiting. Useful when waiting briefly for lagging files,
        but not wanting to poll forever.
    """
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    loops = 0
    idle_rechecks = 0

    logger.info(
        "Continuous mode enabled: will resume from the first incomplete day "
        f"between {start.isoformat()} and {end.isoformat()}"
    )

    while not _shutdown_requested:
        first_missing = _find_first_incomplete_day(start, end)
        if first_missing is None:
            idle_rechecks += 1
            if exit_when_complete:
                logger.info(
                    "No incomplete Business Wire days remain in range; exiting "
                    "because --exit-when-complete was set."
                )
                break
            if max_idle_rechecks is not None and idle_rechecks >= max_idle_rechecks:
                logger.info(
                    "No incomplete Business Wire days remain in range; exiting after "
                    f"{idle_rechecks} idle rechecks."
                )
                break
            logger.info(
                "No incomplete Business Wire days remain in range; "
                f"sleeping {idle_sleep_seconds}s before re-checking. "
                f"(idle recheck {idle_rechecks}"
                + (
                    f"/{max_idle_rechecks}" if max_idle_rechecks is not None else ""
                )
                + ")"
            )
            loops += 1
            if max_loops is not None and loops >= max_loops:
                break
            time.sleep(idle_sleep_seconds)
            continue

        idle_rechecks = 0
        logger.info(
            f"Resuming Business Wire fetch from first incomplete day: {first_missing.isoformat()}"
        )
        _, last_processed = fetch_headlines(first_missing.isoformat(), end.isoformat())
        loops += 1
        if max_loops is not None and loops >= max_loops:
            break

        if _shutdown_requested:
            break

        if last_processed is None:
            logger.info(
                "No processable Business Wire inventory found from the current start; "
                f"sleeping {idle_sleep_seconds}s before retrying."
            )
            time.sleep(idle_sleep_seconds)


def print_status():
    """Print progress for Business Wire headline fetching."""
    bw_dir = OUTPUT_DIR / f"source={SOURCE_KEY}"
    if not bw_dir.exists():
        print(f"No Business Wire data at {bw_dir}")
    else:
        parquets = list(bw_dir.rglob("data.parquet"))
        total = 0
        for p in parquets:
            total += len(pl.read_parquet(p))
        print(f"Business Wire headlines: {len(parquets)} days, {total:,} headlines")

    if INVENTORY_DIR.exists():
        inv_files = list(INVENTORY_DIR.rglob("urls.parquet"))
        inv_total = 0
        for p in inv_files:
            inv_total += len(pl.read_parquet(p))
        print(f"URL inventory: {len(inv_files)} days, {inv_total:,} URLs")
    else:
        print(f"No URL inventory at {INVENTORY_DIR}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch Business Wire headlines from Wayback Machine.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Requires URL inventory from enumerate_businesswire_urls.py.\n\n"
            "Examples:\n"
            "  python src/fetch_businesswire_headlines.py --start 2024-01-01 --end 2024-02-01\n"
            "  python src/fetch_businesswire_headlines.py --status\n"
        ),
    )
    parser.add_argument("--start", type=str, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", type=str, help="End date YYYY-MM-DD (exclusive)")
    parser.add_argument("--status", action="store_true", help="Print progress")
    parser.add_argument(
        "--continuous",
        action="store_true",
        help=(
            "Continuously resume from the first incomplete day in range instead of "
            "exiting after one pass"
        ),
    )
    parser.add_argument(
        "--idle-sleep-seconds",
        type=int,
        default=60,
        help="Sleep interval between continuous-mode rechecks when nothing is pending",
    )
    parser.add_argument(
        "--max-loops",
        type=int,
        default=None,
        help="Optional cap on continuous-mode loops (for testing)",
    )
    parser.add_argument(
        "--exit-when-complete",
        action="store_true",
        help="In continuous mode, exit cleanly once no incomplete days remain in range",
    )
    parser.add_argument(
        "--max-idle-rechecks",
        type=int,
        default=None,
        help=(
            "In continuous mode, exit after this many consecutive idle rechecks "
            "with no incomplete days remaining"
        ),
    )
    args = parser.parse_args()

    if args.status:
        print_status()
    elif args.start and args.end:
        if args.continuous:
            continuous_fetch(
                args.start,
                args.end,
                idle_sleep_seconds=args.idle_sleep_seconds,
                max_loops=args.max_loops,
                exit_when_complete=args.exit_when_complete,
                max_idle_rechecks=args.max_idle_rechecks,
            )
        else:
            fetch_headlines(args.start, args.end)
    else:
        parser.print_help()
