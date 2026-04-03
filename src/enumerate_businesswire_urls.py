"""
Enumerate Business Wire press-release URLs via the Wayback Machine CDX API.

Business Wire's live sitemaps consistently timeout, but the Wayback Machine
has excellent coverage.  This tool queries CDX for captured article URLs by
date, filters for English press releases, and saves the results as one
parquet per day.

The output serves as the URL inventory for a subsequent headline-extraction
pass (which fetches each archived page to read the <h1>).

Usage:
    # Enumerate one month
    python src/enumerate_businesswire_urls.py --start 2024-01-01 --end 2024-02-01

    # Enumerate a full year
    python src/enumerate_businesswire_urls.py --start 2024-01-01 --end 2025-01-01

    # Show progress
    python src/enumerate_businesswire_urls.py --status

Output:
    _data/businesswire_url_inventory/year=YYYY/month=MM/day=DD/urls.parquet

Validation:
    # Inspect slug coverage for one already-enumerated day
    python src/enumerate_businesswire_urls.py --spot-check 2024-01-02
"""

import argparse
import json
import logging
import re
import time
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import requests

from settings import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DATA_DIR = Path(config("DATA_DIR"))
INVENTORY_DIR = DATA_DIR / "businesswire_url_inventory"

CDX_API = "https://web.archive.org/cdx/search/cdx"
CDX_DELAY = 1.5  # seconds between CDX queries (be polite to archive.org)
MAX_RETRIES = 3

HEADERS = {
    "User-Agent": "FINM33200-CourseProject/1.0 (University of Chicago; academic research)",
}

NON_ARTICLE_SLUG_EXTENSIONS = {
    ".css",
    ".doc",
    ".docx",
    ".gif",
    ".ico",
    ".jpeg",
    ".jpg",
    ".js",
    ".json",
    ".pdf",
    ".png",
    ".svg",
    ".txt",
    ".webp",
    ".xml",
}


def _query_cdx_for_date(date_str, session):
    """Query Wayback CDX for all Business Wire articles on a given date.

    Parameters
    ----------
    date_str : str
        Date as 'YYYYMMDD'.
    session : requests.Session

    Returns
    -------
    list[dict]
        List of {url, timestamp, date} dicts for English press releases.
    """
    params = {
        "url": f"businesswire.com/news/home/{date_str}*",
        "output": "json",
        "fl": "timestamp,original,statuscode",
        "filter": "statuscode:200",
        "collapse": "urlkey",
    }
    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(CDX_DELAY)
            resp = session.get(CDX_API, params=params, timeout=60)
            if resp.status_code == 200:
                rows = json.loads(resp.text)
                if len(rows) <= 1:
                    return []
                return _filter_english_articles(rows[1:], date_str)
            elif resp.status_code == 429:
                wait = (2**attempt) * 15
                logger.warning(f"CDX 429 rate limited, waiting {wait}s")
                time.sleep(wait)
            else:
                logger.warning(f"CDX HTTP {resp.status_code} for date {date_str}")
                return []
        except (json.JSONDecodeError, ValueError) as e:
            # CDX sometimes returns truncated/malformed JSON — retry
            snippet = repr(resp.text[:200]) if resp is not None else "N/A"
            logger.warning(
                f"CDX malformed JSON for {date_str} (attempt {attempt + 1}): {e}; "
                f"response start: {snippet}"
            )
            time.sleep(2**attempt)
        except requests.RequestException as e:
            logger.warning(
                f"CDX network error for {date_str} (attempt {attempt + 1}): {e}"
            )
            time.sleep(2**attempt)
    logger.error(f"CDX failed after {MAX_RETRIES} retries for {date_str}")
    return []


def _headline_from_slug(slug):
    """Convert a BW URL slug to an approximate headline.

    Mirrors the Newswire.ca approach: replace hyphens with spaces, title-case.
    Sufficient for TF-IDF crosswalk matching.
    """
    if not slug:
        return None
    # URL-decode percent-encoded characters (e.g. %E2%80%99 -> ')
    from urllib.parse import unquote

    text = unquote(slug).replace("-", " ").strip()
    return text.title() if text else None


def _is_probable_asset_slug(slug):
    """Return True when the trailing path looks like a file asset, not a headline slug."""
    if not slug:
        return False
    slug_lower = slug.lower()
    return any(slug_lower.endswith(ext) for ext in NON_ARTICLE_SLUG_EXTENSIONS)


def _filter_english_articles(rows, date_str):
    """Filter CDX rows to English press-release URLs only.

    Business Wire URLs come in two CDX variants:
      /news/home/YYYYMMDDNNNNNN/en/                     (bare)
      /news/home/YYYYMMDDNNNNNN/en/Some-Headline-Slug   (with slug)

    For each unique article ID, we prefer the slug variant because it
    contains the headline text — enabling fast-path extraction with no
    Wayback page fetch (same approach as Newswire.ca).
    """
    # Collect best URL per article ID: prefer slug-bearing variant
    by_article_id = {}  # article_id -> (url, timestamp, slug_headline)
    formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

    for row in rows:
        ts, url, status = row[0], row[1], row[2]
        url_clean = url.rstrip("/")

        # Must be an English article: contains /en at the right position
        m = re.search(r"/news/home/(\d{14,})/en(?:/(.+))?$", url_clean)
        if not m:
            continue

        article_id = m.group(1)
        slug = m.group(2)  # None for bare /en URLs

        # Skip non-article paths (e.g. /en/de/, /en/fr-ca/, image URLs)
        if slug and "/" in slug:
            continue
        if slug and re.fullmatch(r"[a-z]{2}(?:-[a-z]{2})?", slug.lower()):
            continue
        if _is_probable_asset_slug(slug):
            continue

        headline = _headline_from_slug(slug)

        # Prefer slug-bearing URL over bare URL for the same article
        existing = by_article_id.get(article_id)
        if existing is None or (headline and not existing[2]):
            by_article_id[article_id] = (url_clean, ts, headline)

    results = []
    for article_id, (url, ts, headline) in by_article_id.items():
        results.append(
            {
                "source_url": url,
                "wayback_timestamp": ts,
                "date": formatted_date,
                "headline_from_slug": headline,  # None when no slug
            }
        )
    return results


def _inventory_path(year, month, day):
    """Return Hive-partitioned path for a day's URL inventory."""
    return (
        INVENTORY_DIR
        / f"year={year:04d}"
        / f"month={month:02d}"
        / f"day={day:02d}"
        / "urls.parquet"
    )


def _inventory_spot_check(df, sample_size=5):
    """Summarize slug-headline coverage for one day's inventory."""
    total_urls = len(df)
    if total_urls == 0:
        return {
            "total_urls": 0,
            "slug_headline_count": 0,
            "missing_slug_count": 0,
            "slug_coverage_pct": 0.0,
            "slug_samples": [],
            "missing_slug_samples": [],
        }

    slug_mask = pl.Series([False] * total_urls)
    if "headline_from_slug" in df.columns:
        slug_mask = df["headline_from_slug"].is_not_null() & (
            df["headline_from_slug"].str.strip_chars().str.len_chars() > 0
        )

    slug_df = df.filter(slug_mask).select(["source_url", "headline_from_slug"])
    missing_slug_df = df.filter(~slug_mask).select(["source_url"])
    slug_headline_count = len(slug_df)
    missing_slug_count = len(missing_slug_df)

    return {
        "total_urls": total_urls,
        "slug_headline_count": slug_headline_count,
        "missing_slug_count": missing_slug_count,
        "slug_coverage_pct": 100.0 * slug_headline_count / total_urls,
        "slug_samples": slug_df.head(sample_size).to_dicts(),
        "missing_slug_samples": missing_slug_df.head(sample_size)[
            "source_url"
        ].to_list(),
    }


def _inventory_file_summary(df):
    """Summarize one inventory parquet for status reporting."""
    total_urls = len(df)
    has_slug_column = "headline_from_slug" in df.columns

    if total_urls == 0:
        return {
            "total_urls": 0,
            "has_slug_column": has_slug_column,
            "slug_headline_count": 0,
            "missing_slug_count": 0,
        }

    if not has_slug_column:
        return {
            "total_urls": total_urls,
            "has_slug_column": False,
            "slug_headline_count": 0,
            "missing_slug_count": total_urls,
        }

    slug_mask = df["headline_from_slug"].is_not_null() & (
        df["headline_from_slug"].str.strip_chars().str.len_chars() > 0
    )
    slug_headline_count = int(slug_mask.sum())
    return {
        "total_urls": total_urls,
        "has_slug_column": True,
        "slug_headline_count": slug_headline_count,
        "missing_slug_count": total_urls - slug_headline_count,
    }


def _inventory_status_summary(inventory_dir):
    """Aggregate status across all on-disk inventory parquets."""
    parquets = list(inventory_dir.rglob("urls.parquet"))
    summary = {
        "day_files": len(parquets),
        "total_urls": 0,
        "slug_headline_count": 0,
        "missing_slug_count": 0,
        "slug_ready_day_files": 0,
        "legacy_day_files": 0,
        "legacy_dates": [],
        "date_range": None,
        "slug_coverage_pct": 0.0,
    }

    if not parquets:
        return summary

    dates = []
    for p in parquets:
        df = pl.read_parquet(p)
        file_summary = _inventory_file_summary(df)
        summary["total_urls"] += file_summary["total_urls"]
        summary["slug_headline_count"] += file_summary["slug_headline_count"]
        summary["missing_slug_count"] += file_summary["missing_slug_count"]
        if file_summary["has_slug_column"]:
            summary["slug_ready_day_files"] += 1
        else:
            summary["legacy_day_files"] += 1
            try:
                parts = p.relative_to(inventory_dir).parts
                legacy_date = date(
                    int(parts[0].split("=")[1]),
                    int(parts[1].split("=")[1]),
                    int(parts[2].split("=")[1]),
                )
                summary["legacy_dates"].append(legacy_date.isoformat())
            except Exception:
                continue

        try:
            parts = p.relative_to(inventory_dir).parts
            dates.append(
                date(
                    int(parts[0].split("=")[1]),
                    int(parts[1].split("=")[1]),
                    int(parts[2].split("=")[1]),
                )
            )
        except Exception:
            continue

    if summary["total_urls"] > 0:
        summary["slug_coverage_pct"] = (
            100.0 * summary["slug_headline_count"] / summary["total_urls"]
        )
    if dates:
        dates.sort()
        summary["date_range"] = (dates[0].isoformat(), dates[-1].isoformat())
    summary["legacy_dates"].sort()
    return summary


def print_spot_check(day_str, sample_size=5):
    """Print a local spot-check for one already-enumerated day."""
    target_date = date.fromisoformat(day_str)
    path = _inventory_path(target_date.year, target_date.month, target_date.day)
    if not path.exists():
        print(f"No inventory parquet for {day_str}: {path}")
        return

    df = pl.read_parquet(path)
    summary = _inventory_spot_check(df, sample_size=sample_size)

    print(f"Business Wire inventory spot-check: {day_str}")
    print(f"  Inventory file: {path}")
    print(f"  Total URLs: {summary['total_urls']:,}")
    print(
        f"  Slug headlines available: {summary['slug_headline_count']:,} "
        f"({summary['slug_coverage_pct']:.1f}%)"
    )
    print(f"  Wayback fetch still needed: {summary['missing_slug_count']:,}")

    if summary["slug_samples"]:
        print("  Sample slug-derived headlines:")
        for sample in summary["slug_samples"]:
            print(f"    {sample['headline_from_slug']} <- {sample['source_url']}")

    if summary["missing_slug_samples"]:
        print("  Sample URLs still needing Wayback fetch:")
        for url in summary["missing_slug_samples"]:
            print(f"    {url}")


def _day_already_enumerated(year, month, day, force=False):
    """Check if a day's URL inventory already exists on disk.

    If force=True, returns False for inventory files that lack the
    ``headline_from_slug`` column (need re-enumeration to capture slugs).
    """
    path = _inventory_path(year, month, day)
    if not path.exists():
        return False
    if not force:
        return True
    # Re-enumerate if the parquet lacks the slug column
    try:
        df = pl.read_parquet(path)
        return "headline_from_slug" in df.columns
    except Exception:
        return False


def enumerate_urls(start_date, end_date, force=False):
    """Enumerate Business Wire article URLs via CDX for a date range.

    Saves one parquet per day to the inventory directory.
    Skips days that already have an inventory file (resumable).

    Parameters
    ----------
    start_date, end_date : str
        'YYYY-MM-DD' format (end is exclusive).
    force : bool
        If True, re-enumerate days whose inventory parquets lack the
        ``headline_from_slug`` column (created by an older version of
        this script).
    """
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)

    INVENTORY_DIR.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update(HEADERS)

    total_days = (end - start).days
    total_urls = 0
    skipped = 0

    mode = " (force re-enumerate for slugs)" if force else ""
    logger.info(
        f"Enumerating Business Wire URLs: {start_date} to {end_date} ({total_days} days){mode}"
    )

    current = start
    day_num = 0
    errors = []
    while current < end:
        day_num += 1
        y, m, d = current.year, current.month, current.day

        if _day_already_enumerated(y, m, d, force=force):
            skipped += 1
            current += timedelta(days=1)
            continue

        try:
            date_str = current.strftime("%Y%m%d")
            articles = _query_cdx_for_date(date_str, session)

            if articles:
                out_path = _inventory_path(y, m, d)
                out_path.parent.mkdir(parents=True, exist_ok=True)
                df = pl.DataFrame(articles)
                df.write_parquet(out_path)
                total_urls += len(articles)
                logger.info(
                    f"[{day_num}/{total_days}] {current.isoformat()}: "
                    f"{len(articles)} English articles"
                )
            else:
                # Write empty parquet so we don't re-query
                out_path = _inventory_path(y, m, d)
                out_path.parent.mkdir(parents=True, exist_ok=True)
                df = pl.DataFrame(
                    schema={
                        "source_url": pl.Utf8,
                        "wayback_timestamp": pl.Utf8,
                        "date": pl.Utf8,
                        "headline_from_slug": pl.Utf8,
                    }
                )
                df.write_parquet(out_path)
                if current.weekday() < 5:  # Warn only on weekdays
                    logger.info(
                        f"[{day_num}/{total_days}] {current.isoformat()}: 0 articles"
                    )
        except Exception as e:
            # Log and skip — do NOT write a parquet so this day will be retried
            errors.append(current.isoformat())
            logger.error(
                f"[{day_num}/{total_days}] {current.isoformat()}: unexpected error, "
                f"skipping (will retry on next run): {e}"
            )

        current += timedelta(days=1)

    session.close()
    logger.info(
        f"\nEnumeration complete. {total_urls:,} URLs across {total_days - skipped} new days "
        f"({skipped} skipped)."
    )
    if errors:
        logger.warning(
            f"{len(errors)} days had errors and were NOT saved (will retry on next run): "
            f"{', '.join(errors)}"
        )


def print_status():
    """Print inventory progress."""
    if not INVENTORY_DIR.exists():
        print(f"No inventory at {INVENTORY_DIR}")
        return

    summary = _inventory_status_summary(INVENTORY_DIR)
    print(f"Business Wire URL inventory: {INVENTORY_DIR}")
    print(f"  Day files: {summary['day_files']}")
    print(f"  Total URLs: {summary['total_urls']:,}")
    print(
        f"  Slug fast-path available: {summary['slug_headline_count']:,} "
        f"({summary['slug_coverage_pct']:.1f}%)"
    )
    print(f"  Still need Wayback fetch: {summary['missing_slug_count']:,}")
    print(
        f"  Slug-ready day files: {summary['slug_ready_day_files']} | "
        f"legacy day files needing --force: {summary['legacy_day_files']}"
    )

    if summary["date_range"] is not None:
        start_date, end_date = summary["date_range"]
        print(f"  Date range: {start_date} to {end_date}")

    if summary["legacy_dates"]:
        sample = ", ".join(summary["legacy_dates"][:5])
        suffix = " ..." if len(summary["legacy_dates"]) > 5 else ""
        print(f"  Sample legacy dates: {sample}{suffix}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Enumerate Business Wire URLs via Wayback CDX.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python src/enumerate_businesswire_urls.py --start 2024-01-01 --end 2024-02-01\n"
            "  python src/enumerate_businesswire_urls.py --spot-check 2024-01-02\n"
            "  python src/enumerate_businesswire_urls.py --status\n"
        ),
    )
    parser.add_argument("--start", type=str, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", type=str, help="End date YYYY-MM-DD (exclusive)")
    parser.add_argument(
        "--status", action="store_true", help="Print inventory progress"
    )
    parser.add_argument(
        "--spot-check",
        type=str,
        metavar="YYYY-MM-DD",
        help="Inspect slug-headline coverage for one existing inventory day",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-enumerate days whose inventory lacks headline_from_slug column",
    )
    args = parser.parse_args()

    if args.status:
        print_status()
    elif args.spot_check:
        print_spot_check(args.spot_check)
    elif args.start and args.end:
        enumerate_urls(args.start, args.end, force=args.force)
    else:
        parser.print_help()
