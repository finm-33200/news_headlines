"""
Create a crosswalk between free newswire headlines and RavenPack DJ Press
Release headlines via date-blocked fuzzy matching.

For each calendar date, compares every newswire headline against all
RavenPack headlines from the same date using token_sort_ratio. Keeps only
high-quality matches (default: score >= 80).

Output:
    DATA_DIR / newswire_ravenpack_crosswalk.parquet

Each row links one newswire headline (identified by source_url) to its
best-matching RavenPack headline (identified by rp_story_id), along with
the RavenPack entity identifiers and fuzzy score.

Processing is done month-by-month to keep memory usage bounded. Monthly
chunks are written to a chunks directory and combined at the end.

Usage:
    python create_newswire_ravenpack_crosswalk.py                 # full rebuild
    python create_newswire_ravenpack_crosswalk.py --resume        # skip completed months
    python create_newswire_ravenpack_crosswalk.py --start-month 2024-01 --end-month 2024-03
    python create_newswire_ravenpack_crosswalk.py --combine-only  # merge existing chunks
    python create_newswire_ravenpack_crosswalk.py --min-score 85  # stricter threshold
    python create_newswire_ravenpack_crosswalk.py --status        # show stats
"""

import argparse
import datetime
import logging
import re
import time
from pathlib import Path

import numpy as np
import polars as pl
from rapidfuzz import fuzz, process

from pull_free_newswires import load_newswire_headlines
from settings import config

DATA_DIR = Path(config("DATA_DIR"))
DEFAULT_OUTPUT = DATA_DIR / "newswire_ravenpack_crosswalk.parquet"
DEFAULT_CHUNKS_DIR = DATA_DIR / "crosswalk_chunks"
DEFAULT_MIN_SCORE = 80.0

CROSSWALK_SCHEMA = {
    "date": pl.Date,
    "nw_source_url": pl.Utf8,
    "nw_headline": pl.Utf8,
    "nw_source": pl.Utf8,
    "rp_story_id": pl.Utf8,
    "rp_entity_id": pl.Utf8,
    "rp_entity_name": pl.Utf8,
    "rp_headline": pl.Utf8,
    "rp_source_name": pl.Utf8,
    "fuzzy_score": pl.Float64,
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_PUNCT_RE = re.compile(r"[^\w\s']")
_SPACE_RE = re.compile(r"\s+")


# ---------------------------------------------------------------------------
# Headline normalization
# ---------------------------------------------------------------------------


def normalize_headline(text):
    """Lowercase, strip, remove punctuation (keep apostrophes), collapse spaces."""
    text = text.lower().strip()
    text = _PUNCT_RE.sub("", text)
    text = _SPACE_RE.sub(" ", text)
    return text.strip()


# ---------------------------------------------------------------------------
# Data loading (per-month)
# ---------------------------------------------------------------------------


def _load_month_data(year, month, data_dir=DATA_DIR):
    """Load newswire and RavenPack data for a single month.

    Returns two eager DataFrames (nw, rp) filtered to the given month,
    or (None, None) if either source has no data for that month.
    """
    month_start = datetime.date(year, month, 1)
    if month == 12:
        month_end = datetime.date(year + 1, 1, 1)
    else:
        month_end = datetime.date(year, month + 1, 1)

    # Newswire: use Hive partition columns for efficient filtering
    nw = (
        load_newswire_headlines(data_dir)
        .filter((pl.col("year") == year) & (pl.col("month") == month))
        .collect()
    )
    nw = nw.with_columns(pl.col("date").cast(pl.Date).alias("pub_date"))
    nw = nw.filter(
        pl.col("headline").is_not_null() & (pl.col("headline").str.strip_chars() != "")
    )
    if len(nw) == 0:
        return None, None

    # RavenPack: filter to same month
    rp = (
        pl.scan_parquet(data_dir / "ravenpack_djpr.parquet")
        .with_columns(pl.col("timestamp_utc").cast(pl.Date).alias("date"))
        .filter(
            (pl.col("date") >= month_start) & (pl.col("date") < month_end)
        )
        .filter(
            pl.col("headline").is_not_null()
            & (pl.col("headline").str.strip_chars() != "")
        )
        .select(
            "date",
            "rp_story_id",
            "rp_entity_id",
            "entity_name",
            "headline",
            "source_name",
        )
        .collect()
    )
    if len(rp) == 0:
        return None, None

    return nw, rp


def _discover_months(data_dir=DATA_DIR):
    """Discover all YYYY-MM months available in both newswire and RavenPack.

    Returns a sorted list of (year, month) tuples for months with data in
    both sources.
    """
    # Newswire months from Hive partition columns
    nw_months = (
        load_newswire_headlines(data_dir)
        .select("year", "month")
        .unique()
        .collect()
    )
    nw_set = set(zip(nw_months["year"].to_list(), nw_months["month"].to_list()))

    # RavenPack months
    rp_months = (
        pl.scan_parquet(data_dir / "ravenpack_djpr.parquet")
        .with_columns(pl.col("timestamp_utc").cast(pl.Date).alias("date"))
        .select(
            pl.col("date").dt.year().alias("year"),
            pl.col("date").dt.month().alias("month"),
        )
        .unique()
        .collect()
    )
    rp_set = set(zip(rp_months["year"].to_list(), rp_months["month"].to_list()))

    overlap = sorted(nw_set & rp_set)
    return overlap


# ---------------------------------------------------------------------------
# Legacy data loading (kept for backward compatibility)
# ---------------------------------------------------------------------------


def _load_data(data_dir=DATA_DIR):
    """Load newswire and RavenPack data, returning two DataFrames.

    RavenPack is filtered to the newswire date range to save memory.
    """
    logger.info("Loading newswire headlines...")
    nw = load_newswire_headlines(data_dir).collect()
    nw = nw.with_columns(pl.col("date").cast(pl.Date).alias("pub_date"))
    nw = nw.filter(
        pl.col("headline").is_not_null() & (pl.col("headline").str.strip_chars() != "")
    )
    logger.info(
        f"  Newswire: {len(nw):,} headlines, {nw['pub_date'].min()} to {nw['pub_date'].max()}"
    )

    nw_start = nw["pub_date"].min()
    nw_end = nw["pub_date"].max()

    logger.info("Loading RavenPack headlines...")
    rp = (
        pl.scan_parquet(data_dir / "ravenpack_djpr.parquet")
        .with_columns(pl.col("timestamp_utc").cast(pl.Date).alias("date"))
        .filter((pl.col("date") >= nw_start) & (pl.col("date") <= nw_end))
        .filter(
            pl.col("headline").is_not_null()
            & (pl.col("headline").str.strip_chars() != "")
        )
        .select(
            "date",
            "rp_story_id",
            "rp_entity_id",
            "entity_name",
            "headline",
            "source_name",
        )
        .collect()
    )
    logger.info(f"  RavenPack: {len(rp):,} headlines in newswire date range")

    return nw, rp


# ---------------------------------------------------------------------------
# Per-day matching
# ---------------------------------------------------------------------------


def _match_day(nw_day, rp_day, date_val, min_score):
    """Fuzzy-match newswire vs RavenPack headlines for a single date.

    Returns a list of crosswalk row dicts for matches >= min_score.
    """
    nw_headlines = nw_day["headline"].to_list()
    rp_headlines = rp_day["headline"].to_list()

    if not nw_headlines or not rp_headlines:
        return []

    nw_norms = [normalize_headline(h) for h in nw_headlines]
    rp_norms = [normalize_headline(h) for h in rp_headlines]

    scores = process.cdist(nw_norms, rp_norms, scorer=fuzz.token_sort_ratio, workers=-1)
    best_idx = np.argmax(scores, axis=1)
    best_scores = scores[np.arange(len(nw_norms)), best_idx]

    nw_source_urls = nw_day["source_url"].to_list()
    nw_sources = nw_day["source"].to_list()
    rp_story_ids = rp_day["rp_story_id"].to_list()
    rp_entity_ids = rp_day["rp_entity_id"].to_list()
    rp_entity_names = rp_day["entity_name"].to_list()
    rp_source_names = rp_day["source_name"].to_list()

    rows = []
    for i in range(len(nw_headlines)):
        score = float(best_scores[i])
        if score >= min_score:
            j = int(best_idx[i])
            rows.append(
                {
                    "date": date_val,
                    "nw_source_url": nw_source_urls[i],
                    "nw_headline": nw_headlines[i],
                    "nw_source": nw_sources[i],
                    "rp_story_id": rp_story_ids[j],
                    "rp_entity_id": rp_entity_ids[j],
                    "rp_entity_name": rp_entity_names[j],
                    "rp_headline": rp_headlines[j],
                    "rp_source_name": rp_source_names[j],
                    "fuzzy_score": score,
                }
            )
    return rows


# ---------------------------------------------------------------------------
# Per-month matching
# ---------------------------------------------------------------------------


def _build_month(year, month, min_score, chunks_dir, data_dir=DATA_DIR):
    """Process a single month: load data, match day-by-day, write chunk.

    Returns the number of match rows written, or -1 if no data for this month.
    """
    label = f"{year:04d}-{month:02d}"
    chunk_path = Path(chunks_dir) / f"{label}.parquet"

    nw, rp = _load_month_data(year, month, data_dir)
    if nw is None or rp is None:
        logger.info(f"  {label}: no overlapping data, skipping")
        return -1

    nw_dates = set(nw["pub_date"].unique().to_list())
    rp_dates = set(rp["date"].unique().to_list())
    overlap_dates = sorted(nw_dates & rp_dates)

    if not overlap_dates:
        logger.info(f"  {label}: no overlapping dates, skipping")
        return -1

    all_rows = []
    for d in overlap_dates:
        nw_day = nw.filter(pl.col("pub_date") == d)
        rp_day = rp.filter(pl.col("date") == d)
        rows = _match_day(nw_day, rp_day, d, min_score)
        all_rows.extend(rows)

    if all_rows:
        df = pl.DataFrame(all_rows, schema=CROSSWALK_SCHEMA)
    else:
        df = pl.DataFrame(schema=CROSSWALK_SCHEMA)

    chunk_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(chunk_path)
    return len(df)


# ---------------------------------------------------------------------------
# Main crosswalk builder (legacy, kept for programmatic use)
# ---------------------------------------------------------------------------


def build_crosswalk(nw, rp, min_score=DEFAULT_MIN_SCORE):
    """Build the full crosswalk by iterating over overlapping dates.

    Returns a polars DataFrame with one row per high-quality match.
    """
    nw_dates = set(nw["pub_date"].unique().to_list())
    rp_dates = set(rp["date"].unique().to_list())
    overlap_dates = sorted(nw_dates & rp_dates)

    logger.info(
        f"Overlapping dates: {len(overlap_dates)} "
        f"(newswire: {len(nw_dates)}, ravenpack: {len(rp_dates)})"
    )

    all_rows = []
    t0 = time.time()

    for i, d in enumerate(overlap_dates):
        nw_day = nw.filter(pl.col("pub_date") == d)
        rp_day = rp.filter(pl.col("date") == d)

        rows = _match_day(nw_day, rp_day, d, min_score)
        all_rows.extend(rows)

        if (i + 1) % 50 == 0 or (i + 1) == len(overlap_dates):
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            remaining = (len(overlap_dates) - i - 1) / rate if rate > 0 else 0
            logger.info(
                f"[{i + 1}/{len(overlap_dates)}] {d} — "
                f"{len(all_rows):,} matches so far "
                f"({elapsed:.0f}s elapsed, ~{remaining:.0f}s remaining)"
            )

    if not all_rows:
        logger.warning("No matches found above threshold")
        return pl.DataFrame(schema=CROSSWALK_SCHEMA)

    return pl.DataFrame(all_rows)


# ---------------------------------------------------------------------------
# Chunked crosswalk builder
# ---------------------------------------------------------------------------


def build_crosswalk_chunked(
    min_score=DEFAULT_MIN_SCORE,
    chunks_dir=DEFAULT_CHUNKS_DIR,
    start_month=None,
    end_month=None,
    resume=False,
    data_dir=DATA_DIR,
):
    """Build the crosswalk month-by-month, writing chunks to disk.

    Parameters
    ----------
    min_score : float
        Minimum fuzzy score to keep.
    chunks_dir : Path
        Directory for monthly chunk files.
    start_month : tuple or None
        (year, month) to start from (inclusive).
    end_month : tuple or None
        (year, month) to end at (inclusive).
    resume : bool
        If True, skip months whose chunk file already exists.
    data_dir : Path
        Base data directory.

    Returns
    -------
    int
        Total number of match rows across all months.
    """
    chunks_dir = Path(chunks_dir)
    chunks_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Discovering available months...")
    months = _discover_months(data_dir)
    logger.info(f"  Found {len(months)} months with data in both sources")

    if start_month:
        months = [m for m in months if m >= start_month]
    if end_month:
        months = [m for m in months if m <= end_month]

    logger.info(f"  Processing {len(months)} months")
    if months:
        logger.info(
            f"  Range: {months[0][0]:04d}-{months[0][1]:02d} "
            f"to {months[-1][0]:04d}-{months[-1][1]:02d}"
        )

    total_rows = 0
    t0 = time.time()

    for i, (year, month) in enumerate(months):
        label = f"{year:04d}-{month:02d}"
        chunk_path = chunks_dir / f"{label}.parquet"

        if resume and chunk_path.exists():
            n = len(pl.read_parquet(chunk_path))
            logger.info(f"[{i + 1}/{len(months)}] {label}: already done ({n:,} rows), skipping")
            total_rows += n
            continue

        logger.info(f"[{i + 1}/{len(months)}] {label}: matching...")
        n = _build_month(year, month, min_score, chunks_dir, data_dir)
        if n >= 0:
            total_rows += n
            elapsed = time.time() - t0
            logger.info(
                f"  {label}: {n:,} matches ({total_rows:,} total, {elapsed:.0f}s elapsed)"
            )

    logger.info(f"Done: {total_rows:,} total matches across {len(months)} months")
    return total_rows


def combine_chunks(chunks_dir=DEFAULT_CHUNKS_DIR, output_path=DEFAULT_OUTPUT):
    """Combine all monthly chunk parquets into a single crosswalk file.

    Returns the combined DataFrame.
    """
    chunks_dir = Path(chunks_dir)
    output_path = Path(output_path)

    chunk_files = sorted(chunks_dir.glob("*.parquet"))
    if not chunk_files:
        logger.warning(f"No chunk files found in {chunks_dir}")
        return pl.DataFrame(schema=CROSSWALK_SCHEMA)

    logger.info(f"Combining {len(chunk_files)} chunk files...")
    dfs = [pl.read_parquet(f) for f in chunk_files]
    combined = pl.concat(dfs).sort("date", "nw_source_url")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    combined.write_parquet(output_path)
    logger.info(f"Saved {len(combined):,} rows to {output_path}")

    return combined


# ---------------------------------------------------------------------------
# Status reporting
# ---------------------------------------------------------------------------


def _print_status(output_path):
    """Print summary statistics for an existing crosswalk file."""
    output_path = Path(output_path)
    if not output_path.exists():
        print(f"No crosswalk found at {output_path}", flush=True)
        return

    cw = pl.read_parquet(output_path)
    n = len(cw)
    print(f"Crosswalk: {output_path}", flush=True)
    print(f"  Rows: {n:,}", flush=True)

    if n == 0:
        return

    print(f"  Date range: {cw['date'].min()} to {cw['date'].max()}", flush=True)
    print(f"  Distinct dates: {cw['date'].n_unique()}", flush=True)
    print(f"  Distinct newswire URLs: {cw['nw_source_url'].n_unique():,}", flush=True)
    print(f"  Distinct RP stories: {cw['rp_story_id'].n_unique():,}", flush=True)
    print(f"  Distinct RP entities: {cw['rp_entity_id'].n_unique():,}", flush=True)

    scores = cw["fuzzy_score"]
    print(
        f"\n  Fuzzy score: min={scores.min():.1f}, median={scores.median():.1f}, "
        f"mean={scores.mean():.1f}, max={scores.max():.1f}",
        flush=True,
    )

    print("\n  Matches by newswire source:", flush=True)
    for row in (
        cw.group_by("nw_source")
        .agg(pl.len().alias("n"))
        .sort("n", descending=True)
        .iter_rows(named=True)
    ):
        print(f"    {row['nw_source']}: {row['n']:,}", flush=True)

    print("\n  Matches by RavenPack source:", flush=True)
    for row in (
        cw.group_by("rp_source_name")
        .agg(pl.len().alias("n"))
        .sort("n", descending=True)
        .iter_rows(named=True)
    ):
        print(f"    {row['rp_source_name']}: {row['n']:,}", flush=True)


def _print_chunks_status(chunks_dir):
    """Print summary of completed monthly chunks."""
    chunks_dir = Path(chunks_dir)
    if not chunks_dir.exists():
        print(f"No chunks directory at {chunks_dir}")
        return

    chunk_files = sorted(chunks_dir.glob("*.parquet"))
    if not chunk_files:
        print(f"No chunk files in {chunks_dir}")
        return

    total = 0
    print(f"Chunks directory: {chunks_dir}")
    print(f"  Completed months: {len(chunk_files)}")
    for f in chunk_files:
        n = len(pl.read_parquet(f))
        total += n
        print(f"    {f.stem}: {n:,} rows")
    print(f"  Total rows: {total:,}")


# ---------------------------------------------------------------------------
# CLI helpers
# ---------------------------------------------------------------------------


def _parse_month(s):
    """Parse 'YYYY-MM' string into (year, month) tuple."""
    try:
        parts = s.split("-")
        year, month = int(parts[0]), int(parts[1])
        datetime.date(year, month, 1)  # validate
        return (year, month)
    except (ValueError, IndexError):
        raise argparse.ArgumentTypeError(f"Invalid month format: {s!r} (expected YYYY-MM)")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create a crosswalk between newswire and RavenPack headlines via fuzzy matching.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python create_newswire_ravenpack_crosswalk.py                    # full rebuild\n"
            "  python create_newswire_ravenpack_crosswalk.py --resume           # skip done months\n"
            "  python create_newswire_ravenpack_crosswalk.py --start-month 2024-01 --end-month 2024-03\n"
            "  python create_newswire_ravenpack_crosswalk.py --combine-only     # merge chunks\n"
            "  python create_newswire_ravenpack_crosswalk.py --min-score 85     # stricter threshold\n"
            "  python create_newswire_ravenpack_crosswalk.py --status           # show existing stats\n"
            "  python create_newswire_ravenpack_crosswalk.py --chunks-status    # show chunk progress\n"
        ),
    )
    parser.add_argument(
        "--min-score",
        type=float,
        default=DEFAULT_MIN_SCORE,
        help=f"Minimum fuzzy score to keep (default: {DEFAULT_MIN_SCORE}).",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help=f"Output parquet path (default: {DEFAULT_OUTPUT}).",
    )
    parser.add_argument(
        "--chunks-dir",
        type=str,
        default=None,
        help=f"Directory for monthly chunk files (default: {DEFAULT_CHUNKS_DIR}).",
    )
    parser.add_argument(
        "--start-month",
        type=_parse_month,
        default=None,
        help="First month to process, inclusive (YYYY-MM).",
    )
    parser.add_argument(
        "--end-month",
        type=_parse_month,
        default=None,
        help="Last month to process, inclusive (YYYY-MM).",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip months that already have a chunk file on disk.",
    )
    parser.add_argument(
        "--combine-only",
        action="store_true",
        help="Only combine existing chunk files into the output (no matching).",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Print summary of existing crosswalk and exit.",
    )
    parser.add_argument(
        "--chunks-status",
        action="store_true",
        help="Print summary of completed monthly chunks and exit.",
    )
    args = parser.parse_args()

    output_path = Path(args.output) if args.output else DEFAULT_OUTPUT
    chunks_dir = Path(args.chunks_dir) if args.chunks_dir else DEFAULT_CHUNKS_DIR

    if args.status:
        _print_status(output_path)
    elif args.chunks_status:
        _print_chunks_status(chunks_dir)
    elif args.combine_only:
        combine_chunks(chunks_dir, output_path)
        _print_status(output_path)
    else:
        build_crosswalk_chunked(
            min_score=args.min_score,
            chunks_dir=chunks_dir,
            start_month=args.start_month,
            end_month=args.end_month,
            resume=args.resume,
        )
        combine_chunks(chunks_dir, output_path)
        _print_status(output_path)
