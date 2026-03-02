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

Usage:
    python create_newswire_ravenpack_crosswalk.py                 # default
    python create_newswire_ravenpack_crosswalk.py --min-score 85  # stricter
    python create_newswire_ravenpack_crosswalk.py --status        # show stats
"""

import argparse
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
DEFAULT_MIN_SCORE = 80.0

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
# Data loading
# ---------------------------------------------------------------------------


def _load_data(data_dir=DATA_DIR):
    """Load newswire and RavenPack data, returning two DataFrames.

    RavenPack is filtered to the newswire date range to save memory.
    """
    logger.info("Loading newswire headlines...")
    nw = load_newswire_headlines(data_dir).collect()
    nw = nw.with_columns(pl.col("date").cast(pl.Date).alias("pub_date"))
    nw = nw.filter(pl.col("headline").is_not_null() & (pl.col("headline").str.strip_chars() != ""))
    logger.info(f"  Newswire: {len(nw):,} headlines, {nw['pub_date'].min()} to {nw['pub_date'].max()}")

    nw_start = nw["pub_date"].min()
    nw_end = nw["pub_date"].max()

    logger.info("Loading RavenPack headlines...")
    rp = (
        pl.scan_parquet(data_dir / "ravenpack_djpr.parquet")
        .with_columns(pl.col("timestamp_utc").cast(pl.Date).alias("date"))
        .filter((pl.col("date") >= nw_start) & (pl.col("date") <= nw_end))
        .filter(pl.col("headline").is_not_null() & (pl.col("headline").str.strip_chars() != ""))
        .select("date", "rp_story_id", "rp_entity_id", "entity_name", "headline", "source_name")
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
# Main crosswalk builder
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
        return pl.DataFrame(
            schema={
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
        )

    return pl.DataFrame(all_rows)


# ---------------------------------------------------------------------------
# Status reporting
# ---------------------------------------------------------------------------


def _print_status(output_path):
    """Print summary statistics for an existing crosswalk file."""
    output_path = Path(output_path)
    if not output_path.exists():
        print(f"No crosswalk found at {output_path}")
        return

    cw = pl.read_parquet(output_path)
    n = len(cw)
    print(f"Crosswalk: {output_path}")
    print(f"  Rows: {n:,}")

    if n == 0:
        return

    print(f"  Date range: {cw['date'].min()} to {cw['date'].max()}")
    print(f"  Distinct dates: {cw['date'].n_unique()}")
    print(f"  Distinct newswire URLs: {cw['nw_source_url'].n_unique():,}")
    print(f"  Distinct RP stories: {cw['rp_story_id'].n_unique():,}")
    print(f"  Distinct RP entities: {cw['rp_entity_id'].n_unique():,}")

    scores = cw["fuzzy_score"]
    print(f"\n  Fuzzy score: min={scores.min():.1f}, median={scores.median():.1f}, "
          f"mean={scores.mean():.1f}, max={scores.max():.1f}")

    print("\n  Matches by newswire source:")
    for row in cw.group_by("nw_source").agg(pl.len().alias("n")).sort("n", descending=True).iter_rows(named=True):
        print(f"    {row['nw_source']}: {row['n']:,}")

    print("\n  Matches by RavenPack source:")
    for row in cw.group_by("rp_source_name").agg(pl.len().alias("n")).sort("n", descending=True).iter_rows(named=True):
        print(f"    {row['rp_source_name']}: {row['n']:,}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create a crosswalk between newswire and RavenPack headlines via fuzzy matching.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python create_newswire_ravenpack_crosswalk.py                    # default run\n"
            "  python create_newswire_ravenpack_crosswalk.py --min-score 85     # stricter threshold\n"
            "  python create_newswire_ravenpack_crosswalk.py --status           # show existing stats\n"
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
        "--status",
        action="store_true",
        help="Print summary of existing crosswalk and exit.",
    )
    args = parser.parse_args()

    output_path = Path(args.output) if args.output else DEFAULT_OUTPUT

    if args.status:
        _print_status(output_path)
    else:
        nw, rp = _load_data()
        crosswalk = build_crosswalk(nw, rp, min_score=args.min_score)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        crosswalk.write_parquet(output_path)
        logger.info(f"Saved {len(crosswalk):,} rows to {output_path}")

        _print_status(output_path)
