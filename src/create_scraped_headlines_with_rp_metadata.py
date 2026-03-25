"""
Create a merged dataset joining scraped headlines (newswire + GDELT) with
full RavenPack metadata via the fuzzy-match crosswalks.

The output has two headline columns:
  - ``headline``: the scraped text (safe to send to OpenAI / LLMs)
  - ``rp_headline``: the original RavenPack headline (for reference / QA)

All RavenPack metadata columns from ravenpack_djpr.parquet are included.

Output:
    DATA_DIR / scraped_headlines_with_rp_metadata.parquet

Usage:
    python create_scraped_headlines_with_rp_metadata.py           # default
    python create_scraped_headlines_with_rp_metadata.py --status  # show stats
"""

import argparse
import gc
import logging
import os
import sys
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
DEFAULT_OUTPUT = DATA_DIR / "scraped_headlines_with_rp_metadata.parquet"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Columns carried from each crosswalk into the unified schema
_UNIFIED_COLS = [
    "headline",
    "headline_source",
    "headline_source_url",
    "rp_headline",
    "fuzzy_score",
    "rp_story_id",
]


def _open_ravenpack(data_dir):
    """Open the RavenPack DJPR parquet file for row-group streaming.

    Returns a ``pyarrow.parquet.ParquetFile`` handle — no data is loaded
    into memory.  The caller iterates over row groups one at a time.
    """
    path = data_dir / "ravenpack_djpr.parquet"
    logger.info(f"Opening RavenPack from {path}")
    pf = pq.ParquetFile(path)
    logger.info(f"  {pf.metadata.num_rows:,} rows, {pf.metadata.num_row_groups} row groups")
    return pf


def _load_newswire_crosswalk(data_dir):
    """Load newswire crosswalk and rename to unified column names."""
    path = data_dir / "newswire_ravenpack_crosswalk.parquet"
    if not path.exists():
        logger.warning(f"Newswire crosswalk not found at {path}")
        return None
    cw = pl.read_parquet(path)
    logger.info(f"  Newswire crosswalk: {len(cw):,} rows")
    return cw.select(
        pl.col("nw_headline").alias("headline"),
        pl.lit("newswire").alias("headline_source"),
        pl.col("nw_source_url").alias("headline_source_url"),
        "rp_headline",
        "fuzzy_score",
        "rp_story_id",
    )


def _load_gdelt_crosswalk(data_dir):
    """Load GDELT crosswalk and rename to unified column names."""
    path = data_dir / "gdelt_ravenpack_crosswalk.parquet"
    if not path.exists():
        logger.warning(f"GDELT crosswalk not found at {path}")
        return None
    cw = pl.read_parquet(path)
    logger.info(f"  GDELT crosswalk: {len(cw):,} rows")
    return cw.select(
        pl.col("gdelt_headline").alias("headline"),
        pl.lit("gdelt").alias("headline_source"),
        pl.col("gdelt_source_url").alias("headline_source_url"),
        "rp_headline",
        "fuzzy_score",
        "rp_story_id",
    )


def build_merged_dataset(data_dir=DATA_DIR, output_path=DEFAULT_OUTPUT):
    """Build the merged dataset using row-group chunked processing.

    Streams RavenPack row groups from disk, joins each with the
    (small) crosswalk, and writes results incrementally via
    ``pyarrow.parquet.ParquetWriter``.  Peak memory is roughly one
    row group + crosswalk + join result (~1-2 GB).

    Strategy:
      1. Load both crosswalks, unify column names, and concatenate.
      2. When both sources match the same rp_story_id, keep the higher
         fuzzy_score match.
      3. For each RP row group, left-join with the deduplicated crosswalk
         and write the result to disk immediately.
    """
    # --- crosswalk loading (small, ~60 MB total) ---
    parts = []
    nw_cw = _load_newswire_crosswalk(data_dir)
    if nw_cw is not None:
        parts.append(nw_cw)
    gdelt_cw = _load_gdelt_crosswalk(data_dir)
    if gdelt_cw is not None:
        parts.append(gdelt_cw)

    if not parts:
        logger.error("No crosswalk files found. Cannot build merged dataset.")
        raise FileNotFoundError("At least one crosswalk parquet must exist.")

    unified = pl.concat(parts)
    logger.info(f"  Combined crosswalk: {len(unified):,} rows before dedup")

    unified = unified.sort("fuzzy_score", descending=True).unique(
        subset=["rp_story_id"], keep="first"
    )
    logger.info(f"  Combined crosswalk: {len(unified):,} rows after dedup")

    # Drop rp_headline from the crosswalk side — we'll use the one from rp
    unified = unified.drop("rp_headline")

    # --- row-group streaming over RavenPack ---
    pf = _open_ravenpack(data_dir)
    tmp_path = output_path.with_suffix(".parquet.tmp")
    writer = None
    total_rows = 0
    total_matched = 0
    n_groups = pf.metadata.num_row_groups

    try:
        for i in range(n_groups):
            rg = pf.read_row_group(i)
            chunk = pl.from_arrow(rg)
            del rg

            chunk = chunk.rename({"headline": "rp_headline"})
            merged_chunk = chunk.join(unified, on="rp_story_id", how="left")
            del chunk

            chunk_matched = merged_chunk.filter(pl.col("headline").is_not_null()).height
            total_rows += merged_chunk.height
            total_matched += chunk_matched

            chunk_arrow = merged_chunk.to_arrow()
            del merged_chunk

            if writer is None:
                writer = pq.ParquetWriter(tmp_path, chunk_arrow.schema)
            writer.write_table(chunk_arrow)

            logger.info(
                f"  Row group {i + 1}/{n_groups}: "
                f"{chunk_arrow.num_rows:,} rows, {chunk_matched:,} matched"
            )
            del chunk_arrow
            gc.collect()
    finally:
        if writer is not None:
            writer.close()

    # Atomic rename
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path.rename(output_path)

    logger.info(f"  Merged dataset: {total_rows:,} rows")
    logger.info(
        f"  Rows with scraped headline: {total_matched:,} "
        f"({100 * total_matched / total_rows:.1f}%)"
    )
    return total_rows


def _print_status(output_path):
    """Print summary statistics for the merged dataset (lazy, low memory)."""
    output_path = Path(output_path)
    if not output_path.exists():
        print(f"No merged dataset found at {output_path}")
        return

    lf = pl.scan_parquet(output_path)
    schema = lf.collect_schema()

    n = lf.select(pl.len()).collect().item()
    print(f"Merged dataset: {output_path}")
    print(f"  Total rows: {n:,}")

    if n == 0:
        return

    stats = (
        lf.select(
            pl.col("headline").is_not_null().sum().alias("has_headline"),
            pl.col("headline").is_not_null().mean().alias("pct_headline"),
        )
        .collect()
        .row(0, named=True)
    )
    has = stats["has_headline"]
    print(f"  Rows with scraped headline: {has:,} ({100 * stats['pct_headline']:.1f}%)")
    print(f"  Rows without scraped headline: {n - has:,}")

    if has > 0:
        print("\n  By headline source:")
        source_counts = (
            lf.filter(pl.col("headline").is_not_null())
            .group_by("headline_source")
            .agg(pl.len().alias("n"))
            .sort("n", descending=True)
            .collect()
        )
        for row in source_counts.iter_rows(named=True):
            print(f"    {row['headline_source']}: {row['n']:,}")

        score_stats = (
            lf.filter(pl.col("headline").is_not_null())
            .select(
                pl.col("fuzzy_score").min().alias("min"),
                pl.col("fuzzy_score").median().alias("median"),
                pl.col("fuzzy_score").mean().alias("mean"),
                pl.col("fuzzy_score").max().alias("max"),
            )
            .collect()
            .row(0, named=True)
        )
        print(
            f"\n  Fuzzy score: min={score_stats['min']:.1f}, "
            f"median={score_stats['median']:.1f}, "
            f"mean={score_stats['mean']:.1f}, max={score_stats['max']:.1f}"
        )

    print(f"\n  Columns ({len(schema.names())}):")
    for name, dtype in schema.items():
        print(f"    {name}: {dtype}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create merged dataset of scraped headlines with RavenPack metadata.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
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
        help="Print summary of existing merged dataset and exit.",
    )
    args = parser.parse_args()

    output_path = Path(args.output) if args.output else DEFAULT_OUTPUT

    if args.status:
        _print_status(output_path)
    else:
        total = build_merged_dataset(output_path=output_path)
        logger.info(f"Saved {total:,} rows to {output_path}")
        _print_status(output_path)

    # Force-exit to avoid Windows access violation (0xC0000005) during
    # interpreter shutdown caused by native library cleanup (pyarrow).
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(0)
