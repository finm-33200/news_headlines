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
import logging
from pathlib import Path

import polars as pl

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


def _load_ravenpack(data_dir):
    """Load full RavenPack DJPR dataset."""
    path = data_dir / "ravenpack_djpr.parquet"
    logger.info(f"Loading RavenPack from {path}")
    rp = pl.read_parquet(path)
    logger.info(f"  {len(rp):,} rows, {len(rp.columns)} columns")
    return rp


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


def build_merged_dataset(data_dir=DATA_DIR):
    """Build the merged dataset.

    Strategy:
      1. Load both crosswalks, unify column names, and concatenate.
      2. When both sources match the same rp_story_id, keep the higher
         fuzzy_score match.
      3. Left-join the full ravenpack_djpr onto the deduplicated crosswalk
         via rp_story_id so every matched row gets all RP metadata.
    """
    rp = _load_ravenpack(data_dir)

    # Rename RP headline to avoid collision with scraped headline
    rp = rp.rename({"headline": "rp_headline"})

    # Load crosswalks
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

    # Union all crosswalk matches
    unified = pl.concat(parts)
    logger.info(f"  Combined crosswalk: {len(unified):,} rows before dedup")

    # Deduplicate: for each rp_story_id, keep the match with the highest fuzzy_score
    unified = unified.sort("fuzzy_score", descending=True).unique(
        subset=["rp_story_id"], keep="first"
    )
    logger.info(f"  Combined crosswalk: {len(unified):,} rows after dedup")

    # Join: bring in all RP metadata
    # Drop rp_headline from the crosswalk side — we'll use the one from rp
    unified = unified.drop("rp_headline")
    merged = rp.join(unified, on="rp_story_id", how="left")

    logger.info(f"  Merged dataset: {len(merged):,} rows")
    matched = merged.filter(pl.col("headline").is_not_null())
    logger.info(
        f"  Rows with scraped headline: {len(matched):,} "
        f"({100 * len(matched) / len(merged):.1f}%)"
    )

    return merged


def _print_status(output_path):
    """Print summary statistics for the merged dataset."""
    output_path = Path(output_path)
    if not output_path.exists():
        print(f"No merged dataset found at {output_path}")
        return

    df = pl.read_parquet(output_path)
    n = len(df)
    print(f"Merged dataset: {output_path}")
    print(f"  Total rows: {n:,}")

    if n == 0:
        return

    has_headline = df.filter(pl.col("headline").is_not_null())
    print(f"  Rows with scraped headline: {len(has_headline):,} ({100 * len(has_headline) / n:.1f}%)")
    print(f"  Rows without scraped headline: {n - len(has_headline):,}")

    if len(has_headline) > 0:
        print("\n  By headline source:")
        for row in (
            has_headline.group_by("headline_source")
            .agg(pl.len().alias("n"))
            .sort("n", descending=True)
            .iter_rows(named=True)
        ):
            print(f"    {row['headline_source']}: {row['n']:,}")

        scores = has_headline["fuzzy_score"]
        print(
            f"\n  Fuzzy score: min={scores.min():.1f}, median={scores.median():.1f}, "
            f"mean={scores.mean():.1f}, max={scores.max():.1f}"
        )

    print(f"\n  Columns ({len(df.columns)}):")
    for col in df.columns:
        print(f"    {col}: {df[col].dtype}")


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
        merged = build_merged_dataset()

        output_path.parent.mkdir(parents=True, exist_ok=True)
        merged.write_parquet(output_path)
        logger.info(f"Saved {len(merged):,} rows to {output_path}")

        _print_status(output_path)
