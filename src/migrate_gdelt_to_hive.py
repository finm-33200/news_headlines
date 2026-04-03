"""One-time migration: convert flat GDELT parquet files to Hive-style partitioning.

Converts:  gdelt_sp500_headlines/YYYY-MM.parquet
       ->   gdelt_sp500_headlines/year=YYYY/month=MM/data.parquet

Idempotent: skips months where data.parquet already exists.
After migration, verifies total row count and deletes old flat files.
"""

import re
from pathlib import Path

import polars as pl

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
GDELT_SP500_DIR = DATA_DIR / "gdelt_sp500_headlines"


def migrate():
    flat_files = sorted(GDELT_SP500_DIR.glob("[0-9][0-9][0-9][0-9]-[0-9][0-9].parquet"))

    if not flat_files:
        print("No flat parquet files found — nothing to migrate.")
        return

    print(f"Found {len(flat_files)} flat parquet files to migrate.\n")

    old_total_rows = 0
    new_total_rows = 0

    for f in flat_files:
        match = re.match(r"(\d{4})-(\d{2})\.parquet", f.name)
        if not match:
            continue
        year, month = match.group(1), match.group(2)

        hive_dir = GDELT_SP500_DIR / f"year={year}" / f"month={month}"
        hive_path = hive_dir / "data.parquet"

        df = pl.read_parquet(f)
        old_total_rows += len(df)

        if hive_path.exists():
            print(f"  {f.name}: hive path already exists, skipping write")
            new_total_rows += pl.read_parquet(hive_path).height
            continue

        hive_dir.mkdir(parents=True, exist_ok=True)
        df.write_parquet(hive_path)
        new_total_rows += len(df)
        print(
            f"  {f.name} -> {hive_path.relative_to(GDELT_SP500_DIR)} ({len(df):,} rows)"
        )

    print("\nRow count verification:")
    print(f"  Old flat files: {old_total_rows:,}")
    print(f"  New hive files: {new_total_rows:,}")

    if old_total_rows != new_total_rows:
        print("  ERROR: Row counts do not match! Aborting deletion of old files.")
        return

    print("  OK — counts match.\n")

    print("Deleting old flat files...")
    for f in flat_files:
        f.unlink()
        print(f"  Deleted {f.name}")

    print(f"\nMigration complete. {len(flat_files)} files migrated to Hive layout.")


if __name__ == "__main__":
    migrate()
