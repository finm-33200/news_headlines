"""
Pull RavenPack DJ Press Release headlines from WRDS.

Tables are partitioned by year: ravenpack_dj.rpa_djpr_equities_YYYY.
Pulls from 2000-01-01 through the present (skipping years whose tables
do not yet exist on WRDS).

Key filters (matching Chen, Kelly, and Xiu 2022):
- entity_type = 'COMP' (companies only)
- country_code = 'US' (US only)
- relevance >= 90 (high relevance)
- Single-firm stories only (one entity per provider story)

Each year is written as a separate row group via pyarrow.parquet.ParquetWriter
so that only one year of data is in memory at a time (~200-400 MB peak).
"""

import gc
import os
import sys
from datetime import date
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import wrds
from psycopg2 import ProgrammingError

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
WRDS_USERNAME = config("WRDS_USERNAME")

RP_START_DATE = "2000-01-01"


def pull_ravenpack(
    start_date=RP_START_DATE,
    end_date=None,
    wrds_username=WRDS_USERNAME,
    output_path=None,
):
    """
    Pull RavenPack DJ Press Release headlines from WRDS, year by year.

    Filters for US companies with high relevance (>=90) and single-firm
    stories only (one distinct rp_entity_id per provider story).

    If end_date is None, pulls through the current date. Years whose
    tables do not exist on WRDS are skipped gracefully.

    Writes each year as a row group to a single parquet file at output_path.
    Returns the total number of rows written.
    """
    if end_date is None:
        end_date = date.today().strftime("%Y-%m-%d")
    if output_path is None:
        output_path = Path(DATA_DIR) / "ravenpack_djpr.parquet"

    start_year = int(start_date[:4])
    end_year = int(end_date[:4])

    tmp_path = output_path.with_suffix(".parquet.tmp")
    db = wrds.Connection(wrds_username=wrds_username)
    writer = None
    total_rows = 0

    try:
        for year in range(start_year, end_year + 1):
            table = f"ravenpack_dj.rpa_djpr_equities_{year}"
            print(f"Pulling {table}...")

            query = f"""
            WITH single_firm AS (
                SELECT provider_id, provider_story_id
                FROM {table}
                WHERE entity_type = 'COMP'
                  AND country_code = 'US'
                  AND relevance >= 90
                GROUP BY provider_id, provider_story_id
                HAVING COUNT(DISTINCT rp_entity_id) = 1
            )
            SELECT
                a.timestamp_utc,
                a.rp_story_id,
                a.rp_entity_id,
                a.entity_type,
                a.entity_name,
                a.country_code,
                a.relevance,
                a.event_sentiment_score,
                a.event_relevance,
                a.event_similarity_key,
                a.event_similarity_days,
                a.topic,
                a."group" AS rp_group,
                a."type" AS rp_type,
                a.sub_type,
                a.property,
                a.fact_level,
                a.category,
                a.news_type,
                a.rp_source_id,
                a.source_name,
                a.provider_id,
                a.provider_story_id,
                a.headline,
                a.css
            FROM {table} a
            INNER JOIN single_firm sf
                ON a.provider_id = sf.provider_id
                AND a.provider_story_id = sf.provider_story_id
            WHERE a.entity_type = 'COMP'
              AND a.country_code = 'US'
              AND a.relevance >= 90
              AND a.timestamp_utc >= '{start_date}'
              AND a.timestamp_utc <= '{end_date}'
            """

            try:
                df_year = db.raw_sql(query)
            except ProgrammingError:
                print(f"  {table}: skipping (table does not exist on WRDS)")
                continue
            except MemoryError:
                raise
            except Exception as e:
                print(f"  {table}: skipping ({type(e).__name__}: {e})")
                continue

            table_pa = pa.Table.from_pandas(df_year, preserve_index=False)
            del df_year
            gc.collect()

            if writer is None:
                writer = pq.ParquetWriter(tmp_path, table_pa.schema)

            writer.write_table(table_pa)
            total_rows += table_pa.num_rows
            print(f"  {table}: {table_pa.num_rows:,} rows")
            del table_pa
            gc.collect()
    finally:
        db.close()
        del db
        if writer is not None:
            writer.close()
        gc.collect()

    tmp_path.rename(output_path)
    print(f"Total RavenPack headlines: {total_rows:,}")
    return total_rows


def load_ravenpack(data_dir=DATA_DIR):
    path = Path(data_dir) / "ravenpack_djpr.parquet"
    return pl.read_parquet(path)


if __name__ == "__main__":
    path = Path(DATA_DIR) / "ravenpack_djpr.parquet"
    if path.exists():
        print(f"Already exists: {path} — skipping pull.")
    else:
        pull_ravenpack(start_date=RP_START_DATE, output_path=path)
        print(f"Saved to {path}")
    # Force-exit to avoid Windows access violation (0xC0000005) during
    # interpreter shutdown caused by native library cleanup (psycopg2/pyarrow).
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(0)
