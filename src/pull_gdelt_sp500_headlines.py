"""
Pull GDELT GKG headlines filtered to S&P 500 companies via BigQuery.

Uploads a normalized S&P 500 company names lookup table to BigQuery,
then JOINs it server-side against GDELT's V2Organizations field. This
filters ~436K global headlines/day down to only those mentioning S&P 500
companies, dramatically reducing data transfer.

Three modes:
  Default    — pull a single sample month into the data lake directory
  --full     — pull all months (Feb 2015+) into the data lake (slow, hours+)
  --estimate — estimate cost and time for the full pull

The data lake uses Hive-style partitioning:
  DATA_DIR/gdelt_sp500_headlines/year=YYYY/month=MM/data.parquet

Months already on disk are skipped, so interrupted runs resume where
they left off. Polars auto-detects the Hive partition columns (year,
month) when scanning the directory.

Loader functions:
  load_gdelt_sp500_headlines()  — LazyFrame scanning the Hive-partitioned data lake
  filter_to_month(lf, month)    — filter using Hive partition columns (year, month)

Prerequisites:
- Google Cloud SDK installed
- A GCP project with BigQuery API enabled
- Authentication via: gcloud auth application-default login
- GCP_PROJECT set in .env
- sp500_names_lookup.parquet already built (run pull_sp500_constituents.py first)
"""

import argparse
import html
import time
from datetime import date, datetime
from pathlib import Path

import polars as pl
from google.cloud import bigquery

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
GCP_PROJECT = config("GCP_PROJECT")

SAMPLE_MONTH = "2025-01"

GDELT_FULL_START = "2015-02-01"

GDELT_SP500_DIR = DATA_DIR / "gdelt_sp500_headlines"


def _extract_page_title(extras: str | None) -> str | None:
    """Extract text between <PAGE_TITLE> and </PAGE_TITLE> tags."""
    if extras is None:
        return None
    start_tag = "<PAGE_TITLE>"
    end_tag = "</PAGE_TITLE>"
    start = extras.find(start_tag)
    if start == -1:
        return None
    start += len(start_tag)
    end = extras.find(end_tag, start)
    if end == -1:
        return None
    return extras[start:end]


def _clean_headlines(df: pl.DataFrame) -> pl.DataFrame:
    """Unescape HTML entities, drop empty headlines, dedup keeping earliest."""
    df = df.with_columns(
        pl.col("headline").map_elements(
            lambda x: html.unescape(x) if x is not None else x,
            return_dtype=pl.Utf8,
        )
    )
    df = df.filter(
        pl.col("headline").is_not_null() & (pl.col("headline").str.strip_chars() != "")
    )
    df = df.sort("gkg_date").unique(subset=["headline"], keep="first")
    return df


def _month_start_end(month: str) -> tuple[str, str]:
    """Given 'YYYY-MM', return (first_day, first_day_of_next_month) as date strings."""
    dt = datetime.strptime(month, "%Y-%m").date()
    if dt.month == 12:
        next_month = dt.replace(year=dt.year + 1, month=1, day=1)
    else:
        next_month = dt.replace(month=dt.month + 1, day=1)
    return dt.strftime("%Y-%m-%d"), next_month.strftime("%Y-%m-%d")


def _generate_month_ranges(start_date: str, end_date: str):
    """Yield (month_start, month_end) date string pairs for each month in range."""
    current = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    while current < end:
        if current.month == 12:
            next_month = current.replace(year=current.year + 1, month=1, day=1)
        else:
            next_month = current.replace(month=current.month + 1, day=1)
        month_end = min(next_month, end)
        yield current.strftime("%Y-%m-%d"), month_end.strftime("%Y-%m-%d")
        current = next_month


def _month_filename(month_start: str) -> str:
    """Return output filename for a given month, e.g. '2019-06.parquet'."""
    return f"{month_start[:7]}.parquet"


def _hive_partition_path(output_dir: Path, month_start: str) -> Path:
    """Return Hive-partitioned path: output_dir/year=YYYY/month=MM/data.parquet."""
    year = month_start[:4]
    month = month_start[5:7]
    return output_dir / f"year={year}" / f"month={month}" / "data.parquet"


def _upload_names_lookup(client: bigquery.Client, project: str) -> str:
    """Upload the S&P 500 names lookup table to BigQuery.

    Creates the dataset if needed. Returns the fully qualified table ID.
    """
    dataset_id = f"{project}.news_headlines"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    client.create_dataset(dataset, exists_ok=True)

    table_id = f"{dataset_id}.sp500_names_lookup"

    lookup_path = DATA_DIR / "sp500_names_lookup.parquet"
    if not lookup_path.exists():
        raise FileNotFoundError(
            f"{lookup_path} not found. Run pull_sp500_constituents.py first."
        )

    import pandas as pd

    lookup_df = pd.read_parquet(lookup_path)
    print(f"Uploading {len(lookup_df):,} rows to {table_id}...")

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_dataframe(lookup_df, table_id, job_config=job_config)
    job.result()

    print(f"  Upload complete: {table_id}")
    return table_id


def _build_query(month_start: str, month_end: str, project: str) -> str:
    """Build the BigQuery SQL that joins GDELT against S&P 500 names.

    The query:
    1. Filters to English articles (TranslationInfo IS NULL)
    2. UNNESTs V2Organizations to get individual org entries
    3. Strips trailing char offset from each org entry
    4. Normalizes both sides (lowercase, strip punctuation/suffixes)
    5. INNER JOINs against the uploaded lookup table on comnam_norm
    """
    return f"""
    WITH orgs AS (
        SELECT
            PARSE_TIMESTAMP('%E4Y%m%d%H%M%S', CAST(DATE AS STRING)) AS gkg_date,
            DocumentIdentifier AS source_url,
            SourceCommonName AS source_name,
            Extras,
            V2Tone,
            V2Organizations,
            REGEXP_REPLACE(org_entry, r',\\d+$', '') AS org_name
        FROM `gdelt-bq.gdeltv2.gkg_partitioned`,
            UNNEST(SPLIT(V2Organizations, ';')) AS org_entry
        WHERE _PARTITIONTIME >= TIMESTAMP('{month_start}')
          AND _PARTITIONTIME < TIMESTAMP('{month_end}')
          AND Extras LIKE '%<PAGE_TITLE>%'
          AND TranslationInfo IS NULL
          AND V2Organizations IS NOT NULL
          AND V2Organizations != ''
    ),
    orgs_norm AS (
        SELECT *,
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        LOWER(org_name),
                        r"[&.',]", ' '
                    ),
                    r'\\b(inc|corp|corporation|co|company|ltd|limited|llc|lp|plc|group|holdings|holding|enterprises|enterprise|intl|international|technologies|technology|systems|industries|services|bancorp|bancshares|financial)\\b',
                    ' '
                ),
                r'\\s+', ' '
            ) AS org_name_norm
        FROM orgs
    )
    SELECT DISTINCT
        o.gkg_date,
        o.source_url,
        o.source_name,
        o.Extras,
        o.V2Tone,
        o.V2Organizations,
        TRIM(o.org_name) AS matched_org_raw,
        s.comnam AS matched_company,
        s.permno,
        s.ticker
    FROM orgs_norm o
    INNER JOIN `{project}.news_headlines.sp500_names_lookup` s
        ON TRIM(o.org_name_norm) = s.comnam_norm
    """


def _pull_and_clean_sp500_month(
    client: bigquery.Client,
    month_start: str,
    month_end: str,
    project: str,
    output_dir: Path,
) -> tuple[int, int]:
    """Query BigQuery for one month of S&P 500-filtered headlines.

    Returns (number of cleaned rows written, bytes processed).
    """
    query = _build_query(month_start, month_end, project)

    job = client.query(query)
    rows = job.result()
    bytes_processed = job.total_bytes_processed or 0
    df = pl.from_arrow(rows.to_arrow())

    df = df.with_columns(
        pl.col("Extras")
        .map_elements(_extract_page_title, return_dtype=pl.Utf8)
        .alias("headline")
    ).drop("Extras")

    if len(df) == 0:
        out_path = _hive_partition_path(output_dir, month_start)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(out_path)
        return 0, bytes_processed

    df = _clean_headlines(df)

    out_path = _hive_partition_path(output_dir, month_start)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out_path)
    return len(df), bytes_processed


def pull_gdelt_sp500_sample(
    month=SAMPLE_MONTH,
    project=GCP_PROJECT,
    output_dir=None,
):
    """Pull a single month of GDELT headlines into the data lake directory.

    Writes to output_dir/YYYY-MM.parquet using the same format as the
    full pull. Returns the number of rows written.
    """
    if output_dir is None:
        output_dir = GDELT_SP500_DIR
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    month_start, month_end = _month_start_end(month)

    out_path = _hive_partition_path(output_dir, month_start)
    if out_path.exists():
        print(f"{out_path} already exists. Delete it to re-pull.")
        return 0

    client = bigquery.Client(project=project)

    print("Uploading S&P 500 names lookup to BigQuery...")
    _upload_names_lookup(client, project)

    print(f"Querying GDELT S&P 500 for {month} ({month_start} to {month_end})...")
    n_rows, _ = _pull_and_clean_sp500_month(
        client,
        month_start,
        month_end,
        project,
        output_dir,
    )
    print(f"  Headlines after cleaning: {n_rows:,}")
    print(f"Saved to {out_path}")
    return n_rows


def pull_gdelt_sp500_full(
    start_date=GDELT_FULL_START,
    end_date=None,
    project=GCP_PROJECT,
    output_dir=None,
):
    """Pull the full GDELT headline dataset filtered to S&P 500 companies.

    Each month is cleaned and written as a separate parquet file. Months
    already on disk are skipped for resumability.
    """
    if end_date is None:
        end_date = date.today().strftime("%Y-%m-%d")
    if output_dir is None:
        output_dir = GDELT_SP500_DIR
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    month_ranges = list(_generate_month_ranges(start_date, end_date))
    total_months = len(month_ranges)

    print(
        f"Full GDELT S&P 500 pull: {start_date} to {end_date} ({total_months} months)"
    )
    print("Completed months are skipped on re-run.")
    print(f"Output directory: {output_dir}\n")

    client = bigquery.Client(project=project)

    print("Uploading S&P 500 names lookup to BigQuery...")
    _upload_names_lookup(client, project)
    print()

    for i, (m_start, m_end) in enumerate(month_ranges, 1):
        out_path = _hive_partition_path(output_dir, m_start)
        if out_path.exists():
            print(f"  [{i}/{total_months}] {m_start[:7]}: already exists, skipping")
            continue

        print(f"  [{i}/{total_months}] {m_start[:7]}: querying BigQuery...", end="")
        n_rows, _ = _pull_and_clean_sp500_month(
            client, m_start, m_end, project, output_dir
        )
        print(f" {n_rows:,} rows")

    print(f"\nDone. Monthly parquets are in {output_dir}")


def estimate_full_pull(
    start_date=GDELT_FULL_START,
    end_date=None,
    project=GCP_PROJECT,
):
    """Estimate cost and time for a full GDELT S&P 500 pull.

    Runs 2 test months (one early, one recent), measures wall-clock time
    and bytes processed, then extrapolates to the full date range.
    """
    if end_date is None:
        end_date = date.today().strftime("%Y-%m-%d")

    month_ranges = list(_generate_month_ranges(start_date, end_date))
    total_months = len(month_ranges)

    # Pick two test months: one early, one mid-range
    test_indices = [0, total_months // 2]
    test_months = [month_ranges[i] for i in test_indices]

    client = bigquery.Client(project=project)

    print("Uploading S&P 500 names lookup to BigQuery...")
    _upload_names_lookup(client, project)

    print(f"\nEstimating full pull: {start_date} to {end_date} ({total_months} months)")
    print("Running 2 test queries (dry run disabled — actual queries)...\n")

    total_seconds = 0
    total_bytes = 0

    for m_start, m_end in test_months:
        query = _build_query(m_start, m_end, project)
        print(f"  Test month {m_start[:7]}...", end="", flush=True)
        t0 = time.time()
        job = client.query(query)
        job.result()  # wait for completion
        elapsed = time.time() - t0
        bytes_processed = job.total_bytes_processed or 0
        total_seconds += elapsed
        total_bytes += bytes_processed
        print(f" {elapsed:.1f}s, {bytes_processed / 1e9:.2f} GB scanned")

    avg_seconds = total_seconds / len(test_months)
    avg_bytes = total_bytes / len(test_months)

    est_total_seconds = avg_seconds * total_months
    est_total_bytes = avg_bytes * total_months
    est_cost = est_total_bytes / 1e12 * 6.25  # $6.25 per TB on-demand

    print(f"\n{'=' * 60}")
    print(f"Estimate for {total_months} months ({start_date} to {end_date}):")
    print(
        f"  Wall-clock time:  {est_total_seconds / 3600:.1f} hours ({est_total_seconds / 60:.0f} min)"
    )
    print(f"  Data scanned:     {est_total_bytes / 1e12:.2f} TB")
    print(f"  BigQuery cost:    ${est_cost:.2f} (on-demand @ $6.25/TB)")
    print(f"{'=' * 60}")

    return {
        "total_months": total_months,
        "est_hours": est_total_seconds / 3600,
        "est_tb": est_total_bytes / 1e12,
        "est_cost_usd": est_cost,
    }


def filter_to_month(lf: pl.LazyFrame, ym: str) -> pl.LazyFrame:
    """Filter to a YYYY-MM month using Hive partition columns."""
    year, month = ym.split("-")
    return lf.filter((pl.col("year") == int(year)) & (pl.col("month") == int(month)))


def load_gdelt_sp500_headlines(data_dir=DATA_DIR):
    """Lazy-scan the Hive-partitioned GDELT S&P 500 headlines directory."""
    return pl.scan_parquet(Path(data_dir) / "gdelt_sp500_headlines")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pull GDELT GKG headlines filtered to S&P 500 companies."
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help=(
            "Pull the full dataset (Feb 2015 to present) instead of the "
            "sample month. This takes many hours but skips months already "
            "on disk so interrupted runs can be resumed."
        ),
    )
    parser.add_argument(
        "--estimate",
        action="store_true",
        help="Estimate cost and time for the full pull without downloading data.",
    )
    parser.add_argument(
        "--month",
        type=str,
        default=SAMPLE_MONTH,
        help=f"YYYY-MM month to pull for the sample (default: {SAMPLE_MONTH}).",
    )
    args = parser.parse_args()

    if args.estimate:
        estimate_full_pull()
    elif args.full:
        pull_gdelt_sp500_full()
    else:
        pull_gdelt_sp500_sample(month=args.month)
