"""
Pull a small sample of GDELT GKG headlines from Google BigQuery.

GDELT's GKG 2.0 table stores page titles (headlines) in the Extras XML field
via <PAGE_TITLE>...</PAGE_TITLE> tags, available from ~September 2019 onward.

This is an exploratory pull to assess GDELT as a headline source.

Prerequisites:
- Google Cloud SDK installed
- A GCP project with BigQuery API enabled
- Authentication via: gcloud auth application-default login
- GCP_PROJECT set in .env
"""

import html
from pathlib import Path

import polars as pl
from google.cloud import bigquery

from settings import config

DATA_DIR = Path(config("DATA_DIR"))
GCP_PROJECT = config("GCP_PROJECT")

GDELT_START_DATE = "2024-01-08"
GDELT_END_DATE = "2024-01-15"


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


def pull_gdelt_headlines(
    start_date=GDELT_START_DATE, end_date=GDELT_END_DATE, project=GCP_PROJECT
):
    """
    Pull GDELT GKG headlines from BigQuery for the given date range.

    Uses partition pruning on _PARTITIONTIME and filters for rows
    containing <PAGE_TITLE> tags server-side to minimize bytes scanned.

    Returns a cleaned Polars DataFrame with columns:
        gkg_date, source_url, source_name, headline
    """
    client = bigquery.Client(project=project)

    query = f"""
    SELECT
        PARSE_TIMESTAMP('%E4Y%m%d%H%M%S', CAST(DATE AS STRING)) AS gkg_date,
        DocumentIdentifier AS source_url,
        SourceCommonName AS source_name,
        Extras
    FROM `gdelt-bq.gdeltv2.gkg_partitioned`
    WHERE _PARTITIONTIME >= TIMESTAMP('{start_date}')
      AND _PARTITIONTIME < TIMESTAMP('{end_date}')
      AND Extras LIKE '%<PAGE_TITLE>%'
    """

    print(f"Querying GDELT GKG from {start_date} to {end_date}...")
    rows = client.query(query).result()
    df = pl.from_arrow(rows.to_arrow())
    print(f"  Raw rows returned: {len(df):,}")

    df = df.with_columns(
        pl.col("Extras")
        .map_elements(_extract_page_title, return_dtype=pl.Utf8)
        .alias("headline")
    ).drop("Extras")
    df = _clean_headlines(df)
    print(f"  Headlines after cleaning: {len(df):,}")

    return df


def load_gdelt_headlines(data_dir=DATA_DIR):
    path = Path(data_dir) / "gdelt_gkg_headlines_sample.parquet"
    return pl.read_parquet(path)


if __name__ == "__main__":
    df = pull_gdelt_headlines(
        start_date=GDELT_START_DATE, end_date=GDELT_END_DATE
    )
    path = Path(DATA_DIR) / "gdelt_gkg_headlines_sample.parquet"
    df.write_parquet(path)
    print(f"Saved to {path}")
