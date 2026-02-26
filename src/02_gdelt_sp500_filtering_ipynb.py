# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.18.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# # GDELT for Finance: Filtering Global News to S&P 500 Companies
#
# Financial research often needs firm-level news data — headlines,
# sentiment, and publication timing for individual companies. Commercial
# feeds like **RavenPack** provide this out of the box: articles are
# pre-tagged to entities with relevance scores and sentiment. But
# commercial feeds are expensive and their coverage choices are opaque.
#
# **GDELT** (the Global Database of Events, Language, and Tone) is a
# free, open alternative that monitors virtually all English-language
# news on the open web. The catch: GDELT is a *firehose* — roughly
# 400,000+ articles per day, covering everything from geopolitics to
# sports. To use it for finance, we need to filter it down to articles
# that actually mention the companies we care about.
#
# This notebook walks through that process end to end:
#
# 1. **What is GDELT?** — the project, its tables, and how to access them
# 2. **GDELT tables and how they fit together** — Events, GKG, Mentions
# 3. **Data dictionary** — the fields we use and the output we produce
# 4. **Hive-style partitioning** — how we organize the data lake on disk
# 5. **Setup & data loading**
# 6. **Building the S&P 500 names lookup** — normalization and pitfalls
# 7. **The server-side JOIN** — querying GDELT in BigQuery
# 8. **The filtering funnel** — measuring each filter stage
# 9. **Coverage analysis** — what matched
# 10. **The false positive problem** — what went wrong and why
# 11. **Spot-check** — verifying clean matches
# 12. **Comparison with RavenPack** — how does our DIY filter stack up?
# 13. **Why the overlap is so low** — a source-level analysis
# 14. **Summary & lessons learned**

# %% [markdown]
# ---
# ## 1. What Is GDELT?
#
# The **Global Database of Events, Language, and Tone** (GDELT) is a
# free, open research project that monitors news media worldwide. Some
# key facts:
#
# - **Coverage**: processes news from virtually every country, in 100+
#   languages, from online news, print, and broadcast sources.
# - **Scale**: ingests roughly **400,000+ English-language articles per
#   day** from the open web.
# - **Update frequency**: new data every **15 minutes**, 24/7.
# - **NLP extraction**: for each article, GDELT automatically extracts
#   named entities (people, organizations, locations), themes, tone/
#   sentiment, and other metadata using natural language processing.
# - **Access**: all data is freely available on **Google BigQuery**. You
#   pay only for query scans (typically pennies per query), not for
#   storage.
# - **History**: GDELT 2.0 has been running continuously since
#   **February 2015**.
#
# **How does GDELT compare to RavenPack?**
#
# | | RavenPack | GDELT |
# |---|---|---|
# | **Cost** | Commercial (tens of thousands $/year) | Free (BigQuery scan costs only) |
# | **Entity tagging** | Curated, high-quality entity matching with relevance scores | Raw NLP extraction — noisy, no relevance scores |
# | **Sentiment** | Pre-computed event sentiment scores ($-1$ to $+1$) | Basic tone measures (positive/negative word fractions) |
# | **Filtering** | Pre-filtered to companies | All news worldwide — you filter yourself |
# | **Coverage** | US-focused, financial news emphasis | Global, all topics |
#
# The tradeoff is clear: RavenPack gives you clean, ready-to-use data
# but is expensive and opaque. GDELT gives you everything but requires
# significant engineering to extract a usable signal.

# %% [markdown]
# ---
# ## 2. GDELT Tables and How They Fit Together
#
# GDELT 2.0 exposes three main tables on BigQuery, each capturing a
# different aspect of global news:
#
# | Table | BigQuery ID | What it contains | Update frequency |
# |---|---|---|---|
# | **Events** | `gdelt-bq.gdeltv2.events_partitioned` | Structured event records (who did what to whom, where) using the CAMEO coding system | Every 15 min |
# | **Global Knowledge Graph (GKG)** | `gdelt-bq.gdeltv2.gkg_partitioned` | Article-level metadata: organizations, persons, themes, tone, locations, page titles | Every 15 min |
# | **Mentions** | `gdelt-bq.gdeltv2.eventmentions_partitioned` | Links events to the specific articles that mention them | Every 15 min |
#
# For our purposes, we use the **GKG table** because it gives us two
# things we need:
#
# 1. **Organization names** (`V2Organizations`) — a list of every
#    organization GDELT's NLP detected in the article. We match these
#    against S&P 500 company names.
# 2. **Page titles** (`Extras` field) — the article headline, which we
#    extract from `<PAGE_TITLE>` XML tags.
#
# The Events table uses the CAMEO event coding system, which is oriented
# toward political and conflict events (protests, military actions,
# diplomatic meetings) rather than corporate news — not what we want.
#
# **Important**: all three tables are **partitioned by date**. When
# querying, you should always filter on the partition column
# (`_PARTITIONTIME`) to avoid scanning the entire table history. This
# is called **partition pruning** and is essential for keeping BigQuery
# costs low.

# %% [markdown]
# ---
# ## 3. Data Dictionary
#
# ### GKG fields we use
#
# These are the raw columns from the GKG table that our query touches.
# Understanding them is essential for reading the SQL in Section 7.
#
# | GKG Column | Type | Description |
# |---|---|---|
# | `DATE` | Integer | Record timestamp as `YYYYMMDDHHmmSS` — parsed to a proper timestamp in our query |
# | `DocumentIdentifier` | String | Full URL of the source article |
# | `SourceCommonName` | String | Domain or common name of the news source (e.g., `nytimes.com`) |
# | `V2Organizations` | String | Semicolon-delimited list of organization names mentioned in the article, each suffixed with a character offset (e.g., `apple inc,123;microsoft corp,456`) |
# | `V2Tone` | String | Comma-delimited tone measures: average tone, positive score, negative score, polarity, activity reference density, self/group reference density |
# | `Extras` | String | XML-like field containing `<PAGE_TITLE>...</PAGE_TITLE>` tags — this is where we extract the article headline |
# | `TranslationInfo` | String | Translation metadata; `NULL` for natively English articles — we use this to filter to English only |
# | `_PARTITIONTIME` | Timestamp | BigQuery partition key — always filter on this for cost control |
#
# The `V2Organizations` field is the key to our matching strategy. It
# gives us GDELT's best guess at which organizations are mentioned in
# each article. We normalize these names and JOIN against our S&P 500
# lookup table.
#
# ### Output columns: `gdelt_sp500_headlines/year=YYYY/month=MM/data.parquet`
#
# The data lake uses **Hive-style partitioning**: each month is stored at
# `gdelt_sp500_headlines/year=YYYY/month=MM/data.parquet`. Polars
# auto-detects the `year` and `month` partition columns when scanning
# the directory with `pl.scan_parquet()`. The columns in each file are:
#
# | Column | Description |
# |---|---|
# | `gkg_date` | GKG record timestamp (parsed from the raw integer `DATE` field) |
# | `source_url` | Full URL of the source article (`DocumentIdentifier`) |
# | `source_name` | Domain / common name of the news source (`SourceCommonName`) |
# | `headline` | Page title extracted from `<PAGE_TITLE>` tags in `Extras`, HTML-unescaped |
# | `matched_org_raw` | The raw organization string from `V2Organizations` that triggered the match |
# | `matched_company` | S&P 500 company name from the CRSP lookup that matched |
# | `permno` | CRSP permanent security identifier |
# | `ticker` | Stock ticker symbol |
# | `V2Tone` | Tone measures from the GKG record |
# | `V2Organizations` | Full semicolon-delimited organization list from the article |

# %% [markdown]
# ---
# ## 4. Hive-Style Partitioning: Organizing the Data Lake
#
# Our pipeline stores the filtered GDELT headlines as a collection of
# Parquet files organized in **Hive-style partitioned** directories.
# This is a storage convention worth understanding because it appears
# everywhere in modern data engineering — Spark, Hive, DuckDB, Polars,
# and PyArrow all recognize it natively.
#
# ### What is Hive-style partitioning?
#
# Instead of writing one giant file, you split the data into folders
# whose names encode the value of one or more **partition columns**.
# The convention is `column_name=value`:
#
# ```
# gdelt_sp500_headlines/
# ├── year=2015/
# │   ├── month=02/
# │   │   └── data.parquet
# │   ├── month=03/
# │   │   └── data.parquet
# │   └── ...
# ├── year=2016/
# │   └── month=01/
# │       └── data.parquet
# └── ...
# ```
#
# Each leaf file contains only the rows for that year–month combination.
# The partition columns (`year`, `month`) are not stored inside the
# Parquet file itself — they are inferred from the directory names at
# read time.
#
# ### Why we use it
#
# Three practical benefits drive this design choice:
#
# 1. **Partition pruning.** When you filter on a partition column,
#    the reader skips entire directories that don't match — it never
#    opens those files at all. For example, loading a single month
#    from a multi-year data lake reads one Parquet file instead of
#    scanning every file and filtering in memory. This is the same
#    idea as BigQuery's `_PARTITIONTIME` pruning (Section 2), but
#    applied to your local file system.
#
# 2. **Incremental writes.** Each month is an independent file in
#    its own directory. Adding a new month means writing one new file
#    without touching any existing data. If a pull is interrupted
#    mid-run, you resume by writing only the months that are missing —
#    no need to reprocess the entire dataset.
#
# 3. **Transparency.** The directory names are human-readable. A
#    quick `ls` tells you which months are on disk, how large each
#    file is, and whether anything is missing — no code required.
#
# ### How Polars reads it
#
# Polars auto-detects the Hive layout when you point `scan_parquet`
# at the top-level directory:
#
# ```python
# lf = pl.scan_parquet("gdelt_sp500_headlines/")
# ```
#
# This returns a lazy frame with all the original data columns **plus**
# two virtual columns — `year` and `month` — inferred from the
# directory names. Filtering on these columns triggers partition
# pruning automatically:
#
# ```python
# # Only reads the file at year=2025/month=01/data.parquet
# jan_2025 = lf.filter(
#     (pl.col("year") == 2025) & (pl.col("month") == 1)
# ).collect()
# ```

# %% [markdown]
# ---
# ## 5. Setup & Data Loading

# %%
from datetime import datetime
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import polars as pl
from google.cloud import bigquery

from pull_gdelt_sp500_headlines import (
    SAMPLE_MONTH,
    filter_to_month,
    load_gdelt_sp500_headlines,
)
from settings import config

DATA_DIR = Path(config("DATA_DIR"))
GCP_PROJECT = config("GCP_PROJECT")

client = bigquery.Client(project=GCP_PROJECT)

# Derive date range from the sample month
_sm_dt = datetime.strptime(SAMPLE_MONTH, "%Y-%m").date()
SAMPLE_START = _sm_dt.strftime("%Y-%m-%d")
if _sm_dt.month == 12:
    SAMPLE_END = _sm_dt.replace(year=_sm_dt.year + 1, month=1, day=1).strftime(
        "%Y-%m-%d"
    )
else:
    SAMPLE_END = _sm_dt.replace(month=_sm_dt.month + 1, day=1).strftime("%Y-%m-%d")

# %% [markdown]
# We work with two files already on disk:
#
# - **S&P 500-filtered GDELT sample** — one month from the data lake
#   (`gdelt_sp500_headlines/year=YYYY/month=MM/data.parquet`), already
#   pulled by `pull_gdelt_sp500_headlines.py`.
# - **S&P 500 names lookup** — the mapping table used for the
#   server-side JOIN, built by `pull_sp500_constituents.py`.

# %%
gd_sp = filter_to_month(load_gdelt_sp500_headlines(), SAMPLE_MONTH).collect()
lookup = pd.read_parquet(DATA_DIR / "sp500_names_lookup.parquet")

print(f"Sample month: {SAMPLE_MONTH} ({SAMPLE_START} to {SAMPLE_END})")
print(f"S&P 500-filtered headlines (1 month): {len(gd_sp):>10,}")
print(f"S&P 500 names lookup rows:            {len(lookup):>10,}")

# %% [markdown]
# ---
# ## 6. Building the S&P 500 Names Lookup
#
# Before we can filter GDELT, we need a table of company names to match
# against. This section explains how the lookup table is built.
#
# ### Where the names come from: CRSP stocknames
#
# We pull from WRDS/CRSP's `stocknames` table joined with
# `dsp500list_v2` (the S&P 500 membership history). This gives us every
# historical name for every stock that has ever been in the S&P 500.
# Having historical names matters because GDELT articles from 2010 might
# reference "Apple Computer" rather than "Apple Inc" — we want to match
# both.
#
# ```python
# # From pull_sp500_constituents.py
# db.raw_sql("""
#     SELECT s.permno, s.indno, s.mbrstartdt, s.mbrenddt, s.mbrflg, s.indfam,
#            n.comnam, n.ticker, n.ncusip, n.namedt, n.nameenddt, n.siccd, n.exchcd
#     FROM crsp_m_indexes.dsp500list_v2 s
#     LEFT JOIN crsp.stocknames n ON s.permno = n.permno
# """)
# ```
#
# The result has multiple rows per company when a name changes over
# time — for example, PERMNO 14593 has rows for both "APPLE COMPUTER
# INC" and "APPLE INC".

# %% [markdown]
# ### Name normalization
#
# GDELT might store an organization as "Apple Inc", "apple", or
# "APPLE INC." — we need all of these to match the same lookup entry.
# The normalization function lowercases the name, strips punctuation,
# removes common corporate suffixes (inc, corp, ltd, etc.), and
# collapses whitespace:
#
# ```python
# # From pull_sp500_constituents.py
#
# SUFFIX_PATTERN = re.compile(
#     r"\b(inc|corp|corporation|co|company|ltd|limited|llc|lp|plc|"
#     r"group|holdings|holding|enterprises|enterprise|intl|international|"
#     r"technologies|technology|systems|industries|services|bancorp|"
#     r"bancshares|financial)\b"
# )
#
# def normalize_company_name(name: str) -> str:
#     s = name.lower()
#     s = re.sub(r"[&.',]", " ", s)       # remove punctuation
#     s = SUFFIX_PATTERN.sub(" ", s)       # strip suffixes
#     s = re.sub(r"\s+", " ", s).strip()   # collapse whitespace
#     return s
# ```
#
# The same normalization logic is replicated in BigQuery SQL (using
# nested `REGEXP_REPLACE` calls) so the JOIN can happen server-side.
# Let's see what the normalized names look like:

# %%
print("Sample normalized names from the lookup table:\n")
samples = lookup.sample(10, random_state=42)[
    ["comnam", "comnam_norm", "ticker", "permno"]
]
for _, row in samples.iterrows():
    ticker = row.ticker if pd.notna(row.ticker) else "N/A"
    print(f'  "{row.comnam}"  →  "{row.comnam_norm}"  (ticker={ticker})')

# %% [markdown]
# ### Filtering out short names
#
# After normalization, some company names reduce to very short strings
# or even common English words. "GAP INC" becomes "gap", "FOX CORP"
# becomes "fox", "BALL CORP" becomes "ball". These would match
# thousands of unrelated GDELT organizations. Our lookup table filters
# out any name where `len(comnam_norm) < 5`:
#
# ```python
# # From pull_sp500_constituents.py — build_sp500_names_lookup()
# lookup = lookup[lookup["comnam_norm"].str.len() >= 5].reset_index(drop=True)
# ```
#
# This removes the worst single-word offenders at the cost of missing a
# few legitimate short-named companies. Let's see which names are still
# at the boundary:

# %%
# Show normalized names that are <= 6 chars — high false-positive risk
short_names = lookup[lookup["comnam_norm"].str.len() <= 6].copy()
short_names = short_names.sort_values("comnam_norm").drop_duplicates("comnam_norm")

print(f"Lookup entries with normalized name <= 6 characters: {len(short_names)}")
print("\nExamples of short normalized names:\n")
for _, row in short_names.head(20).iterrows():
    ticker = row.ticker if pd.notna(row.ticker) else "N/A"
    print(f'  "{row.comnam}"  →  "{row.comnam_norm}"  (ticker={ticker})')

# %% [markdown]
# ---
# ## 7. The Server-Side JOIN: Querying GDELT in BigQuery
#
# A naive approach would be to download all GDELT rows and filter
# locally — but that would mean transferring millions of rows per day.
# Instead, we upload our small lookup table (~4,500 rows) to BigQuery
# and JOIN server-side. One table scan, minimal data transfer.
#
# Here is the query from `pull_gdelt_sp500_headlines.py`. It uses two
# CTEs (Common Table Expressions) before the final JOIN:
#
# ```sql
# -- CTE 1: "orgs" — filter and unnest
# -- Filters to the date partition, requires English articles
# -- (TranslationInfo IS NULL), requires a <PAGE_TITLE> in Extras,
# -- requires non-empty V2Organizations. Then UNNESTs the semicolon-
# -- delimited V2Organizations into one row per org mention per article.
# -- Strips the trailing character offset (e.g., ",123") from each entry.
#
# WITH orgs AS (
#     SELECT
#         PARSE_TIMESTAMP('%E4Y%m%d%H%M%S', CAST(DATE AS STRING)) AS gkg_date,
#         DocumentIdentifier AS source_url,
#         SourceCommonName AS source_name,
#         Extras,
#         V2Tone,
#         V2Organizations,
#         REGEXP_REPLACE(org_entry, r',\d+$', '') AS org_name
#     FROM `gdelt-bq.gdeltv2.gkg_partitioned`,
#         UNNEST(SPLIT(V2Organizations, ';')) AS org_entry
#     WHERE _PARTITIONTIME >= TIMESTAMP('{month_start}')
#       AND _PARTITIONTIME < TIMESTAMP('{month_end}')
#       AND Extras LIKE '%<PAGE_TITLE>%'
#       AND TranslationInfo IS NULL
#       AND V2Organizations IS NOT NULL
#       AND V2Organizations != ''
# ),
#
# -- CTE 2: "orgs_norm" — normalize organization names
# -- Mirrors the Python normalize_company_name() function in SQL:
# -- lowercase, strip punctuation, remove corporate suffixes, collapse
# -- whitespace. This lets us JOIN on the same normalized form.
#
# orgs_norm AS (
#     SELECT *,
#         REGEXP_REPLACE(
#             REGEXP_REPLACE(
#                 REGEXP_REPLACE(
#                     LOWER(org_name),
#                     r"[&.',]", ' '
#                 ),
#                 r'\b(inc|corp|corporation|co|company|ltd|limited|llc|lp|plc
#                   |group|holdings|holding|enterprises|enterprise|intl
#                   |international|technologies|technology|systems|industries
#                   |services|bancorp|bancshares|financial)\b',
#                 ' '
#             ),
#             r'\s+', ' '
#         ) AS org_name_norm
#     FROM orgs
# )
#
# -- Final SELECT: INNER JOIN against the uploaded S&P 500 lookup table
# -- on the normalized name. Only articles mentioning an S&P 500 company
# -- survive the JOIN.
#
# SELECT DISTINCT
#     o.gkg_date, o.source_url, o.source_name, o.Extras,
#     o.V2Tone, o.V2Organizations,
#     TRIM(o.org_name) AS matched_org_raw,
#     s.comnam AS matched_company,
#     s.permno, s.ticker
# FROM orgs_norm o
# INNER JOIN `{project}.news_headlines.sp500_names_lookup` s
#     ON TRIM(o.org_name_norm) = s.comnam_norm
# ```
#
# After the query returns, two Python post-processing steps clean the
# data:
#
# **1. Headline extraction** — the `Extras` field contains XML-like
# tags; we extract the text between `<PAGE_TITLE>` and `</PAGE_TITLE>`:
#
# ```python
# # From pull_gdelt_sp500_headlines.py
# def _extract_page_title(extras):
#     start_tag = "<PAGE_TITLE>"
#     end_tag = "</PAGE_TITLE>"
#     start = extras.find(start_tag)
#     if start == -1:
#         return None
#     start += len(start_tag)
#     end = extras.find(end_tag, start)
#     if end == -1:
#         return None
#     return extras[start:end]
# ```
#
# **2. Headline cleaning** — unescape HTML entities (`&amp;` → `&`),
# drop empty headlines, and deduplicate keeping the earliest occurrence:
#
# ```python
# # From pull_gdelt_sp500_headlines.py
# def _clean_headlines(df):
#     df = df.with_columns(
#         pl.col("headline").map_elements(
#             lambda x: html.unescape(x) if x is not None else x,
#             return_dtype=pl.Utf8,
#         )
#     )
#     df = df.filter(
#         pl.col("headline").is_not_null()
#         & (pl.col("headline").str.strip_chars() != "")
#     )
#     df = df.sort("gkg_date").unique(subset=["headline"], keep="first")
#     return df
# ```

# %% [markdown]
# ---
# ## 8. The Filtering Funnel
#
# Each filter in our query cuts the data dramatically. Let's measure
# each stage with cheap BigQuery `COUNT(*)` queries against our
# sample month. These queries scan metadata or a single small column,
# so they cost fractions of a cent.

# %%
funnel_queries = {
    "1. All GKG rows (in partition window)": f"""
        SELECT COUNT(*) AS n
        FROM `gdelt-bq.gdeltv2.gkg_partitioned`
        WHERE _PARTITIONTIME >= TIMESTAMP('{SAMPLE_START}')
          AND _PARTITIONTIME < TIMESTAMP('{SAMPLE_END}')
    """,
    "2. Has headline (<PAGE_TITLE>)": f"""
        SELECT COUNT(*) AS n
        FROM `gdelt-bq.gdeltv2.gkg_partitioned`
        WHERE _PARTITIONTIME >= TIMESTAMP('{SAMPLE_START}')
          AND _PARTITIONTIME < TIMESTAMP('{SAMPLE_END}')
          AND Extras LIKE '%<PAGE_TITLE>%'
    """,
    "3. English only (TranslationInfo IS NULL)": f"""
        SELECT COUNT(*) AS n
        FROM `gdelt-bq.gdeltv2.gkg_partitioned`
        WHERE _PARTITIONTIME >= TIMESTAMP('{SAMPLE_START}')
          AND _PARTITIONTIME < TIMESTAMP('{SAMPLE_END}')
          AND Extras LIKE '%<PAGE_TITLE>%'
          AND TranslationInfo IS NULL
    """,
    "4. Has V2Organizations": f"""
        SELECT COUNT(*) AS n
        FROM `gdelt-bq.gdeltv2.gkg_partitioned`
        WHERE _PARTITIONTIME >= TIMESTAMP('{SAMPLE_START}')
          AND _PARTITIONTIME < TIMESTAMP('{SAMPLE_END}')
          AND Extras LIKE '%<PAGE_TITLE>%'
          AND TranslationInfo IS NULL
          AND V2Organizations IS NOT NULL
          AND V2Organizations != ''
    """,
}

print(f"Filtering funnel for GDELT GKG ({SAMPLE_START} to {SAMPLE_END}):\n")
print(f"{'Stage':<45} {'Rows':>12} {'% of prev':>10}")
print("-" * 69)

funnel_counts = {}
prev_n = None
for label, query in funnel_queries.items():
    result = client.query(query).result()
    n = list(result)[0]["n"]
    funnel_counts[label] = n
    pct = f"{n / prev_n * 100:.1f}%" if prev_n else ""
    print(f"{label:<45} {n:>12,} {pct:>10}")
    prev_n = n

# Add the final two stages from the local data
n_sp500 = len(gd_sp)
pct_sp500 = f"{n_sp500 / prev_n * 100:.1f}%"
funnel_counts["5. Matches S&P 500 company name"] = n_sp500
print(f"{'5. Matches S&P 500 company name':<45} {n_sp500:>12,} {pct_sp500:>10}")

# %%
# Visualize the funnel
labels = list(funnel_counts.keys())
counts = list(funnel_counts.values())

fig, ax = plt.subplots(figsize=(10, 5))
bars = ax.barh(labels[::-1], counts[::-1], color="steelblue", edgecolor="white")

# Add count labels on the bars
for bar, count in zip(bars, counts[::-1]):
    ax.text(
        bar.get_width() + max(counts) * 0.01,
        bar.get_y() + bar.get_height() / 2,
        f"{count:,}",
        va="center",
        fontsize=9,
    )

ax.set_xlabel("Number of rows")
ax.set_title(f"GDELT Filtering Funnel ({SAMPLE_START} to {SAMPLE_END})")
ax.set_xlim(0, max(counts) * 1.15)
fig.tight_layout()
plt.show()

total_reduction = counts[0] / counts[-1] if counts[-1] > 0 else float("inf")
print(
    f"\nTotal reduction: {counts[0]:,} → {counts[-1]:,} ({total_reduction:.0f}× smaller)"
)

# %% [markdown]
# ---
# ## 9. What Matched: Coverage Analysis
#
# How many distinct S&P 500 companies appeared in one month of
# GDELT?

# %%
matched_companies = (
    gd_sp.group_by("matched_company", "ticker", "permno")
    .agg(pl.len().alias("n_headlines"))
    .sort("n_headlines", descending=True)
)

n_permnos = gd_sp["permno"].n_unique()
n_companies = matched_companies.height

print(f"Distinct PERMNOs matched:   {n_permnos}")
print(f"Distinct (company, ticker): {n_companies}")

# %%
print("Top 25 companies by headline count:\n")
for row in matched_companies.head(25).iter_rows():
    company, ticker, permno, n = row
    ticker = ticker if ticker is not None else "N/A"
    print(f"  {n:>6,}  {ticker:<8s}  {company}")

# %% [markdown]
# Look carefully at the top of that list. If any single company has
# tens of thousands of headlines in a single week, something is
# probably wrong. That leads us to the next section.

# %% [markdown]
# ---
# ## 10. The False Positive Problem
#
# ### Why some matches are wrong
#
# Our normalization strips corporate suffixes like "bancorp", "corp",
# and "inc". This is essential for matching — but it can produce
# **generic normalized names** that match far too many GDELT
# organizations.
#
# The classic example: `"UNITED STATES BANCORP"` normalizes to
# `"united states"` (because `bancorp` is a stripped suffix). In GDELT,
# *any* organization with "united states" in its name matches —
# "United States Congress", "United States Department of Defense", etc.
#
# Let's see which companies are most affected:

# %%
# Find companies whose normalized name is suspiciously common
SUSPICIOUS_THRESHOLD = 20_000

company_counts = (
    gd_sp.group_by("matched_company")
    .agg(pl.len().alias("n"))
    .sort("n", descending=True)
)

# Show the top 10 with their normalized forms
top_offenders = company_counts.head(10).to_pandas()
lookup_map = lookup.drop_duplicates("comnam")[["comnam", "comnam_norm"]].set_index(
    "comnam"
)

print(f"{'Company':<35} {'Norm. Name':<25} {'Headlines':>10}")
print("-" * 72)
for _, row in top_offenders.iterrows():
    company = row["matched_company"]
    n = row["n"]
    norm = (
        lookup_map.loc[company, "comnam_norm"] if company in lookup_map.index else "?"
    )
    print(f"{company:<35} {norm:<25} {n:>10,}")

# %% [markdown]
# ### What false-positive headlines look like
#
# Let's look at the actual headlines attributed to our worst
# false-positive offenders. These are real GDELT headlines that matched
# an S&P 500 company name only because of the generic normalized form.

# %%
# Show irrelevant headlines for the top false-positive companies
fp_companies = ["UNITED STATES BANCORP"]
# Add any company with high headline count (likely false positive)
big_hitters = company_counts.filter(pl.col("n") > SUSPICIOUS_THRESHOLD)[
    "matched_company"
].to_list()
fp_companies = list(dict.fromkeys(fp_companies + big_hitters))  # dedupe, preserve order

for company in fp_companies[:3]:
    subset = gd_sp.filter(pl.col("matched_company") == company)
    norm = (
        lookup_map.loc[company, "comnam_norm"] if company in lookup_map.index else "?"
    )
    print(f"\n{'=' * 80}")
    print(f'{company}  (normalizes to "{norm}")')
    print(f"{len(subset):,} headlines — sample of clearly irrelevant ones:")
    print(f"{'=' * 80}")
    # Show headlines that do NOT contain the company's ticker or a recognizable
    # fragment — these are almost certainly false positives
    sample = subset.sample(min(15, len(subset)), seed=42)
    for row in sample.iter_rows(named=True):
        matched_org = row.get("matched_org_raw", "")
        hl = row["headline"][:120]
        print(f"  org: {matched_org:<30s}  headline: {hl}")

# %% [markdown]
# ### Categories of false positives
#
# There are several recurring patterns:
#
# 1. **Names that become common phrases after suffix stripping** —
#    "UNITED STATES BANCORP" → "united states", "NEWS CORP" → "news"
#
# 2. **Very short names** — "GAP INC" → "gap", "FOX CORP" → "fox",
#    "BALL CORP" → "ball". These common English words appear in many
#    GDELT organization names.
#
# 3. **Names that become empty** — "LLC CORP" → "" and
#    "LIMITED INC" → "" (all tokens are suffixes).
#
# This is a fundamental tension in name matching: **aggressive
# normalization improves recall** (we catch more true matches) **but
# hurts precision** (we also catch more false matches). There is no
# free lunch.

# %% [markdown]
# ### Quantifying the damage

# %%
fig, axes = plt.subplots(1, 2, figsize=(12, 4))

headline_counts = matched_companies["n_headlines"].to_numpy()

# Left panel: full distribution
axes[0].hist(headline_counts, bins=50, color="steelblue", edgecolor="white")
axes[0].set_xlabel("Headlines per company")
axes[0].set_ylabel("Number of companies")
axes[0].set_title("Distribution of headline counts per company")

# Right panel: log-log rank plot
ranked = np.sort(headline_counts)[::-1]
axes[1].loglog(range(1, len(ranked) + 1), ranked, "o-", markersize=3, color="steelblue")
axes[1].set_xlabel("Company rank")
axes[1].set_ylabel("Headlines")
axes[1].set_title("Rank-frequency plot (log-log)")

fig.tight_layout()
plt.show()

# %%
# Flag the top false-positive candidates — companies with a very high headline
# count are almost certainly over-matching
suspicious = matched_companies.filter(pl.col("n_headlines") > SUSPICIOUS_THRESHOLD)
suspicious_headlines = gd_sp.filter(
    pl.col("matched_company").is_in(suspicious["matched_company"])
)

n_suspicious = len(suspicious_headlines)
n_clean = len(gd_sp) - n_suspicious

print(f"Companies with > {SUSPICIOUS_THRESHOLD:,} headlines/month: {suspicious.height}")
print(
    f"Headlines from suspicious matches:    {n_suspicious:>8,} ({n_suspicious / len(gd_sp) * 100:.1f}%)"
)
print(
    f"Headlines from remaining companies:   {n_clean:>8,} ({n_clean / len(gd_sp) * 100:.1f}%)"
)

# %%
print("Suspicious companies:\n")
for row in suspicious.iter_rows():
    company, ticker, permno, n = row
    ticker = ticker if ticker is not None else "N/A"
    print(f"  {n:>6,}  {ticker:<8s}  {company}")

# %% [markdown]
# ### Mitigation strategies
#
# Our pipeline already applies one mitigation — the minimum 5-character
# filter on normalized names (Section 6). But that doesn't catch longer
# problematic names like "united states" (13 chars). Here are additional
# strategies, ordered from simplest to most involved:
#
# 1. **Manual blocklist** — maintain a list of known-bad normalized
#    names ("united states", etc.) and exclude them from the lookup.
#    Simple but requires manual curation.
#
# 2. **Longer minimum name length** — raise the threshold from 5 to 8+
#    characters. Eliminates more false positives but sacrifices some
#    legitimate short-named companies.
#
# 3. **Post-hoc headline filtering** — after downloading the matched
#    headlines, verify that the company name (or ticker) actually
#    appears in the headline text, not just the metadata.
#
# 4. **Ticker-based matching** — some GDELT entries include ticker
#    symbols. Using tickers as a secondary match signal could improve
#    precision.
#
# 5. **Time-aware matching** — only match company names that were valid
#    during the article's publication date, using `namedt`/`nameenddt`
#    from CRSP. This prevents matching defunct names against modern
#    articles.

# %% [markdown]
# ---
# ## 11. Spot-Check: Do the Clean Matches Look Right?
#
# After removing the suspicious high-count companies, let's verify that
# the remaining matches are actually relevant headlines about the
# matched companies.

# %%
clean_sp = gd_sp.filter(~pl.col("matched_company").is_in(suspicious["matched_company"]))

# Sample some well-known companies
for company in [
    "APPLE INC",
    "MICROSOFT CORP",
    "NVIDIA CORP",
    "JPMORGAN CHASE & CO",
    "WALMART INC",
]:
    subset = clean_sp.filter(pl.col("matched_company") == company)
    if len(subset) == 0:
        continue
    print(f"\n{'=' * 70}")
    print(f"{company} — {len(subset):,} headlines")
    print(f"{'=' * 70}")
    for row in subset.sample(min(5, len(subset)), seed=42).iter_rows(named=True):
        print(f"  [{row['ticker']}] {row['headline'][:100]}")

# %% [markdown]
# ---
# ## 12. Comparison with RavenPack
#
# How does our DIY GDELT filtering compare to the industry-standard
# RavenPack feed? RavenPack costs tens of thousands of dollars per year
# while GDELT is free, so any reasonable coverage from GDELT is
# valuable as an alternative or complement.

# %%
rp = pl.scan_parquet(DATA_DIR / "ravenpack_djpr.parquet")

# Filter RavenPack to same date window
rp_window = (
    rp.with_columns(pl.col("timestamp_utc").cast(pl.Date).alias("date"))
    .filter(
        (pl.col("date") >= pl.lit(SAMPLE_START).str.to_date("%Y-%m-%d"))
        & (pl.col("date") < pl.lit(SAMPLE_END).str.to_date("%Y-%m-%d"))
    )
    .collect()
)

print(f"Same period ({SAMPLE_MONTH}):")
print(f"  RavenPack (US, high relevance):     {len(rp_window):>8,}")
print(f"  GDELT S&P 500 filtered (all):       {len(gd_sp):>8,}")
print(f"  GDELT S&P 500 filtered (clean):     {n_clean:>8,}")

# %%
# Compare daily volumes
rp_daily = (
    rp_window.with_columns(pl.col("timestamp_utc").cast(pl.Date).alias("date"))
    .group_by("date")
    .agg(pl.len().alias("n"))
    .sort("date")
)

clean_daily = (
    clean_sp.with_columns(pl.col("gkg_date").cast(pl.Date).alias("date"))
    .group_by("date")
    .agg(pl.len().alias("n"))
    .sort("date")
)

fig, ax = plt.subplots(figsize=(10, 4))
ax.bar(
    rp_daily["date"].to_list(),
    rp_daily["n"].to_list(),
    color="darkorange",
    alpha=0.8,
    label="RavenPack",
    width=0.35,
)
ax.bar(
    [d for d in clean_daily["date"].to_list()],
    clean_daily["n"].to_list(),
    color="steelblue",
    alpha=0.8,
    label="GDELT S&P 500 (clean)",
    width=0.35,
)
ax.set_ylabel("Headlines per day")
ax.set_title("Daily headline volume: RavenPack vs GDELT S&P 500 filtered")
ax.legend()
ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
fig.tight_layout()
plt.show()

# %% [markdown]
# ---
# ## 13. Why the Overlap Is So Low: A Source-Level Analysis
#
# The daily volume comparison above shows that GDELT produces *more*
# headlines per day than RavenPack for S&P 500 companies — yet when we
# fuzzy-match the two (see notebook 03), only about **7% of RavenPack
# headlines have a near-exact match** (score >= 90) in GDELT. Why?
#
# The answer is that these two datasets draw from **fundamentally
# different news ecosystems**. Let's look at the actual sources each
# one uses.

# %%
# What sources does RavenPack use?
rp_sources = (
    rp_window.group_by("source_name")
    .agg(pl.len().alias("n"))
    .sort("n", descending=True)
)

print("RavenPack — top sources (this week):\n")
for row in rp_sources.head(10).iter_rows():
    print(f"  {row[1]:>6,}  {row[0]}")
print(f"\n  Total unique sources: {rp_sources.height}")

# %%
# What sources does GDELT use?
gd_sources = (
    gd_sp.group_by("source_name").agg(pl.len().alias("n")).sort("n", descending=True)
)

print("GDELT S&P 500 filtered — top sources (same week):\n")
for row in gd_sources.head(15).iter_rows():
    print(f"  {row[1]:>6,}  {row[0]}")
print(f"\n  Total unique sources: {gd_sources.height}")

# %% [markdown]
# The picture is stark:
#
# - **RavenPack is ~95% wire services** — Dow Jones Newswires, PR
#   Newswire, Business Wire, GlobeNewswire. These are licensed,
#   structured feeds that publish corporate earnings announcements, SEC
#   filings, analyst actions, and press releases. They are the backbone
#   of institutional financial news.
#
# - **GDELT crawls the open web** — Yahoo Finance, ticker aggregator
#   sites (tickerreport.com, marketscreener.com), tabloids, local news,
#   and thousands of smaller outlets. It does *not* have direct access
#   to the licensed wire feeds that RavenPack subscribes to.
#
# Let's check: do RavenPack's key wire services appear in GDELT at all?

# %%
# Check which RP sources show up in GDELT (even via their .com domains)
gd_source_list = gd_sources["source_name"].drop_nulls().to_list()

rp_key_wires = {
    "Dow Jones / WSJ": ["dowjones", "wsj"],
    "PR Newswire": ["prnewswire"],
    "Business Wire": ["businesswire"],
    "GlobeNewswire": ["globenewswire"],
    "MarketWatch": ["marketwatch"],
}

print("RavenPack's key wire services — presence in GDELT:\n")
for label, keywords in rp_key_wires.items():
    matches = [
        (s, gd_sources.filter(pl.col("source_name") == s)["n"].item())
        for s in gd_source_list
        if any(k in s.lower() for k in keywords)
    ]
    if matches:
        match_strs = [f"{s} ({n:,})" for s, n in matches[:3]]
        print(f"  {label:<25s} → {', '.join(match_strs)}")
    else:
        print(f"  {label:<25s} → NOT FOUND in GDELT")

# %%
# What fraction of GDELT headlines come from wire services?
wire_keywords = ["prnewswire", "businesswire", "globenewswire", "newswire", "presswire"]
n_wire = gd_sp.filter(
    pl.col("source_name").is_not_null()
    & pl.col("source_name").str.to_lowercase().str.contains("|".join(wire_keywords))
).height

print(
    f"GDELT headlines from wire services: {n_wire:,} / {len(gd_sp):,} ({n_wire / len(gd_sp) * 100:.1f}%)"
)
print(
    f"\nIn other words, ~{100 - n_wire / len(gd_sp) * 100:.0f}% of GDELT's S&P 500 coverage"
)
print("comes from sources that RavenPack doesn't cover at all.")

# %% [markdown]
# ### Implications
#
# The low fuzzy-match rate (~7%) is **not a failure of our filtering**
# — it reflects a fundamental difference in source coverage:
#
# | | RavenPack | GDELT |
# |---|---|---|
# | **Primary sources** | Licensed wire services (DJ, PR Newswire, Business Wire) | Open web (Yahoo, aggregators, blogs, regional news) |
# | **Content type** | Press releases, earnings, SEC filings | General news articles, opinion, regional coverage |
# | **Wire service %** | ~95% | ~1–2% |
#
# This means GDELT and RavenPack are **mostly complementary, not
# overlapping**. The ~7% overlap comes from the small number of press
# releases that also get republished on the open web (e.g.,
# prnewswire.com, globenewswire.com).
#
# ### What if you want to replicate RavenPack's coverage for free?
#
# You would need to scrape the wire services directly. The three major
# free, publicly-accessible wire service websites are:
#
# - **prnewswire.com** — PR Newswire's public-facing site. Structured
#   press release pages with company tags, dates, and full text.
# - **businesswire.com** — Business Wire's public site. Similar
#   structure.
# - **globenewswire.com** — GlobeNewswire's public site.
#
# Together, these three sources account for the majority of RavenPack's
# non-Dow-Jones content. Scraping them would give you structured press
# releases with company names, timestamps, and headlines — much closer
# to RavenPack's corpus than GDELT provides.
#
# The **Dow Jones Newswires** content (RavenPack's single largest
# source at 26M+ articles) is behind a paywall and cannot be freely
# scraped. This is the irreducible gap between a free pipeline and a
# commercial feed like RavenPack.

# %% [markdown]
# ---
# ## 14. Summary & Lessons Learned
#
# ### What worked
#
# | Strategy | Why it works |
# |---|---|
# | Server-side JOIN in BigQuery | Upload a small lookup table (~4,500 rows), scan GDELT once, avoid downloading millions of rows |
# | Historical names from CRSP `stocknames` | Captures name changes over time (e.g., "APPLE COMPUTER INC" to "APPLE INC") |
# | Suffix stripping | GDELT often stores names without corporate suffixes — normalization makes both sides match |
# | Minimum name length filter (>= 5 chars) | Removes the worst single-word false positives (gap, fox, ball, etc.) |
#
# ### What didn't work
#
# - **Generic names after normalization**: "UNITED STATES BANCORP" →
#   "united states" matches every US government entity.
# - **Entity ambiguity**: "Morgan Stanley" the person vs. the firm.
# - **Rebranded companies**: "FACEBOOK INC" and "TWITTER INC" still
#   appear widely in GDELT even after the platforms rebranded to Meta
#   and X.
#
# ### Key takeaway
#
# Working with open data like GDELT requires understanding the data's
# structure, limitations, and noise. The filtering funnel shows that
# going from raw data to a usable research dataset involves multiple
# stages of cleaning, each with tradeoffs between **coverage**
# (catching all relevant articles) and **precision** (avoiding false
# matches). There is no single perfect filter — the right balance
# depends on your downstream research question.

# %%
