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
# # Data Sources Overview
#
# Quick look at each raw dataset pulled by the pipeline.
# Five sources are currently collected or derived:
#
# 1. **RavenPack** Dow Jones Press Release headlines (via WRDS)
# 2. **S&P 500 Constituents** historical membership (via WRDS/CRSP)
# 3. **GDELT** S&P 500–filtered headlines (via BigQuery)
# 4. **Free Newswires** S&P 500–filtered headlines (PR Newswire, Business Wire, GlobeNewswire)
# 5. **Newswire–RavenPack Crosswalk** fuzzy-matched headline bridge between free newswires and RavenPack

# %%
from pathlib import Path

import polars as pl

from settings import config

DATA_DIR = Path(config("DATA_DIR"))

# %% [markdown]
# ---
# ## 1. RavenPack — `ravenpack_djpr.parquet`
#
# Source script: `pull_ravenpack.py`
#
# One row per news article / entity event from the RavenPack Dow Jones Press
# Release feed. Filtered for US companies (`country_code='US'`),
# high relevance ($\geq 90$), and single-firm stories only.
#
# **Key columns:**
#
# | Column | Description |
# |---|---|
# | `timestamp_utc` | Publication timestamp (UTC) |
# | `rp_entity_id` | RavenPack entity identifier |
# | `entity_name` | Company name |
# | `headline` | Article headline text |
# | `event_sentiment_score` | Sentiment score (−1 to +1) |
# | `css` | Composite Sentiment Score (0–100) |
# | `relevance` | Entity relevance to the article (0–100) |
# | `topic` | RavenPack topic classification |
# | `news_type` | News type (e.g. press-release, article) |
# | `source_name` | Name of the news source |

# %%
rp = pl.scan_parquet(DATA_DIR / "ravenpack_djpr.parquet")
n_rows = rp.select(pl.len()).collect().item()
cols = rp.collect_schema().names()
print(f"Rows: {n_rows:,}  |  Columns: {len(cols)}")
print(f"Column names: {cols}")

# %% [markdown]
# ### Example rows

# %%
rp.head(5).collect()

# %% [markdown]
# ### Full headline examples by news type

# %%
for row in (
    rp.group_by("news_type")
    .agg(pl.len().alias("n"))
    .sort("n", descending=True)
    .head(6)
    .collect()
    .iter_rows(named=True)
):
    sample = (
        rp.filter(pl.col("news_type") == row["news_type"])
        .select("headline")
        .head(3)
        .collect()["headline"]
        .to_list()
    )
    print(f"── {row['news_type']} ({row['n']:,} articles) ──")
    for h in sample:
        print(f"  • {h}")
    print()

# %% [markdown]
# ### Date range

# %%
rp.select(
    pl.col("timestamp_utc").min().alias("earliest"),
    pl.col("timestamp_utc").max().alias("latest"),
).collect()

# %% [markdown]
# ### Summary statistics — sentiment & relevance

# %%
rp.select(
    pl.col("event_sentiment_score").mean().alias("mean_sentiment"),
    pl.col("event_sentiment_score").std().alias("std_sentiment"),
    pl.col("event_sentiment_score").median().alias("median_sentiment"),
    pl.col("css").mean().alias("mean_css"),
    pl.col("css").std().alias("std_css"),
    pl.col("relevance").mean().alias("mean_relevance"),
).collect()

# %% [markdown]
# ### Articles per month

# %%
import matplotlib.dates as mdates
import matplotlib.pyplot as plt

rp_monthly = (
    rp.with_columns(pl.col("timestamp_utc").cast(pl.Date).alias("date"))
    .group_by(pl.col("date").dt.truncate("1mo"))
    .agg(pl.len().alias("n_articles"))
    .sort("date")
    .collect()
)

fig, ax = plt.subplots(figsize=(10, 3))
ax.bar(
    rp_monthly["date"].to_list(),
    rp_monthly["n_articles"].to_list(),
    width=25,
    color="steelblue",
    alpha=0.8,
)
ax.xaxis.set_major_locator(mdates.YearLocator(2))
ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
ax.set_ylabel("Articles")
ax.set_title("RavenPack DJPR — articles per month")
fig.tight_layout()
plt.show()

# %% [markdown]
# ### Headline-text duplicates
#
# Each row is unique by `(rp_story_id, rp_entity_id)`, but the same
# headline string can appear on multiple story IDs (re-transmissions,
# corrections, etc.).  How prevalent is this?

# %%
n_total = rp.select(pl.len()).collect().item()
n_unique_story = rp.select(pl.col("rp_story_id").n_unique()).collect().item()
n_unique_headline = rp.select(pl.col("headline").n_unique()).collect().item()
n_unique_headline_date = (
    rp.with_columns(pl.col("timestamp_utc").cast(pl.Date).alias("date"))
    .select(pl.struct("headline", "date").n_unique())
    .collect()
    .item()
)

print(f"Total rows:                    {n_total:>12,}")
print(f"Unique rp_story_id:            {n_unique_story:>12,}")
print(f"Unique headline texts:         {n_unique_headline:>12,}")
print(f"Unique (headline, date) pairs: {n_unique_headline_date:>12,}")

# %% [markdown]
# ### Examples: same headline, different story IDs

# %%
rp_collected = rp.collect()
dup_headlines = (
    rp_collected.group_by("headline")
    .agg(
        pl.col("rp_story_id").n_unique().alias("n_stories"),
        pl.len().alias("n_rows"),
    )
    .filter(pl.col("n_stories") > 1)
    .sort("n_stories", descending=True)
)
print(f"Headlines appearing under multiple story IDs: {len(dup_headlines):,}")
print()
for row in dup_headlines.head(5).iter_rows(named=True):
    print(f"  \"{row['headline']}\"")
    print(f"    → {row['n_stories']} distinct story IDs, {row['n_rows']} total rows")

# %% [markdown]
# ---
# ## 2. S&P 500 Constituents — `sp500_constituents.parquet`
#
# Source script: `pull_sp500_constituents.py`
#
# Historical membership list of the S&P 500 index from CRSP
# (`crsp_m_indexes.dsp500list_v2`). Each row is one membership spell
# for a single PERMNO — i.e. the period during which a stock was part
# of the index. Used here as an entity-information lookup (company
# identifiers and index membership dates).
#
# **Key columns:**
#
# | Column | Description |
# |---|---|
# | `permno` | CRSP permanent security identifier |
# | `mbrstartdt` | Date the stock entered the S&P 500 (inclusive) |
# | `mbrenddt` | Date the stock exited the S&P 500 (exclusive) |
# | `indno` | Index identifier |
# | `mbrflg` | Membership flag |

# %%
sp = pl.scan_parquet(DATA_DIR / "sp500_constituents.parquet")
n_rows_sp = sp.select(pl.len()).collect().item()
cols_sp = sp.collect_schema().names()
print(f"Rows: {n_rows_sp:,}  |  Columns: {len(cols_sp)}")
print(f"Column names: {cols_sp}")

# %% [markdown]
# ### Example rows

# %%
sp.head(5).collect()

# %% [markdown]
# ### Membership date range

# %%
sp.select(
    pl.col("mbrstartdt").min().alias("earliest_start"),
    pl.col("mbrstartdt").max().alias("latest_start"),
    pl.col("mbrenddt").min().alias("earliest_end"),
    pl.col("mbrenddt").max().alias("latest_end"),
).collect()

# %% [markdown]
# ### Unique constituents over full history

# %%
n_unique = sp.select(pl.col("permno").n_unique()).collect().item()
print(f"Distinct PERMNOs that have ever been in the S&P 500: {n_unique:,}")

# %% [markdown]
# ### Number of constituents on a sample date

# %%
import datetime

sample_date = datetime.date(2023, 6, 30)
n_on_date = (
    sp.filter(
        (pl.col("mbrstartdt") <= sample_date) & (pl.col("mbrenddt") > sample_date)
    )
    .select(pl.len())
    .collect()
    .item()
)
print(f"Constituents on {sample_date}: {n_on_date}")

# %% [markdown]
# ---
# ## 3. GDELT S&P 500 — `gdelt_sp500_headlines/`
#
# Source script: `pull_gdelt_sp500_headlines.py`
#
# Page-title headlines extracted from GDELT's Global Knowledge Graph 2.0,
# filtered server-side in BigQuery to articles mentioning S&P 500 companies.
# Data is stored as a monthly data lake (`gdelt_sp500_headlines/YYYY-MM.parquet`).
# Here we load the sample month (January 2025).
#
# **Key columns:**
#
# | Column | Description |
# |---|---|
# | `gkg_date` | GKG record timestamp |
# | `source_url` | Full URL of the source article |
# | `source_name` | Domain / common name of the news source |
# | `headline` | Page title extracted from `<PAGE_TITLE>` tags |
# | `matched_company` | S&P 500 company name matched via V2Organizations |
# | `permno` | CRSP permanent security identifier |
# | `ticker` | Stock ticker symbol |

# %%
from pull_gdelt_sp500_headlines import (
    SAMPLE_MONTH,
    filter_to_month,
    load_gdelt_sp500_headlines,
)

gd = filter_to_month(load_gdelt_sp500_headlines(), SAMPLE_MONTH)
n_rows_gd = gd.select(pl.len()).collect().item()
cols_gd = gd.collect_schema().names()
print(f"Rows: {n_rows_gd:,}  |  Columns: {len(cols_gd)}")
print(f"Column names: {cols_gd}")

# %% [markdown]
# ### Example rows

# %%
gd.head(5).collect()

# %% [markdown]
# ### Full headline examples by source

# %%
top_sources = (
    gd.group_by("source_name")
    .agg(pl.len().alias("n"))
    .sort("n", descending=True)
    .head(5)
    .collect()["source_name"]
    .to_list()
)
for src in top_sources:
    sample = (
        gd.filter(pl.col("source_name") == src)
        .select("headline")
        .head(3)
        .collect()["headline"]
        .to_list()
    )
    print(f"── {src} ──")
    for h in sample:
        print(f"  • {h}")
    print()

# %% [markdown]
# ### Date range

# %%
gd.select(
    pl.col("gkg_date").min().alias("earliest"),
    pl.col("gkg_date").max().alias("latest"),
).collect()

# %% [markdown]
# ### Summary statistics

# %%
gd_collected = gd.collect()
headline_lengths = gd_collected.select(
    pl.col("headline").str.len_chars().alias("headline_len")
)
print(f"Unique sources: {gd_collected['source_name'].n_unique():,}")
print(
    f"Headline length — mean: {headline_lengths['headline_len'].mean():.0f},  "
    f"median: {headline_lengths['headline_len'].median():.0f},  "
    f"max: {headline_lengths['headline_len'].max():,}"
)

# %% [markdown]
# ### Top 15 sources

# %%
(
    gd_collected.group_by("source_name")
    .agg(pl.len().alias("n"))
    .sort("n", descending=True)
    .head(15)
)

# %% [markdown]
# ### Headlines per day

# %%
gd_daily = (
    gd_collected.with_columns(pl.col("gkg_date").cast(pl.Date).alias("date"))
    .group_by("date")
    .agg(pl.len().alias("n_headlines"))
    .sort("date")
)

fig, ax = plt.subplots(figsize=(8, 3))
ax.bar(
    gd_daily["date"].to_list(),
    gd_daily["n_headlines"].to_list(),
    color="darkorange",
    alpha=0.8,
)
ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
ax.set_ylabel("Headlines")
ax.set_title(f"GDELT S&P 500 sample ({SAMPLE_MONTH}) — headlines per day")
fig.tight_layout()
plt.show()

# %% [markdown]
# ---
# ## 4. Free Newswires — `newswire_sp500_headlines/`
#
# Source script: `pull_free_newswires.py`
#
# Press release headlines scraped from PR Newswire via sitemap crawling,
# then filtered locally to S&P 500 companies using normalized company
# name substring matching. Data is stored as a Hive-partitioned data lake
# (`newswire_sp500_headlines/year=YYYY/month=MM/data.parquet`).
# Complements GDELT by covering the same licensed wire services that
# RavenPack draws from.
#
# **Key columns:**
#
# | Column | Description |
# |---|---|
# | `date` | Publication date (YYYY-MM-DD) |
# | `headline` | Press release headline text |
# | `source_url` | Full URL of the press release |
# | `source_name` | Wire service name (PR Newswire) |
# | `matched_company` | S&P 500 company name matched via substring |
# | `permno` | CRSP permanent security identifier |
# | `ticker` | Stock ticker symbol |

# %%
import re

from pull_free_newswires import load_newswire_headlines
from pull_sp500_constituents import load_sp500_names_lookup, normalize_company_name

raw = load_newswire_headlines().collect()

# Map Hive partition key to human-readable source name
if "source" in raw.columns and "source_name" not in raw.columns:
    source_map = {"prnewswire": "PR Newswire"}
    raw = raw.with_columns(
        pl.col("source")
        .replace_strict(source_map, default="Unknown")
        .alias("source_name")
    )

# Filter to S&P 500 companies via normalized name matching
lookup = pl.from_pandas(load_sp500_names_lookup())
names = sorted(
    [n for n in lookup["comnam_norm"].unique().to_list() if n], key=len, reverse=True
)
pattern = "(" + "|".join(re.escape(n) for n in names) + ")"

raw = raw.with_columns(
    pl.col("headline")
    .map_elements(normalize_company_name, return_dtype=pl.Utf8)
    .alias("headline_norm")
)
nw = raw.filter(pl.col("headline_norm").str.contains(pattern))
nw = nw.with_columns(
    pl.col("headline_norm").str.extract(pattern, group_index=1).alias("matched_norm")
)
lookup_dedup = lookup.select("comnam_norm", "comnam", "permno", "ticker").unique(
    subset=["comnam_norm"], keep="first"
)
nw = nw.join(
    lookup_dedup, left_on="matched_norm", right_on="comnam_norm", how="left"
).rename({"comnam": "matched_company"})
nw = nw.drop("headline_norm", "matched_norm").lazy()

n_rows_nw = nw.select(pl.len()).collect().item()
cols_nw = nw.collect_schema().names()
print(f"Rows: {n_rows_nw:,}  |  Columns: {len(cols_nw)}")
print(f"Column names: {cols_nw}")

# %% [markdown]
# ### Example rows

# %%
nw.head(5).collect()

# %% [markdown]
# ### Full headline examples

# %%
for h in nw.select("headline").head(8).collect()["headline"].to_list():
    print(f"  • {h}")

# %% [markdown]
# ### Headlines by wire service

# %%
nw_collected = nw.collect()
(
    nw_collected.group_by("source_name")
    .agg(pl.len().alias("n"))
    .sort("n", descending=True)
)

# %% [markdown]
# ### Top matched companies

# %%
(
    nw_collected.group_by("matched_company")
    .agg(pl.len().alias("n"))
    .sort("n", descending=True)
    .head(15)
)

# %% [markdown]
# ---
# ## 5. Newswire–RavenPack Crosswalk — `newswire_ravenpack_crosswalk.parquet`
#
# Source script: `create_newswire_ravenpack_crosswalk.py`
#
# Fuzzy-matched bridge between free newswire headlines and RavenPack DJ Press
# Release headlines. Each row is the single best RavenPack match for a given
# newswire headline on the same calendar date, kept only when the
# `token_sort_ratio` score is ≥ 80. The crosswalk enables enriching free
# newswire data with RavenPack's entity identifiers and sentiment scores.
#
# **Key columns:**
#
# | Column | Description |
# |---|---|
# | `date` | Calendar date of the matched headlines |
# | `nw_source_url` | URL of the newswire press release |
# | `nw_headline` | Original headline from the newswire source |
# | `nw_source` | Wire service identifier (e.g. `prnewswire`) |
# | `rp_story_id` | RavenPack unique story identifier |
# | `rp_entity_id` | RavenPack entity identifier |
# | `rp_entity_name` | Company name as recorded by RavenPack |
# | `rp_headline` | Headline text from RavenPack |
# | `rp_source_name` | News source as classified by RavenPack |
# | `fuzzy_score` | Token-sort-ratio similarity score (80–100) |

# %%
cw = pl.scan_parquet(DATA_DIR / "newswire_ravenpack_crosswalk.parquet")
n_rows_cw = cw.select(pl.len()).collect().item()
cols_cw = cw.collect_schema().names()
print(f"Rows: {n_rows_cw:,}  |  Columns: {len(cols_cw)}")
print(f"Column names: {cols_cw}")

# %% [markdown]
# ### Example rows

# %%
cw.head(5).collect()

# %% [markdown]
# ### Full headline pairs (newswire → RavenPack)

# %%
pairs = cw.select("nw_headline", "rp_headline", "fuzzy_score").head(6).collect()
for row in pairs.iter_rows(named=True):
    print(f"  NW:  {row['nw_headline']}")
    print(f"  RP:  {row['rp_headline']}")
    print(f"  score: {row['fuzzy_score']}")
    print()

# %% [markdown]
# ### Date range

# %%
cw.select(
    pl.col("date").min().alias("earliest"),
    pl.col("date").max().alias("latest"),
).collect()

# %% [markdown]
# ### Fuzzy-score distribution

# %%
cw_collected = cw.collect()
print(
    f"fuzzy_score — mean: {cw_collected['fuzzy_score'].mean():.1f},  "
    f"median: {cw_collected['fuzzy_score'].median():.1f},  "
    f"min: {cw_collected['fuzzy_score'].min():.1f},  "
    f"max: {cw_collected['fuzzy_score'].max():.1f}"
)

fig, ax = plt.subplots(figsize=(8, 3))
ax.hist(cw_collected["fuzzy_score"].to_list(), bins=40, color="mediumseagreen", alpha=0.8)
ax.set_xlabel("Fuzzy score")
ax.set_ylabel("Count")
ax.set_title("Newswire–RavenPack crosswalk — fuzzy-score distribution")
fig.tight_layout()
plt.show()

# %% [markdown]
# ### Matched headlines per month

# %%
cw_monthly = (
    cw_collected.with_columns(pl.col("date").dt.truncate("1mo").alias("month"))
    .group_by("month")
    .agg(pl.len().alias("n_matches"))
    .sort("month")
)

fig, ax = plt.subplots(figsize=(10, 3))
ax.bar(
    cw_monthly["month"].to_list(),
    cw_monthly["n_matches"].to_list(),
    width=25,
    color="mediumseagreen",
    alpha=0.8,
)
ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
fig.autofmt_xdate(rotation=45)
ax.set_ylabel("Matches")
ax.set_title("Newswire–RavenPack crosswalk — matched headlines per month")
fig.tight_layout()
plt.show()

# %%
