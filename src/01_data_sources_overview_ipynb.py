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
# Two sources are currently collected:
#
# 1. **RavenPack** Dow Jones Press Release headlines (via WRDS)
# 2. **GDELT** Global Knowledge Graph headlines (via BigQuery)

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
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

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
# ---
# ## 2. GDELT — `gdelt_gkg_headlines_sample.parquet`
#
# Source script: `pull_gdelt_small_sample.py`
#
# Page-title headlines extracted from GDELT's Global Knowledge Graph 2.0,
# pulled from Google BigQuery. This is a small exploratory sample
# (one week, Jan 8–15 2024).
#
# **Key columns:**
#
# | Column | Description |
# |---|---|
# | `gkg_date` | GKG record timestamp |
# | `source_url` | Full URL of the source article |
# | `source_name` | Domain / common name of the news source |
# | `headline` | Page title extracted from `<PAGE_TITLE>` tags |

# %%
gd = pl.scan_parquet(DATA_DIR / "gdelt_gkg_headlines_sample.parquet")
n_rows_gd = gd.select(pl.len()).collect().item()
cols_gd = gd.collect_schema().names()
print(f"Rows: {n_rows_gd:,}  |  Columns: {len(cols_gd)}")
print(f"Column names: {cols_gd}")

# %% [markdown]
# ### Example rows

# %%
gd.filter(pl.col("gkg_date").dt.year() == 2025).head(5).collect()

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
print(f"Headline length — mean: {headline_lengths['headline_len'].mean():.0f},  "
      f"median: {headline_lengths['headline_len'].median():.0f},  "
      f"max: {headline_lengths['headline_len'].max():,}")

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
ax.set_title("GDELT GKG sample — headlines per day")
fig.tight_layout()
plt.show()

# %%
