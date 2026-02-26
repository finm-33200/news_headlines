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
# # GDELT Data Lake Analysis
#
# Explore the full GDELT S&P 500 data lake stored in
# `gdelt_sp500_headlines/year=YYYY/month=MM/data.parquet` (Hive-partitioned).
# This notebook scans all available monthly parquet files and summarises
# temporal coverage, company distribution, source diversity, and data quality.

# %% [markdown]
# ## 1. Load the Data Lake

# %%
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import polars as pl

from pull_gdelt_sp500_headlines import GDELT_SP500_DIR, load_gdelt_sp500_headlines
from settings import config

DATA_DIR = Path(config("DATA_DIR"))

# %%
lf = load_gdelt_sp500_headlines()
df = lf.collect()
print(f"Total rows in data lake: {len(df):,}")
print(f"Columns: {df.columns}")

# %% [markdown]
# ## 2. Temporal Coverage — Monthly Headline Counts

# %%
monthly = (
    df.with_columns(pl.col("gkg_date").cast(pl.Date).dt.truncate("1mo").alias("month"))
    .group_by("month")
    .agg(pl.len().alias("n_headlines"))
    .sort("month")
)

fig, ax = plt.subplots(figsize=(12, 4))
ax.bar(
    monthly["month"].to_list(),
    monthly["n_headlines"].to_list(),
    width=25,
    color="steelblue",
    alpha=0.8,
)
ax.xaxis.set_major_locator(mdates.YearLocator())
ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
ax.set_ylabel("Headlines")
ax.set_title("GDELT S&P 500 — headlines per month (data lake)")
fig.tight_layout()
plt.show()

print(f"Months in data lake: {monthly.height}")
print(f"Date range: {monthly['month'].min()} to {monthly['month'].max()}")

# %% [markdown]
# ## 3. Company Distribution Across Years

# %%
yearly_companies = (
    df.with_columns(pl.col("gkg_date").dt.year().alias("year"))
    .group_by("year")
    .agg(pl.col("permno").n_unique().alias("n_companies"))
    .sort("year")
)

fig, ax = plt.subplots(figsize=(10, 4))
ax.bar(
    yearly_companies["year"].to_list(),
    yearly_companies["n_companies"].to_list(),
    color="teal",
    alpha=0.8,
)
ax.set_xlabel("Year")
ax.set_ylabel("Distinct PERMNOs")
ax.set_title("Distinct S&P 500 companies per year in GDELT data lake")
fig.tight_layout()
plt.show()

# %% [markdown]
# ## 4. Top Companies by Total Coverage

# %%
top_companies = (
    df.group_by("matched_company", "ticker", "permno")
    .agg(pl.len().alias("n_headlines"))
    .sort("n_headlines", descending=True)
)

print("Top 25 companies by total headline count:\n")
for row in top_companies.head(25).iter_rows(named=True):
    ticker = row["ticker"] if row["ticker"] else "N/A"
    print(f"  {row['n_headlines']:>8,}  {ticker:<8s}  {row['matched_company']}")

# %% [markdown]
# ## 5. Source Distribution

# %%
top_sources = (
    df.group_by("source_name").agg(pl.len().alias("n")).sort("n", descending=True)
)

print("Top 20 sources across all months:\n")
for row in top_sources.head(20).iter_rows(named=True):
    print(f"  {row['n']:>8,}  {row['source_name']}")

print(f"\nTotal unique sources: {top_sources.height:,}")

# %% [markdown]
# ## 6. Data Quality — File Sizes and Row Counts

# %%
parquet_dir = Path(GDELT_SP500_DIR)
files = sorted(parquet_dir.glob("year=*/month=*/data.parquet"))

file_stats = []
for f in files:
    size_mb = f.stat().st_size / 1e6
    # Extract year-month label from Hive directory structure
    label = f"{f.parent.parent.name.split('=')[1]}-{f.parent.name.split('=')[1]}"
    file_stats.append({"file": label, "size_mb": size_mb})

file_stats_df = pl.DataFrame(file_stats)

# Merge with monthly row counts
monthly_with_name = monthly.with_columns(
    pl.col("month").cast(pl.Utf8).str.slice(0, 7).alias("file")
)
quality = file_stats_df.join(
    monthly_with_name.select("file", "n_headlines"), on="file", how="left"
)

print(f"{'Month':<12s} {'Size (MB)':>10s} {'Rows':>12s}")
print("-" * 36)
for row in quality.iter_rows(named=True):
    n = row["n_headlines"] if row["n_headlines"] is not None else 0
    print(f"  {row['file']:<10s} {row['size_mb']:>8.1f}  {n:>10,}")

print(f"\nTotal files: {len(files)}")
print(f"Total size:  {file_stats_df['size_mb'].sum():.1f} MB")

# %% [markdown]
# ## 7. Daily Volume Trends

# %%
daily = (
    df.with_columns(pl.col("gkg_date").cast(pl.Date).alias("date"))
    .group_by("date")
    .agg(pl.len().alias("n_headlines"))
    .sort("date")
)

fig, ax = plt.subplots(figsize=(14, 4))
ax.plot(
    daily["date"].to_list(),
    daily["n_headlines"].to_list(),
    linewidth=0.5,
    color="steelblue",
    alpha=0.7,
)
ax.xaxis.set_major_locator(mdates.YearLocator())
ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
ax.set_ylabel("Headlines per day")
ax.set_title("GDELT S&P 500 — daily headline volume")
fig.tight_layout()
plt.show()

mean_daily = daily["n_headlines"].mean()
median_daily = daily["n_headlines"].median()
print(f"Daily volume — mean: {mean_daily:,.0f}, median: {median_daily:,.0f}")

# %%
