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
# # Fuzzy Matching: RavenPack vs GDELT
#
# Explore whether headlines from RavenPack and GDELT can be linked via
# fuzzy string matching. We use **rapidfuzz** with `token_sort_ratio`
# scoring, blocked by calendar date to keep the comparison space manageable.

# %% [markdown]
# ## 1. Imports & Config

# %%
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import polars as pl
from rapidfuzz import fuzz, process

from pull_sp500_constituents import normalize_company_name
from settings import config

DATA_DIR = Path(config("DATA_DIR"))

# %% [markdown]
# ## 2. Load Data & Inspect Date Ranges

# %%
from pull_gdelt_sp500_headlines import (
    SAMPLE_MONTH,
    filter_to_month,
    load_gdelt_sp500_headlines,
)

rp = pl.scan_parquet(DATA_DIR / "ravenpack_djpr.parquet")
gd = filter_to_month(load_gdelt_sp500_headlines(), SAMPLE_MONTH)

# %%
rp_range = rp.select(
    pl.col("timestamp_utc").cast(pl.Date).min().alias("earliest"),
    pl.col("timestamp_utc").cast(pl.Date).max().alias("latest"),
).collect()
print("RavenPack date range:")
print(rp_range)

# %%
gd_range = gd.select(
    pl.col("gkg_date").cast(pl.Date).min().alias("earliest"),
    pl.col("gkg_date").cast(pl.Date).max().alias("latest"),
).collect()
print("GDELT date range:")
print(gd_range)

# %% [markdown]
# ### Compute overlap window

# %%
rp_earliest = rp_range["earliest"].item()
rp_latest = rp_range["latest"].item()
gd_earliest = gd_range["earliest"].item()
gd_latest = gd_range["latest"].item()

overlap_start = max(rp_earliest, gd_earliest)
overlap_end = min(rp_latest, gd_latest)
print(f"Overlap window: {overlap_start} to {overlap_end}")

# %% [markdown]
# ### Subset both datasets to the overlap period

# %%
rp_overlap = (
    rp.with_columns(pl.col("timestamp_utc").cast(pl.Date).alias("date"))
    .filter((pl.col("date") >= overlap_start) & (pl.col("date") <= overlap_end))
    .select("date", "headline", "entity_name")
    .collect()
)
print(f"RavenPack headlines in overlap: {len(rp_overlap):,}")

# %%
gd_overlap = (
    gd.with_columns(pl.col("gkg_date").cast(pl.Date).alias("date"))
    .filter((pl.col("date") >= overlap_start) & (pl.col("date") <= overlap_end))
    .select("date", "headline", "permno", "ticker")
    .collect()
)
print(f"GDELT headlines in overlap: {len(gd_overlap):,}")

# %% [markdown]
# ### Match RavenPack entities to S&P 500 members
#
# GDELT headlines already carry `permno` and `ticker` from the
# server-side JOIN. RavenPack only has `entity_name`, so we normalize
# it and match against the S&P 500 names lookup table.

# %%
sp500_lookup = pl.from_pandas(pd.read_parquet(DATA_DIR / "sp500_names_lookup.parquet"))

rp_overlap = rp_overlap.with_columns(
    pl.col("entity_name")
    .map_elements(normalize_company_name, return_dtype=pl.Utf8)
    .alias("entity_name_norm")
)

# Join to get permno/ticker for RP headlines that match an S&P 500 member
rp_overlap = rp_overlap.join(
    sp500_lookup.select("comnam_norm", "permno", "ticker").unique(),
    left_on="entity_name_norm",
    right_on="comnam_norm",
    how="left",
)

n_rp_matched = rp_overlap.filter(pl.col("permno").is_not_null()).height
print(
    f"RavenPack headlines matched to S&P 500: {n_rp_matched:,} / {len(rp_overlap):,} ({n_rp_matched / len(rp_overlap) * 100:.1f}%)"
)
print(
    f"Distinct S&P 500 members in RavenPack:  {rp_overlap.filter(pl.col('permno').is_not_null())['permno'].n_unique()}"
)
print(f"Distinct S&P 500 members in GDELT:      {gd_overlap['permno'].n_unique()}")

# %% [markdown]
# ### Limit to a manageable sample
#
# The S&P 500–filtered GDELT sample is much smaller than the raw
# firehose, but we still limit to **2 days** and cap GDELT at 5,000
# headlines per day to keep `cdist` fast in this exploratory notebook.

# %%
from datetime import timedelta

SAMPLE_DAYS = 2
GDELT_CAP_PER_DAY = 5_000

sample_end = overlap_start + timedelta(days=SAMPLE_DAYS)

rp_sample = rp_overlap.filter(
    (pl.col("date") >= overlap_start) & (pl.col("date") < sample_end)
)
gd_sample = gd_overlap.filter(
    (pl.col("date") >= overlap_start) & (pl.col("date") < sample_end)
)

# Cap GDELT per day via random sampling (shuffle then take head per group)
gd_sample = (
    gd_sample.sample(fraction=1.0, shuffle=True, seed=42)
    .group_by("date")
    .head(GDELT_CAP_PER_DAY)
)

print(f"Sample period: {rp_sample['date'].min()} to {rp_sample['date'].max()}")
print(f"RavenPack sample: {len(rp_sample):,} headlines")
print(
    f"GDELT sample:     {len(gd_sample):,} headlines (capped at {GDELT_CAP_PER_DAY}/day)"
)

# %% [markdown]
# ## 3. Normalize Headlines


# %%
def normalize_headline_col(df: pl.DataFrame, col: str = "headline") -> pl.DataFrame:
    """Lowercase, strip whitespace, remove punctuation (keep apostrophes), collapse spaces."""
    return df.with_columns(
        pl.col(col)
        .str.to_lowercase()
        .str.strip_chars()
        .str.replace_all(r"[^\w\s']", "")
        .str.replace_all(r"\s+", " ")
        .alias("headline_norm")
    )


rp_sample = normalize_headline_col(rp_sample)
gd_sample = normalize_headline_col(gd_sample)

# %% [markdown]
# ### Before / after examples

# %%
print("RavenPack — before vs after normalization:")
rp_sample.select("headline", "headline_norm").head(5)

# %%
print("GDELT — before vs after normalization:")
gd_sample.select("headline", "headline_norm").head(5)

# %% [markdown]
# ## 4. Fuzzy Matching (blocked by date)
#
# For each calendar date, we compute the pairwise fuzzy similarity between
# all RavenPack and GDELT headlines using `rapidfuzz.process.cdist` with
# `token_sort_ratio`. For each RavenPack headline we keep the best GDELT match.

# %%
dates_in_common = sorted(
    set(rp_sample["date"].unique().to_list())
    & set(gd_sample["date"].unique().to_list())
)
print(f"Dates with headlines in both sources: {len(dates_in_common)}")

# %%
crosswalk_rows = []

for d in dates_in_common:
    rp_day_df = rp_sample.filter(pl.col("date") == d)
    gd_day_df = gd_sample.filter(pl.col("date") == d)

    rp_day = rp_day_df["headline_norm"].to_list()
    gd_day = gd_day_df["headline_norm"].to_list()

    rp_permnos = rp_day_df["permno"].to_list()
    gd_permnos = gd_day_df["permno"].to_list()
    gd_tickers = gd_day_df["ticker"].to_list()

    if not rp_day or not gd_day:
        continue

    # cdist returns a matrix of shape (len(rp_day), len(gd_day))
    scores = process.cdist(rp_day, gd_day, scorer=fuzz.token_sort_ratio, workers=-1)

    # For each RP headline, find the best GDELT match
    best_idx = np.argmax(scores, axis=1)
    best_scores = scores[np.arange(len(rp_day)), best_idx]

    for i, (rp_hl, gd_idx, score) in enumerate(zip(rp_day, best_idx, best_scores)):
        crosswalk_rows.append(
            {
                "date": d,
                "rp_headline": rp_hl,
                "gd_headline": gd_day[gd_idx],
                "fuzzy_score": float(score),
                "rp_permno": rp_permnos[i],
                "gd_permno": gd_permnos[gd_idx],
                "gd_ticker": gd_tickers[gd_idx],
            }
        )

crosswalk = pl.DataFrame(crosswalk_rows)
print(f"Crosswalk rows: {len(crosswalk):,}")
crosswalk.head(5)

# %% [markdown]
# ## 5. Score Distribution & Threshold Analysis

# %%
scores_np = crosswalk["fuzzy_score"].to_numpy()

fig, ax = plt.subplots(figsize=(10, 4))
ax.hist(scores_np, bins=50, color="steelblue", edgecolor="white", alpha=0.8)
ax.axvline(80, color="green", linestyle="--", linewidth=1.5, label="Good (>=80)")
ax.axvline(
    60, color="orange", linestyle="--", linewidth=1.5, label="Borderline (60-79)"
)
ax.set_xlabel("Fuzzy Score")
ax.set_ylabel("Count")
ax.set_title("Distribution of Best-Match Fuzzy Scores (RavenPack → GDELT)")
ax.legend()
fig.tight_layout()
plt.show()

# %%
n_total = len(crosswalk)
n_good = crosswalk.filter(pl.col("fuzzy_score") >= 80).height
n_border = crosswalk.filter(
    (pl.col("fuzzy_score") >= 60) & (pl.col("fuzzy_score") < 80)
).height
n_non = crosswalk.filter(pl.col("fuzzy_score") < 60).height

print(f"{'Tier':<15} {'Count':>8} {'Pct':>8}")
print("-" * 33)
print(f"{'Good (>=80)':<15} {n_good:>8,} {n_good / n_total * 100:>7.1f}%")
print(f"{'Borderline':<15} {n_border:>8,} {n_border / n_total * 100:>7.1f}%")
print(f"{'Non-match':<15} {n_non:>8,} {n_non / n_total * 100:>7.1f}%")
print("-" * 33)
print(f"{'Total':<15} {n_total:>8,}")

# %% [markdown]
# ## 6. S&P 500 Member Coverage
#
# Beyond headline-level match rates, we want to know: how many S&P 500
# **members** are covered by each source, and how many are linked by the
# fuzzy crosswalk?

# %%
# Distinct S&P 500 members in each source (within the sample window)
rp_sp500 = set(
    rp_sample.filter(pl.col("permno").is_not_null())["permno"].unique().to_list()
)
gd_sp500 = set(gd_sample["permno"].unique().to_list())

# S&P 500 members linked via good fuzzy matches
good_crosswalk = crosswalk.filter(pl.col("fuzzy_score") >= 80)
matched_rp_permnos = set(
    good_crosswalk.filter(pl.col("rp_permno").is_not_null())["rp_permno"]
    .unique()
    .to_list()
)
matched_gd_permnos = set(good_crosswalk["gd_permno"].unique().to_list())
linked_permnos = matched_rp_permnos & matched_gd_permnos

print(f"{'Source':<35} {'Members':>8}")
print("-" * 45)
print(f"{'RavenPack (S&P 500 matched)':<35} {len(rp_sp500):>8}")
print(f"{'GDELT (S&P 500 filtered)':<35} {len(gd_sp500):>8}")
print(f"{'In both sources (union)':<35} {len(rp_sp500 | gd_sp500):>8}")
print(f"{'In both sources (intersection)':<35} {len(rp_sp500 & gd_sp500):>8}")
print(f"{'Linked via good fuzzy match':<35} {len(linked_permnos):>8}")

# %%
fig, ax = plt.subplots(figsize=(8, 4))

labels = [
    "RavenPack\n(S&P 500)",
    "GDELT\n(S&P 500)",
    "Both\n(intersect)",
    "Fuzzy-linked\n(score≥80)",
]
counts = [len(rp_sp500), len(gd_sp500), len(rp_sp500 & gd_sp500), len(linked_permnos)]
colors = ["darkorange", "steelblue", "mediumpurple", "green"]

bars = ax.bar(labels, counts, color=colors, edgecolor="white", alpha=0.85)
for bar, count in zip(bars, counts):
    ax.text(
        bar.get_x() + bar.get_width() / 2,
        bar.get_height() + 1,
        str(count),
        ha="center",
        va="bottom",
        fontweight="bold",
    )

ax.set_ylabel("Distinct S&P 500 members (permno)")
ax.set_title("S&P 500 Member Coverage by Source")
fig.tight_layout()
plt.show()

# %% [markdown]
# ### Which members appear in one source but not the other?

# %%
rp_only = rp_sp500 - gd_sp500
gd_only = gd_sp500 - rp_sp500

# Show a sample of members unique to each source
rp_only_details = (
    rp_sample.filter(pl.col("permno").is_in(list(rp_only)))
    .group_by("permno", "entity_name")
    .agg(pl.len().alias("n"))
    .sort("n", descending=True)
)

gd_only_details = (
    gd_sample.filter(pl.col("permno").is_in(list(gd_only)))
    .group_by("permno", "ticker")
    .agg(pl.len().alias("n"))
    .sort("n", descending=True)
)

print(f"S&P 500 members in RavenPack only: {len(rp_only)}")
if rp_only_details.height > 0:
    print("  Top examples:")
    for row in rp_only_details.head(10).iter_rows(named=True):
        print(
            f"    permno={row['permno']}  {row['entity_name']:<30s}  ({row['n']} headlines)"
        )

print(f"\nS&P 500 members in GDELT only: {len(gd_only)}")
if gd_only_details.height > 0:
    print("  Top examples:")
    for row in gd_only_details.head(10).iter_rows(named=True):
        ticker = row["ticker"] if row["ticker"] else "N/A"
        print(f"    permno={row['permno']}  {ticker:<8s}  ({row['n']} headlines)")

# %% [markdown]
# ## 7. Build Merge Keys & Crosswalk
#
# Assign each headline a row ID within its source dataset. The crosswalk
# maps `(date, rp_row_id)` → `(date, gd_row_id, score, tier)`.

# %%
crosswalk = crosswalk.with_columns(
    pl.when(pl.col("fuzzy_score") >= 80)
    .then(pl.lit("good"))
    .when(pl.col("fuzzy_score") >= 60)
    .then(pl.lit("borderline"))
    .otherwise(pl.lit("non-match"))
    .alias("tier")
)

# Add row IDs
crosswalk = crosswalk.with_row_index("rp_row_id")
crosswalk.head(10)

# %% [markdown]
# ## 8. Headline Overlap Analysis
#
# For each RavenPack headline, the crosswalk already contains the best
# GDELT match and its fuzzy score (from Section 4). Now we ask two
# questions:
#
# 1. **Coverage:** What fraction of RavenPack headlines have a usable
#    GDELT counterpart (i.e., a "good" match with score >= 80)?
# 2. **Diversity:** Is the matching spreading across many distinct GDELT
#    articles, or are multiple RavenPack headlines collapsing onto the
#    same GDELT article?

# %% [markdown]
# ### Overall match rates

# %%
n_total = len(crosswalk)
tier_counts = (
    crosswalk.group_by("tier").agg(pl.len().alias("n")).sort("n", descending=True)
)

for row in tier_counts.iter_rows(named=True):
    pct = row["n"] / n_total * 100
    print(f"  {row['tier']:<12s}  {row['n']:>6,}  ({pct:5.1f}%)")

n_good = crosswalk.filter(pl.col("tier") == "good").height
n_gd_distinct = crosswalk.filter(pl.col("tier") == "good")["gd_headline"].n_unique()

print(
    f"\n{n_good:,} of {n_total:,} RavenPack headlines ({n_good / n_total * 100:.1f}%) "
    f"found a good GDELT match (score >= 80)."
)
print(f"These map to {n_gd_distinct:,} distinct GDELT headlines.")

# %% [markdown]
# **How to read this:** The "good" rate tells you how much of
# RavenPack's headline content could plausibly be recovered from GDELT
# alone. Headlines in the "non-match" tier have no usable GDELT link --
# they represent content that is unique to RavenPack (or at least not
# findable via fuzzy matching within the same day).

# %% [markdown]
# ### GDELT reuse: one-to-one or many-to-one?
#
# Each RavenPack headline maps to exactly one best GDELT match, but
# multiple RavenPack headlines can map to the **same** GDELT article.
# High reuse means GDELT has fewer unique articles than RavenPack for
# the matched firms/dates -- the crosswalk is "compressing" information.

# %%
good_matches = crosswalk.filter(pl.col("tier") == "good")
gd_reuse = (
    good_matches.group_by("gd_headline")
    .agg(pl.len().alias("n_rp_matched"))
    .group_by("n_rp_matched")
    .agg(pl.len().alias("n_gd_headlines"))
    .sort("n_rp_matched")
)

print("GDELT headline reuse (among good matches):")
print(f"  {'RP headlines matched':<25s} {'GDELT headlines':>16s}")
print("  " + "-" * 43)
for row in gd_reuse.iter_rows(named=True):
    label = str(row["n_rp_matched"]) if row["n_rp_matched"] < 5 else "5+"
    print(f"  {label:<25s} {row['n_gd_headlines']:>16,}")

n_reused = gd_reuse.filter(pl.col("n_rp_matched") > 1)["n_gd_headlines"].sum()
n_unique = gd_reuse.filter(pl.col("n_rp_matched") == 1)["n_gd_headlines"].sum()
if n_reused is None:
    n_reused = 0
if n_unique is None:
    n_unique = 0
print(f"\n{n_unique:,} GDELT headlines matched exactly 1 RP headline (one-to-one).")
print(f"{n_reused:,} GDELT headlines matched 2+ RP headlines (many-to-one).")

# %% [markdown]
# **What this means:** If most matches are one-to-one, the crosswalk
# preserves information well -- each RP headline links to a distinct
# GDELT article. If many GDELT headlines are reused, it suggests GDELT
# has fewer unique articles than RavenPack for these firms and dates,
# and the fuzzy matcher is "recycling" the same GDELT text as the
# nearest neighbor for multiple RP headlines.

# %% [markdown]
# ## 9. Match Examples
#
# ### Top 10 good matches (highest scores)

# %%
(
    crosswalk.filter(pl.col("tier") == "good")
    .sort("fuzzy_score", descending=True)
    .head(10)
    .select("date", "rp_headline", "gd_headline", "fuzzy_score")
)

# %% [markdown]
# ### Top 10 borderline matches

# %%
(
    crosswalk.filter(pl.col("tier") == "borderline")
    .sort("fuzzy_score", descending=True)
    .head(10)
    .select("date", "rp_headline", "gd_headline", "fuzzy_score")
)

# %% [markdown]
# ### Top 10 non-matches (highest-scoring non-matches — most instructive)

# %%
(
    crosswalk.filter(pl.col("tier") == "non-match")
    .sort("fuzzy_score", descending=True)
    .head(10)
    .select("date", "rp_headline", "gd_headline", "fuzzy_score")
)

# %% [markdown]
# ## 10. Summary & Next Steps
#
# **Key findings from this exploratory fuzzy match:**
#
# - The date-blocked approach limits the comparison space to within-day pairs,
#   making fuzzy matching tractable even on large datasets.
# - `token_sort_ratio` handles word-order variation between sources.
# - The score distribution and tier breakdown show what fraction of RavenPack
#   headlines find plausible GDELT counterparts.
# - The headline overlap analysis (Section 8) quantifies match coverage and
#   checks whether the crosswalk maps one-to-one or many-to-one — i.e.,
#   whether GDELT provides distinct articles or recycles the same text.
# - The S&P 500 member coverage analysis (Section 6) shows how many distinct
#   index constituents appear in each source and how many are linked by the
#   fuzzy crosswalk — giving a firm-level view beyond headline counts.
#
# **Next steps:**
#
# 1. Expand the sample window to the full overlap period (or a larger slice)
#    once matching quality is validated.
# 2. Tune the score threshold — inspect borderline matches to calibrate.
# 3. Consider additional blocking keys (e.g., source domain) to improve
#    precision and reduce false matches.
# 4. Use the crosswalk to merge sentiment/metadata across sources.

# %%
