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
# # Newswire Data Exploration & Fuzzy Matching
#
# Explore raw press-release headlines scraped from PR Newswire, Business Wire,
# and GlobeNewswire. Then fuzzy-match these headlines against GDELT and
# RavenPack to measure overlap. Matching is purely headline-text-based,
# blocked by calendar date.

# %% [markdown]
# ## 1. Imports & Config

# %%
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import polars as pl
from rapidfuzz import fuzz, process

from settings import config

DATA_DIR = Path(config("DATA_DIR"))

# %% [markdown]
# ## 2. Load Newswire Data

# %%
from pull_free_newswires import load_newswire_headlines
from pull_gdelt_sp500_headlines import (
    SAMPLE_MONTH,
    filter_to_month,
    load_gdelt_sp500_headlines,
)

nw = filter_to_month(load_newswire_headlines(), SAMPLE_MONTH).collect()
print(f"Total newswire headlines ({SAMPLE_MONTH}): {len(nw):,}")
print(f"Columns: {nw.columns}")

# %%
source_counts = (
    nw.group_by("source")
    .agg(pl.len().alias("n"))
    .sort("n", descending=True)
)
print("Headlines by source:")
source_counts

# %%
nw = nw.with_columns(pl.col("date").cast(pl.Date).alias("pub_date"))
print(f"Date range: {nw['pub_date'].min()} to {nw['pub_date'].max()}")
print(f"Distinct dates: {nw['pub_date'].n_unique()}")

# %% [markdown]
# ### Example headlines by source

# %%
for src in sorted(nw["source"].unique().to_list()):
    subset = nw.filter(pl.col("source") == src)
    print(f"\n{'=' * 70}")
    print(f"Source: {src}  ({len(subset):,} headlines)")
    print("=" * 70)
    for row in subset.head(5).iter_rows(named=True):
        print(f"  [{row['date']}] {row['headline'][:100]}")

# %% [markdown]
# ## 3. Load GDELT & RavenPack
#
# Filter both to the sample month for exploratory matching.

# %%
gd = filter_to_month(load_gdelt_sp500_headlines(), SAMPLE_MONTH).collect()
gd = gd.with_columns(pl.col("gkg_date").cast(pl.Date).alias("date"))
print(f"GDELT headlines ({SAMPLE_MONTH}): {len(gd):,}")

# %%
rp = pl.scan_parquet(DATA_DIR / "ravenpack_djpr.parquet")

nw_start = nw["pub_date"].min()
nw_end = nw["pub_date"].max()

rp = (
    rp.with_columns(pl.col("timestamp_utc").cast(pl.Date).alias("date"))
    .filter((pl.col("date") >= nw_start) & (pl.col("date") <= nw_end))
    .select("date", "headline", "entity_name", "source_name")
    .collect()
)
print(f"RavenPack headlines in newswire window ({nw_start} to {nw_end}): {len(rp):,}")

# %%
nw_date_set = set(nw["pub_date"].unique().to_list())
gd_date_set = set(gd["date"].unique().to_list())
rp_date_set = set(rp["date"].unique().to_list())

nw_gd_dates = sorted(nw_date_set & gd_date_set)
nw_rp_dates = sorted(nw_date_set & rp_date_set)

print(f"Newswire dates:             {len(nw_date_set)}")
print(f"GDELT dates:                {len(gd_date_set)}")
print(f"RavenPack dates:            {len(rp_date_set)}")
print(f"Newswire-GDELT overlap:     {len(nw_gd_dates)} days")
print(f"Newswire-RavenPack overlap: {len(nw_rp_dates)} days")

# %% [markdown]
# ## 4. Normalize Headlines

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


nw_norm = normalize_headline_col(nw)
gd_norm = normalize_headline_col(gd)
rp_norm = normalize_headline_col(rp)

# %%
print("Newswire — before vs after:")
nw_norm.select("headline", "headline_norm").head(3)

# %%
print("GDELT — before vs after:")
gd_norm.select("headline", "headline_norm").head(3)

# %%
print("RavenPack — before vs after:")
rp_norm.select("headline", "headline_norm").head(3)

# %% [markdown]
# ## 5. Fuzzy Match: Newswire vs GDELT
#
# For each calendar date in common, compute pairwise fuzzy similarity
# between newswire and GDELT headlines using `rapidfuzz.process.cdist`
# with `token_sort_ratio`. For each newswire headline, keep the best
# GDELT match.

# %%
GDELT_CAP_PER_DAY = 5_000

nw_match = nw_norm  # has "pub_date" as pl.Date
gd_match = gd_norm  # has "date" as pl.Date

gd_crosswalk_rows = []

for d in nw_gd_dates:
    nw_day = nw_match.filter(pl.col("pub_date") == d)
    gd_day = gd_match.filter(pl.col("date") == d)

    if len(gd_day) > GDELT_CAP_PER_DAY:
        gd_day = gd_day.sample(n=GDELT_CAP_PER_DAY, seed=42)

    nw_hl = nw_day["headline_norm"].to_list()
    gd_hl = gd_day["headline_norm"].to_list()
    nw_sources = nw_day["source"].to_list()
    nw_headlines_raw = nw_day["headline"].to_list()
    gd_headlines_raw = gd_day["headline"].to_list()

    if not nw_hl or not gd_hl:
        continue

    scores = process.cdist(nw_hl, gd_hl, scorer=fuzz.token_sort_ratio, workers=-1)
    best_idx = np.argmax(scores, axis=1)
    best_scores = scores[np.arange(len(nw_hl)), best_idx]

    for i, (gd_i, score) in enumerate(zip(best_idx, best_scores)):
        gd_crosswalk_rows.append(
            {
                "date": d,
                "nw_source": nw_sources[i],
                "nw_headline": nw_headlines_raw[i],
                "gd_headline": gd_headlines_raw[gd_i],
                "fuzzy_score": float(score),
            }
        )

gd_crosswalk = pl.DataFrame(gd_crosswalk_rows)
print(f"Newswire-GDELT crosswalk rows: {len(gd_crosswalk):,}")

# %% [markdown]
# ## 6. Fuzzy Match: Newswire vs RavenPack
#
# Same approach, matching newswire against RavenPack headlines.

# %%
RP_CAP_PER_DAY = 5_000

rp_crosswalk_rows = []

for d in nw_rp_dates:
    nw_day = nw_match.filter(pl.col("pub_date") == d)
    rp_day = rp_norm.filter(pl.col("date") == d)

    if len(rp_day) > RP_CAP_PER_DAY:
        rp_day = rp_day.sample(n=RP_CAP_PER_DAY, seed=42)

    nw_hl = nw_day["headline_norm"].to_list()
    rp_hl = rp_day["headline_norm"].to_list()
    nw_sources = nw_day["source"].to_list()
    nw_headlines_raw = nw_day["headline"].to_list()
    rp_headlines_raw = rp_day["headline"].to_list()

    if not nw_hl or not rp_hl:
        continue

    scores = process.cdist(nw_hl, rp_hl, scorer=fuzz.token_sort_ratio, workers=-1)
    best_idx = np.argmax(scores, axis=1)
    best_scores = scores[np.arange(len(nw_hl)), best_idx]

    for i, (rp_i, score) in enumerate(zip(best_idx, best_scores)):
        rp_crosswalk_rows.append(
            {
                "date": d,
                "nw_source": nw_sources[i],
                "nw_headline": nw_headlines_raw[i],
                "rp_headline": rp_headlines_raw[rp_i],
                "fuzzy_score": float(score),
            }
        )

rp_crosswalk = pl.DataFrame(rp_crosswalk_rows)
print(f"Newswire-RavenPack crosswalk rows: {len(rp_crosswalk):,}")

# %% [markdown]
# ## 7. Score Distributions

# %%
fig, axes = plt.subplots(1, 2, figsize=(12, 4), sharey=True)

for ax, cw, title in [
    (axes[0], gd_crosswalk, "Newswire vs GDELT"),
    (axes[1], rp_crosswalk, "Newswire vs RavenPack"),
]:
    if len(cw) == 0:
        ax.set_title(f"{title}\n(no matches)")
        continue
    scores_np = cw["fuzzy_score"].to_numpy()
    ax.hist(scores_np, bins=50, color="steelblue", edgecolor="white", alpha=0.8)
    ax.axvline(80, color="green", linestyle="--", linewidth=1.5, label="Good (>=80)")
    ax.axvline(
        60, color="orange", linestyle="--", linewidth=1.5, label="Borderline (60-79)"
    )
    ax.set_xlabel("Fuzzy Score")
    ax.set_title(title)
    ax.legend(fontsize=8)

axes[0].set_ylabel("Count")
fig.suptitle("Best-Match Fuzzy Score Distributions", fontweight="bold")
fig.tight_layout()
plt.show()

# %%
def tier_summary(crosswalk: pl.DataFrame, label: str) -> None:
    n_total = len(crosswalk)
    if n_total == 0:
        print(f"{label}: no matches to summarize")
        return
    n_good = crosswalk.filter(pl.col("fuzzy_score") >= 80).height
    n_border = crosswalk.filter(
        (pl.col("fuzzy_score") >= 60) & (pl.col("fuzzy_score") < 80)
    ).height
    n_non = crosswalk.filter(pl.col("fuzzy_score") < 60).height

    print(f"\n{label}")
    print(f"{'Tier':<15} {'Count':>8} {'Pct':>8}")
    print("-" * 33)
    print(f"{'Good (>=80)':<15} {n_good:>8,} {n_good / n_total * 100:>7.1f}%")
    print(f"{'Borderline':<15} {n_border:>8,} {n_border / n_total * 100:>7.1f}%")
    print(f"{'Non-match':<15} {n_non:>8,} {n_non / n_total * 100:>7.1f}%")
    print("-" * 33)
    print(f"{'Total':<15} {n_total:>8,}")


tier_summary(gd_crosswalk, "Newswire vs GDELT")
tier_summary(rp_crosswalk, "Newswire vs RavenPack")

# %% [markdown]
# ## 8. Match Examples
#
# ### Newswire vs GDELT

# %%
print("Top 10 GOOD matches (Newswire vs GDELT):")
(
    gd_crosswalk.filter(pl.col("fuzzy_score") >= 80)
    .sort("fuzzy_score", descending=True)
    .head(10)
    .select("date", "nw_headline", "gd_headline", "fuzzy_score")
)

# %%
print("Top 10 BORDERLINE matches (Newswire vs GDELT):")
(
    gd_crosswalk.filter(
        (pl.col("fuzzy_score") >= 60) & (pl.col("fuzzy_score") < 80)
    )
    .sort("fuzzy_score", descending=True)
    .head(10)
    .select("date", "nw_headline", "gd_headline", "fuzzy_score")
)

# %%
print("Top 10 NON-MATCHES — highest-scoring (Newswire vs GDELT):")
(
    gd_crosswalk.filter(pl.col("fuzzy_score") < 60)
    .sort("fuzzy_score", descending=True)
    .head(10)
    .select("date", "nw_headline", "gd_headline", "fuzzy_score")
)

# %% [markdown]
# ### Newswire vs RavenPack

# %%
print("Top 10 GOOD matches (Newswire vs RavenPack):")
(
    rp_crosswalk.filter(pl.col("fuzzy_score") >= 80)
    .sort("fuzzy_score", descending=True)
    .head(10)
    .select("date", "nw_headline", "rp_headline", "fuzzy_score")
)

# %%
print("Top 10 BORDERLINE matches (Newswire vs RavenPack):")
(
    rp_crosswalk.filter(
        (pl.col("fuzzy_score") >= 60) & (pl.col("fuzzy_score") < 80)
    )
    .sort("fuzzy_score", descending=True)
    .head(10)
    .select("date", "nw_headline", "rp_headline", "fuzzy_score")
)

# %%
print("Top 10 NON-MATCHES — highest-scoring (Newswire vs RavenPack):")
(
    rp_crosswalk.filter(pl.col("fuzzy_score") < 60)
    .sort("fuzzy_score", descending=True)
    .head(10)
    .select("date", "nw_headline", "rp_headline", "fuzzy_score")
)

# %% [markdown]
# ## 9. Summary
#
# **Key findings:**
#
# - The newswire data lake contains raw press-release headlines with no
#   company identifiers — matching against GDELT and RavenPack relies
#   purely on headline text similarity.
# - Date-blocked `token_sort_ratio` fuzzy matching keeps the comparison
#   space tractable (one `cdist` call per calendar date).
# - The score distributions and tier breakdowns show what fraction of
#   newswire headlines find plausible counterparts in each reference
#   dataset.
# - Good matches (score >= 80) confirm that the same press releases
#   propagate across wire services, GDELT, and RavenPack.
# - Non-matches highlight content unique to each source — either
#   editorial filtering or differences in coverage scope.
#
# **Next steps:**
#
# 1. Use matched headlines to transfer company identifiers (permno/ticker)
#    from GDELT/RavenPack onto newswire headlines.
# 2. Expand to the full crawl once matching quality is validated.
# 3. Investigate whether additional newswire sources improve coverage.

# %% [markdown]
# ## 10. Load Newswire–RavenPack Crosswalk
#
# Load the production crosswalk built by `create_newswire_ravenpack_crosswalk.py`.
# This links free newswire headlines to RavenPack headlines via date-blocked
# fuzzy matching (token_sort_ratio >= 80).

# %%
cw = pl.read_parquet(DATA_DIR / "newswire_ravenpack_crosswalk.parquet")
print(f"Crosswalk rows: {len(cw):,}")
print(f"Columns: {cw.columns}")
print(f"Date range: {cw['date'].min()} to {cw['date'].max()}")
print(f"Distinct dates: {cw['date'].n_unique()}")

# %%
cw.head(5)

# %% [markdown]
# ## 11. Coverage Statistics
#
# How many newswire headlines found a RavenPack match, and vice versa?

# %% [markdown]
# ### Left side: newswire coverage

# %%
nw_full = load_newswire_headlines().collect()
nw_full = nw_full.with_columns(pl.col("date").cast(pl.Date).alias("pub_date"))

nw_total = len(nw_full)
nw_matched_urls = cw["nw_source_url"].n_unique()
nw_total_urls = nw_full["source_url"].n_unique()

print(f"Total newswire headlines:   {nw_total:,}")
print(f"Unique newswire URLs:       {nw_total_urls:,}")
print(f"Matched newswire URLs:      {nw_matched_urls:,}")
print(f"Newswire match rate:        {nw_matched_urls / nw_total_urls * 100:.1f}%")

# %%
nw_by_source = (
    nw_full.group_by("source")
    .agg(pl.col("source_url").n_unique().alias("total_urls"))
)
cw_by_source = (
    cw.group_by("nw_source")
    .agg(pl.col("nw_source_url").n_unique().alias("matched_urls"))
)
nw_coverage = (
    nw_by_source.join(cw_by_source, left_on="source", right_on="nw_source", how="left")
    .with_columns(pl.col("matched_urls").fill_null(0))
    .with_columns((pl.col("matched_urls") / pl.col("total_urls") * 100).alias("match_pct"))
    .sort("total_urls", descending=True)
)
print("Newswire coverage by source:")
nw_coverage

# %% [markdown]
# ### Right side: RavenPack coverage

# %%
rp_full = (
    pl.scan_parquet(DATA_DIR / "ravenpack_djpr.parquet")
    .with_columns(pl.col("timestamp_utc").cast(pl.Date).alias("date"))
    .filter(
        (pl.col("date") >= cw["date"].min())
        & (pl.col("date") <= cw["date"].max())
    )
    .select("date", "rp_story_id", "entity_name", "source_name", "headline")
    .collect()
)

rp_total_stories = rp_full["rp_story_id"].n_unique()
rp_matched_stories = cw["rp_story_id"].n_unique()

print(f"Total RP stories in date range:  {rp_total_stories:,}")
print(f"Matched RP stories:              {rp_matched_stories:,}")
print(f"RP match rate:                   {rp_matched_stories / rp_total_stories * 100:.1f}%")

# %%
rp_by_source = (
    rp_full.group_by("source_name")
    .agg(pl.col("rp_story_id").n_unique().alias("total_stories"))
)
cw_rp_by_source = (
    cw.group_by("rp_source_name")
    .agg(pl.col("rp_story_id").n_unique().alias("matched_stories"))
)
rp_coverage = (
    rp_by_source.join(cw_rp_by_source, left_on="source_name", right_on="rp_source_name", how="left")
    .with_columns(pl.col("matched_stories").fill_null(0))
    .with_columns((pl.col("matched_stories") / pl.col("total_stories") * 100).alias("match_pct"))
    .sort("total_stories", descending=True)
)
print("RavenPack coverage by source:")
rp_coverage

# %% [markdown]
# ### Fuzzy score distribution of crosswalk matches

# %%
fig, ax = plt.subplots(figsize=(8, 4))
scores_np = cw["fuzzy_score"].to_numpy()
ax.hist(scores_np, bins=50, color="steelblue", edgecolor="white", alpha=0.8)
ax.axvline(90, color="green", linestyle="--", linewidth=1.5, label="Excellent (>=90)")
ax.axvline(80, color="orange", linestyle="--", linewidth=1.5, label="Threshold (80)")
ax.set_xlabel("Fuzzy Score")
ax.set_ylabel("Count")
ax.set_title("Crosswalk Fuzzy Score Distribution (matches only)", fontweight="bold")
ax.legend(fontsize=8)
fig.tight_layout()
plt.show()

# %%
print(f"Fuzzy score stats:")
print(f"  Min:    {scores_np.min():.1f}")
print(f"  Median: {np.median(scores_np):.1f}")
print(f"  Mean:   {scores_np.mean():.1f}")
print(f"  Max:    {scores_np.max():.1f}")
print(f"  >= 90:  {(scores_np >= 90).sum():,} ({(scores_np >= 90).mean() * 100:.1f}%)")
print(f"  80-89:  {((scores_np >= 80) & (scores_np < 90)).sum():,} ({((scores_np >= 80) & (scores_np < 90)).mean() * 100:.1f}%)")

# %% [markdown]
# ## 12. Time-Series: Matched Articles per Day
#
# Daily count of crosswalk matches from 2020 to present, with total newswire
# and RavenPack headlines per day for context.

# %%
cw_daily = (
    cw.group_by("date")
    .agg(pl.len().alias("matched"))
    .sort("date")
)

nw_daily = (
    nw_full.group_by("pub_date")
    .agg(pl.len().alias("nw_total"))
    .sort("pub_date")
)

rp_daily = (
    rp_full.group_by("date")
    .agg(pl.len().alias("rp_total"))
    .sort("date")
)

# %%
fig, ax = plt.subplots(figsize=(14, 5))

ax.plot(
    nw_daily["pub_date"].to_list(),
    nw_daily["nw_total"].to_list(),
    color="lightcoral",
    alpha=0.4,
    linewidth=0.8,
    label="Newswire total",
)
ax.plot(
    rp_daily["date"].to_list(),
    rp_daily["rp_total"].to_list(),
    color="plum",
    alpha=0.4,
    linewidth=0.8,
    label="RavenPack total",
)
ax.plot(
    cw_daily["date"].to_list(),
    cw_daily["matched"].to_list(),
    color="steelblue",
    alpha=0.8,
    linewidth=1.0,
    label="Matched (crosswalk)",
)

ax.set_xlabel("Date")
ax.set_ylabel("Articles per Day")
ax.set_title("Daily Headline Counts: Newswire, RavenPack, and Matched", fontweight="bold")
ax.legend(fontsize=9)
fig.tight_layout()
plt.show()

# %% [markdown]
# ### Monthly averages

# %%
cw_monthly = (
    cw.with_columns(pl.col("date").dt.truncate("1mo").alias("month"))
    .group_by("month")
    .agg(
        pl.len().alias("total_matches"),
        pl.col("date").n_unique().alias("active_days"),
    )
    .with_columns((pl.col("total_matches") / pl.col("active_days")).alias("matches_per_day"))
    .sort("month")
)

fig, ax = plt.subplots(figsize=(14, 4))
ax.bar(
    cw_monthly["month"].to_list(),
    cw_monthly["matches_per_day"].to_list(),
    width=25,
    color="steelblue",
    edgecolor="white",
    alpha=0.8,
)
ax.set_xlabel("Month")
ax.set_ylabel("Avg Matched Articles / Day")
ax.set_title("Monthly Average: Matched Headlines per Day", fontweight="bold")
fig.tight_layout()
plt.show()

# %%
print("Monthly crosswalk summary:")
cw_monthly.select("month", "total_matches", "active_days", "matches_per_day")

# %% [markdown]
# ### Coverage by newswire source over time

# %%
cw_source_monthly = (
    cw.with_columns(pl.col("date").dt.truncate("1mo").alias("month"))
    .group_by("month", "nw_source")
    .agg(pl.len().alias("matches"))
    .sort("month", "nw_source")
)

sources = sorted(cw_source_monthly["nw_source"].unique().to_list())
colors = {"prnewswire": "steelblue"}

fig, ax = plt.subplots(figsize=(14, 4))
for src in sources:
    subset = cw_source_monthly.filter(pl.col("nw_source") == src)
    ax.plot(
        subset["month"].to_list(),
        subset["matches"].to_list(),
        label=src,
        color=colors.get(src, "gray"),
        linewidth=1.2,
        alpha=0.8,
    )
ax.set_xlabel("Month")
ax.set_ylabel("Matched Headlines")
ax.set_title("Monthly Matched Headlines by Newswire Source", fontweight="bold")
ax.legend(fontsize=9)
fig.tight_layout()
plt.show()

# %%
