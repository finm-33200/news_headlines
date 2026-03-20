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
# # Data Sources & Strategy
#
# ## Why This Pipeline Exists
#
# **RavenPack** is the gold standard for firm-level financial news data:
# every headline is pre-tagged with entity identifiers, relevance scores,
# sentiment, and topic classifications. But its **terms of use prohibit
# uploading headline text to LLMs** like ChatGPT for NLP analysis.
#
# **The workaround:** source headlines independently from free sources,
# then **fuzzy-match** them to RavenPack to inherit its entity metadata.
# Only the independently-sourced headlines get uploaded to LLMs —
# RavenPack's text stays local, but its metadata travels via the
# crosswalk.
#
# **Three headline sources in this pipeline:**
#
# 1. **RavenPack** — the metadata reference. High-quality entity tags and
#    sentiment, but headlines cannot be uploaded to LLMs.
# 2. **GDELT** — free, massive, but noisy. Covers the open web, not wire
#    services. Only ~7% of its headlines match RavenPack per-headline,
#    but its enormous volume makes it the **largest contributor to overall
#    RavenPack coverage**.
# 3. **Scraped newswires** (PR Newswire, Business Wire, GlobeNewswire) —
#    higher per-headline match rates (same wire services RavenPack draws
#    from), but smaller total volume limits their coverage contribution.
#
# Both GDELT and newswire crosswalks are combined to maximize RavenPack
# coverage. The pipeline links each free headline to its best RavenPack
# match, transferring entity IDs and sentiment without violating terms
# of use.

# %%
import re
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import polars as pl
from settings import config

DATA_DIR = Path(config("DATA_DIR"))

# %% [markdown]
# ---
# ## Key Result: RavenPack Headline Coverage

# %%
_cw = pl.read_parquet(DATA_DIR / "newswire_ravenpack_crosswalk.parquet")

from pull_free_newswires import load_newswire_headlines as _load_nw

_nw_total_urls = (
    _load_nw()
    .with_columns(pl.col("date").cast(pl.Date))
    .filter(
        (pl.col("date") >= _cw["date"].min()) & (pl.col("date") <= _cw["date"].max())
    )
    .select(pl.col("source_url").n_unique())
    .collect()
    .item()
)
_nw_matched_urls = _cw["nw_source_url"].n_unique()
_nw_match_rate = _nw_matched_urls / _nw_total_urls * 100

_rp_full = (
    pl.scan_parquet(DATA_DIR / "ravenpack_djpr.parquet")
    .with_columns(pl.col("timestamp_utc").cast(pl.Date).alias("date"))
    .filter(
        (pl.col("date") >= _cw["date"].min()) & (pl.col("date") <= _cw["date"].max())
    )
    .select("rp_story_id")
)
_rp_total_stories = _rp_full.select(pl.col("rp_story_id").n_unique()).collect().item()
_rp_matched_stories = _cw["rp_story_id"].n_unique()
_rp_match_rate = _rp_matched_stories / _rp_total_stories * 100

pl.DataFrame({
    "metric": [
        "RP headlines matched",
        "NW headlines matched",
        "Crosswalk pairs",
        "Date range",
    ],
    "value": [
        f"{_rp_match_rate:.1f}% ({_rp_matched_stories:,} / {_rp_total_stories:,})",
        f"{_nw_match_rate:.1f}% ({_nw_matched_urls:,} / {_nw_total_urls:,})",
        f"{len(_cw):,}",
        f"{_cw['date'].min()} to {_cw['date'].max()} ({_cw['date'].n_unique():,} dates)",
    ],
})

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
# RavenPack's strengths: curated entity matching, sentiment scores, and
# topic classification. Its content is **~95% wire services** (Dow Jones,
# PR Newswire, Business Wire, GlobeNewswire). The limitation: headline
# text cannot be uploaded to LLMs under RavenPack's terms of use.
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
# ### Articles per month

# %%
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
# ## 2. S&P 500 Constituents — `sp500_constituents.parquet`
#
# Source script: `pull_sp500_constituents.py`
#
# Historical membership list of the S&P 500 index from CRSP
# (`crsp_m_indexes.dsp500list_v2`). Each row is one membership spell
# for a single PERMNO — i.e. the period during which a stock was part
# of the index.
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
# ---
# ## 3. GDELT S&P 500 — `gdelt_sp500_headlines/`
#
# Source script: `pull_gdelt_sp500_headlines.py`
#
# Page-title headlines extracted from GDELT's Global Knowledge Graph 2.0,
# filtered server-side in BigQuery to articles mentioning S&P 500 companies.
# Stored as a Hive-partitioned data lake (`year=YYYY/month=MM/data.parquet`).
#
# GDELT is **free and massive** (~400k articles/day), but its content comes
# from the **open web** — blogs, aggregators, regional news — not the wire
# services that dominate RavenPack. This means fuzzy-match overlap with
# RavenPack is low (~7%). The false-positive problem and filtering quality
# are explored in notebook 02. We will use GDELT in more depth in a
# separate project.
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
# ### Date range

# %%
gd.select(
    pl.col("gkg_date").min().alias("earliest"),
    pl.col("gkg_date").max().alias("latest"),
).collect()

# %% [markdown]
# ### Headlines per day

# %%
gd_collected = gd.collect()
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
# ## 4. Free Newswires — `newswire_headlines/`
#
# Source script: `pull_free_newswires.py`
#
# Press release headlines scraped from PR Newswire via sitemap crawling,
# then filtered locally to S&P 500 companies using normalized company
# name substring matching.
#
# These are the **same wire services that RavenPack draws from**, scraped
# directly from their public-facing websites. This makes scraped newswires
# the **primary headline source** for matching to RavenPack — because
# the underlying press releases are identical, fuzzy-match rates are much
# higher than with GDELT.
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
# ---
# ## 5. Newswire–RavenPack Crosswalk — `newswire_ravenpack_crosswalk.parquet`
#
# Source script: `create_newswire_ravenpack_crosswalk.py`
#
# Fuzzy-matched bridge between free newswire headlines and RavenPack DJ Press
# Release headlines. Each row is the single best RavenPack match for a given
# newswire headline on the same calendar date, kept only when the
# `token_sort_ratio` score is $\geq 80$.
#
# **This is the key pipeline output.** It enables enriching free newswire data
# with RavenPack's entity identifiers and sentiment scores, without uploading
# RavenPack's headline text to any external service.
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
# ---
# ## GDELT vs Scraped Newswires: Different Strengths
#
# RavenPack's content is **~95% wire services** — Dow Jones Newswires,
# PR Newswire, Business Wire, and GlobeNewswire. Scraping these wire
# services directly gives us the **same underlying press releases** that
# RavenPack processes, so the **per-headline match rate is high**.
#
# GDELT crawls the **open web** — Yahoo Finance, aggregator sites,
# regional news, blogs. Only **~7%** of GDELT's S&P 500 headlines match
# RavenPack because they draw from fundamentally different source
# ecosystems. However, GDELT's **sheer volume** (hundreds of thousands
# of articles per day) compensates: despite the low per-headline match
# rate, GDELT actually covers a **larger fraction of the RavenPack
# universe** than newswire alone.
#
# In short: **newswire matches are more precise; GDELT matches are more
# numerous.** Both sources are needed for maximum RavenPack coverage.
# See notebook 03 (Crosswalk Quality) for the per-source breakdown chart.

# %% [markdown]
# ---
# ## Source Comparison
#
# | | RavenPack | GDELT | Scraped Newswires |
# |---|---|---|---|
# | **Cost** | Commercial | Free (BQ scan costs) | Free |
# | **Entity tagging** | Curated, high quality | Raw NLP, noisy | None (via crosswalk) |
# | **Primary sources** | Licensed wire services (~95%) | Open web (~98%) | Wire services (same as RP) |
# | **Per-headline RP match rate** | N/A | ~7% (different ecosystems) | Much higher (same sources) |
# | **RP coverage contribution** | N/A | Larger (volume compensates) | Smaller (fewer total headlines) |
# | **ChatGPT upload?** | No (terms of use) | Yes | Yes |
# | **Role in pipeline** | Metadata reference | Largest coverage contributor | Higher-precision matches |

# %% [markdown]
# ---
# ## Bottom Line

# %%
pl.DataFrame({
    "metric": [
        "RP headlines matched",
        "NW headlines matched",
        "Crosswalk pairs",
        "Date range",
    ],
    "value": [
        f"{_rp_match_rate:.1f}% ({_rp_matched_stories:,} / {_rp_total_stories:,})",
        f"{_nw_match_rate:.1f}% ({_nw_matched_urls:,} / {_nw_total_urls:,})",
        f"{len(_cw):,}",
        f"{_cw['date'].min()} to {_cw['date'].max()} ({_cw['date'].n_unique():,} dates)",
    ],
})
