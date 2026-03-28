# TODO: Backfill PR Newswire Coverage (2012-07 through 2019-12)

> **See also**: [moredata_source_expansion_plan.md](moredata_source_expansion_plan.md)
> for the broader source expansion audit (P0–P4 priority list). This backfill
> is Priority P1 in that plan.

## Problem

The coverage chart (`rp_match_rate_coverage.html`) shows a gap from mid-2012 through end-2019 where match rate drops to near zero. This is caused by missing PR Newswire sitemap data — the live URLs at `prnewswire.com` return 404 for those months.

**On-disk data confirms the gap:**

| Period | Day-parquets | Notes |
|--------|-------------|-------|
| 2010-12 to 2012-06 | ~313 | Partial coverage |
| **2012-07 to 2019-12** | **0** | No directories, no `.complete` markers |
| 2020-01 onward | ~328/year | Good coverage |

GDELT (starts Feb 2015, ~7% match rate) is not the fix. PR Newswire is the dominant source (~80%+ match rate).

## Root Cause

PR Newswire's monthly gzipped sitemaps (`Sitemap_Index_{Mon}_{Year}.xml.gz`) are no longer hosted for 2012-07 through 2019-12. The scraper's Wayback Machine fallback (`_WAYBACK_TIMESTAMPS` in `pull_free_newswires.py`) only covers 9 corrupted months in 2021-2023 — nothing for the 90-month gap.

## Proposed Fix

### 1. Create `src/discover_wayback_timestamps.py`

A standalone script that:
- Iterates over all 90 missing months (2012-07 through 2019-12)
- Constructs each sitemap URL: `https://www.prnewswire.com/Sitemap_Index_{Mon}_{Year}.xml.gz`
- Queries the Wayback CDX API (`http://web.archive.org/cdx/search/cdx?url={url}&output=json`) for archived snapshots
- For each snapshot (newest first), downloads via `https://web.archive.org/web/{timestamp}id_/{url}`, validates it decompresses and parses as XML with `/news-release` URLs
- Outputs a Python dict literal for `_WAYBACK_TIMESTAMPS` to stdout

Design: 1s delay between CDX queries, 2s between downloads. Optional `--cache-dir` for resumability. Dependencies: `requests`, `gzip`, `lxml` (already in environment).

### 2. Update `_WAYBACK_TIMESTAMPS` in `pull_free_newswires.py`

Merge discovered timestamps into the existing dict at line 255. Sort by key. Some months may have no archive — document as known gaps.

### 3. Run the backfill

```bash
python src/pull_free_newswires.py --full --start 2012-07-01 --end 2020-01-01
```

### 4. Rebuild crosswalk and coverage chart

Re-run the newswire-RavenPack crosswalk and coverage chart to verify the gap is filled.

## Verification

1. Spot-check: `python src/pull_free_newswires.py --month 2015-06`
2. Status: `python src/pull_free_newswires.py --status`
3. Visual: re-run `src/save_coverage_chart.py` and inspect the chart

## Edge Cases

- Some months may have no Wayback archive — those remain zero-data (acceptable)
- Older sitemaps likely lack `<news:title>` metadata → slow per-page fetch path (already implemented)
- Wayback 429 rate limiting → existing `_fetch` helper handles with exponential backoff
