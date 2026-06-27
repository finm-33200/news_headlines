# TODO: Backfill PR Newswire Coverage (2012-07 through 2019-12)

> **See also**: [moredata_source_expansion_plan.md](moredata_source_expansion_plan.md)
> for the broader source expansion audit (P0–P4 priority list). This backfill
> is Priority P1 in that plan.

## Problem

The coverage chart (`rp_match_rate_coverage.html`) shows a gap from mid-2012 through end-2019 where match rate drops to near zero. This is caused by missing PR Newswire sitemap data — the live URLs at `prnewswire.com` return 404 for those months.

**On-disk data confirms a historical PR Newswire coverage gap, but the rerun story is subtler than “missing files” alone:**

| Period | Day-parquets | Notes |
|--------|-------------|-------|
| 2010-12 to 2012-06 | ~313 | Partial coverage |
| **2012-07 to 2019-12** | **sparse / inconsistent** | Some months still have month-level `.complete` markers even when daily output is partial or absent, so reruns may skip them |
| 2020-01 onward | ~328/year | Good coverage |

GDELT (starts Feb 2015, ~7% match rate) is not the fix. PR Newswire is the dominant source (~80%+ match rate).

## Root Cause

PR Newswire's monthly gzipped sitemaps (`Sitemap_Index_{Mon}_{Year}.xml.gz`) are no longer hosted for 2012-07 through 2019-12. That historical gap is now partially addressed: the scraper's Wayback Machine fallback (`_WAYBACK_TIMESTAMPS` in `pull_free_newswires.py`) contains validated timestamps for **79 of the 90** missing months, and the remaining **11 months** are explicitly tracked as having no validated Wayback archive yet.

## Current Status

### 1. `src/discover_wayback_timestamps.py` exists and validates cleanly

The standalone discovery/validation script is already implemented. It:
- Iterates over the missing 2012-07 through 2019-12 monthly sitemap URLs
- Queries the Wayback CDX API for archived snapshots
- Downloads candidate archives and validates they decompress and contain `/news-release` URLs
- Can validate the existing `_WAYBACK_TIMESTAMPS` block without making network calls

As of this validation pass, `python src/discover_wayback_timestamps.py --validate-existing` reports:
- 90 expected gap months
- 79 timestamped gap months
- 11 explicit no-archive months
- 100% of gap months accounted for

### 2. `_WAYBACK_TIMESTAMPS` has already been updated

The PR Newswire scraper now includes the discovered historical timestamp block plus the older corrupted-month fallbacks outside the 2012–2019 gap.

### 3. Remaining work: run the actual backfill

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
