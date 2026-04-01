# Track One: Upstream Headline Source Expansion

*Branch: `moredata` | Created: 2026-03-27*

## Executive Summary

The pipeline currently ingests headlines from one newswire source (PR Newswire via
sitemap crawling) plus GDELT (via BigQuery). RavenPack's content is ~95% wire
services — PR Newswire, Business Wire, GlobeNewswire, and Dow Jones Newswires.
Adding more wire services directly increases crosswalk match rate because we're
matching the same underlying press releases RavenPack processes.

This document audits candidate upstream sources, proposes acquisition methods for
each, and recommends a priority order for implementation.

---

## Current State

### What's working

| Source | Method | Coverage | On-disk |
|--------|--------|----------|---------|
| **PR Newswire** | Gz sitemap crawl + Wayback fallback | 2010–present | ~4k day-parquets, gaps in 2012–2019 |
| **GDELT** | BigQuery S&P 500 filter | Feb 2015–present | Monthly parquets |
| **GlobeNewswire** | Plain monthly sitemaps (fast-path) | Apr 2023–present | **Implemented** — ~10K headlines/month |

### Known gaps

1. **PR Newswire 2012-07 through 2019-12**: historical monthly sitemap gap.
   `_WAYBACK_TIMESTAMPS` now covers 79 of 90 months; 11 months still have no
   validated Wayback archive. See `docs_src/todo_backfill_newswire_coverage.md`.
2. **Business Wire**: Removed from scraper — sitemaps consistently timeout.
3. **GlobeNewswire**: ~~Removed~~ **Now implemented** — 36 monthly sitemaps on
   `sitemaps.globenewswire.com`, Apr 2023–present. Validated: 10K+ headlines/month
   extracted via fast-path (titles in sitemap XML, no per-page HTTP).

### Architecture notes

- `pull_free_newswires.py` uses a scraper-class pattern (`ALL_SCRAPERS` list)
  designed for multiple sources. Adding a new source = adding a new scraper class.
- Output format is Hive-partitioned:
  `newswire_headlines/source={key}/year=YYYY/month=MM/day=DD/data.parquet`
- Crosswalk (`create_newswire_ravenpack_crosswalk.py`) is source-agnostic — it
  reads all `newswire_headlines/**/*.parquet` via `load_newswire_headlines()`.
- No code changes needed downstream when new sources are added.

---

## Source-by-Source Findings

### 1. GlobeNewswire — HIGHEST PRIORITY NEW SOURCE

**Key discovery**: The main-domain sitemap is nearly empty, but GlobeNewswire
hosts a comprehensive monthly sitemap index on a **separate subdomain**:

```
https://sitemaps.globenewswire.com/news-en.xml     (sitemap index)
https://sitemaps.globenewswire.com/news/en/YYYY-MM.xml  (monthly sitemap)
https://sitemaps.globenewswire.com/news/en/latest.xml   (rolling recent)
```

- **36 monthly sitemaps** covering **April 2023 through March 2026**
- January 2024 alone: **10,448 articles**
- Each entry includes: URL, publication date, **title**, keywords, stock tickers, genre
- Titles are embedded in the sitemap XML — **fast-path extraction** (no per-page HTTP)

**Acquisition methods:**

| Method | Feasibility | Historical depth | Speed |
|--------|------------|-----------------|-------|
| Sitemap (sitemaps subdomain) | Excellent | Apr 2023–present | Fast (metadata in XML) |
| Wayback CDX for sitemaps | Good | Pre-2023 | Moderate |
| RSS (20 items/feed, by category) | Working but tiny | Current only | Real-time |
| Wayback CDX for individual articles | Good (5K+ CDX pages) | 2010+ | Slow |

**Recommended approach:**
1. Implement `GlobeNewswireScraper` class targeting `sitemaps.globenewswire.com`
2. Monthly sitemaps are plain XML (not gzipped) — simpler than PR Newswire
3. For pre-2023: query Wayback CDX for monthly sitemap URLs, recover from archive
4. Expected RavenPack overlap: **High** — GlobeNewswire is one of RP's four primary feeds

**Estimated effort**: Small — the sitemap format is simpler than PR Newswire's
gzipped archives, and the existing `_parse_sitemap_news_entries()` should work
with minimal adaptation.

---

### 2. Newswire.ca / Cision — HIGH PRIORITY

**Key discovery**: Newswire.ca has the **same gz sitemap structure** as PR Newswire:

```
https://www.newswire.ca/sitemap-gz.xml              (sitemap index)
https://www.newswire.ca/Sitemap_Index_Mon_YYYY.xml.gz  (monthly gz)
```

- **185 compressed sitemap files** covering ~2010 through 2026
- Same `Sitemap_Index_{Mon}_{YYYY}.xml.gz` naming convention as PR Newswire
- Also has paginated archive: `?page=N&pagesize=100`

**Acquisition methods:**

| Method | Feasibility | Historical depth | Speed |
|--------|------------|-----------------|-------|
| Gz sitemap (identical pattern to PRN) | Excellent | 2010–present | Fast |
| Paginated archive endpoint | Good | Unknown depth | Moderate |
| Wayback CDX | Unknown | Unknown | Slow |

**Recommended approach:**
1. The existing `PRNewswireScraper` logic can be largely reused — same gz format
2. Create `NewswireCaScraper` class with base URL `newswire.ca`
3. Parse sitemap XML for press-release URLs and metadata

**Estimated effort**: Small — structurally identical to existing PR Newswire scraper.

**Expected RavenPack overlap**: Moderate — Cision/CNW distributes Canadian and
some US press releases. Many cross-listed companies issue releases on both PR
Newswire and Newswire.ca. The overlap with RavenPack's DJ Press Release feed
will be lower than PR Newswire or GlobeNewswire but still meaningful.

---

### 3. Business Wire — MEDIUM PRIORITY (Wayback-dependent)

Business Wire's live sitemaps and server consistently **timeout**. No public API
or accessible paginated archive exists. However, Wayback Machine coverage is
**excellent** (8,000+ CDX pages, ~1B+ captures).

**Acquisition methods:**

| Method | Feasibility | Historical depth | Speed |
|--------|------------|-----------------|-------|
| Live sitemap | Broken (timeouts) | N/A | N/A |
| Wayback CDX URL enumeration | Excellent | 2000s–present | Moderate |
| Atom feeds (full-text, by category) | Works for forward collection | Current only | Real-time |
| Wayback CDX for article pages | Excellent (8K+ CDX pages) | Deep | Slow |

**Recommended approach (Wayback CDX):**

Business Wire URLs embed the date: `/news/home/YYYYMMDDNNNNNN/en/`

1. Query Wayback CDX API for all captured URLs matching
   `businesswire.com/news/home/YYYYMMDD*` for each date
2. Extract headline from the `<title>` tag of each archived page
3. Extract publication date from URL or `<meta>` tags
4. This is a **URL enumeration** approach, not a sitemap approach

```
https://web.archive.org/cdx/search/cdx?url=businesswire.com/news/home/YYYYMMDD*&output=json&fl=timestamp,original,statuscode&filter=statuscode:200
```

**Alternative: Atom feeds for forward collection:**
- Business Wire offers custom Atom feeds with full-text content
- Useful for incremental forward collection but not historical backfill
- Feed builder at `businesswire.com/help/feed-options`

**Estimated effort**: Medium — requires new Wayback CDX enumeration logic not
in the current codebase. Rate limiting (1-2 req/s to archive.org) means backfill
is slow.

**Expected RavenPack overlap**: **Very high** — Business Wire is one of RP's
primary wire services.

---

### 4. PR Newswire Backfill (2012-07 to 2019-12) — HIGH PRIORITY

Already documented in `docs_src/todo_backfill_newswire_coverage.md`. The approach
is sound:

1. Create `src/discover_wayback_timestamps.py`
2. Query Wayback CDX for each of the ~90 missing monthly sitemap gz URLs
3. Populate `_WAYBACK_TIMESTAMPS` in `pull_free_newswires.py`
4. Run `python src/discover_wayback_timestamps.py --validate-existing`
5. Re-run `--full --start 2012-07-01 --end 2020-01-01`

**Note**: On-disk data shows some coverage in 2012-2019 already (136-284
day-parquets/year vs 328+ for complete years), suggesting partial success on
prior runs. The remaining months with 0 parquets need the Wayback fallback.

**Estimated effort**: Small — the TODO doc has a complete implementation plan.

---

### 5. AccessWire — NOT VIABLE

- Sitemaps return 403
- RSS is explicitly disallowed in robots.txt
- No accessible API
- Newsroom is JavaScript-only with no pagination endpoint

**Recommendation**: Skip entirely.

---

### 7. MarketWatch / Dow Jones Newswires — NOT VIABLE

- Aggressive DataDome bot protection (401 on all automated requests)
- No accessible sitemap, RSS, or API
- Press releases reportedly expire after 90 days
- Content originates from the same wire services we can scrape directly

**Recommendation**: Skip — go to upstream wires instead.

---

### 8. Yahoo Finance — NOT VIABLE

- Entirely JavaScript-rendered; 503 responses to programmatic requests
- No RSS, sitemap, or API for press releases
- Content originates from PR Newswire, Business Wire, GlobeNewswire

**Recommendation**: Skip — go to upstream wires instead.

---

## Recommended Priority Order

| Priority | Source | Why | Effort | Expected Impact |
|----------|--------|-----|--------|-----------------|
| **P0** | GlobeNewswire (sitemap) | **DONE** — scraper implemented and validated. 36 months, ~10K headlines/month. | Small | High |
| **P1** | PR Newswire backfill | Timestamp block largely populated; validate and backfill remaining 11 no-archive months only if new captures appear | Small | High (fills 90-month gap) |
| **P2** | Newswire.ca / Cision | **DONE** — `NewswireCaScraper` implemented. 179 monthly gz sitemaps, ~2.6K/month. Slug-based fast-path (0 page fetches). | Small | Moderate |
| **P3** | Business Wire (Wayback) | **TOOLING DONE** — CDX enumeration + Wayback headline fetcher built. 218 articles/day from CDX. Two-phase pipeline. | Medium | High |

---

## Experiment Plan

### Phase 1: Quick wins (P0 + P1)

**GlobeNewswire scraper: DONE**
- `GlobeNewswireScraper` class added to `pull_free_newswires.py`
- Sitemap index discovery via `available_months()` method
- `EARLIEST_MONTH` attribute skips pre-2023 months in full crawls
- Validated: 9,919 headlines extracted for Jan 2025 (0 page fetches)
- Full backfill: `python src/pull_free_newswires.py --full --start 2023-04-01`

**PR Newswire Wayback backfill:**
- `src/discover_wayback_timestamps.py` implemented and validated (dry-run)
- Full discovery run in progress (querying CDX for ~90 missing months)
- Next steps: update `_WAYBACK_TIMESTAMPS` with results, re-crawl gap months

**Validation**: Re-run `save_coverage_chart.py` and `03_crosswalk_quality_ipynb.py`
to visualize the before/after impact on RP match rate.

### Phase 2: Cision/CNW (P2) — DONE

`NewswireCaScraper` class added to `pull_free_newswires.py`:
- Reuses PRN gz sitemap structure (`Sitemap_Index_{Mon}_{YYYY}.xml.gz`)
- 179 monthly sitemaps, ~2,600 releases/month (2011–present)
- **Slug-based fast-path**: headlines extracted from URL slugs + `lastmod`
  dates, with **0 per-page HTTP fetches**. Slug-derived headlines are
  approximate (title-cased, no punctuation) but sufficient for TF-IDF
  crosswalk matching.
- `_crawl_scraper_for_month` updated to call `scraper.parse_entries_from_xml()`
  when available, enabling scraper-specific fast paths.
- Full backfill: `python src/pull_free_newswires.py --full --start 2011-01-01`

Validated: 2,613 headlines extracted for Jan 2023, 31 days, 0 page fetches.

### Phase 3: Business Wire (P3) — TOOLING DONE

Business Wire's live sitemaps still timeout, so a two-phase Wayback-based
pipeline was built:

**Phase 3a: URL enumeration** (`src/enumerate_businesswire_urls.py`)
- Queries Wayback CDX for `businesswire.com/news/home/YYYYMMDD*` per day
- Client-side filter for English articles (`/en/` suffix)
- Saves URL inventory as Hive-partitioned parquets
- Validated: 218 English articles for Jan 2, 2024; 308 for Jan 3, 2024
- Full enumeration estimate: ~7,300 CDX queries = ~3 hours

**Phase 3b: Headline extraction** (`src/fetch_businesswire_headlines.py`)
- Reads URL inventory, fetches each archived page from Wayback
- Extracts headline from `<h1>` tag, date from URL
- Rate-limited to 1 req/s, 2 concurrent workers
- Resumable (completed days skipped)
- Full extraction estimate: ~1.6M pages at 1 req/s = ~18 days

**To run:**
```bash
# Phase 1: enumerate URLs (fast, ~3 hours for 20 years)
python src/enumerate_businesswire_urls.py --start 2004-01-01 --end 2026-04-01

# Phase 2: fetch headlines (slow, days/weeks)
python src/fetch_businesswire_headlines.py --start 2004-01-01 --end 2026-04-01
```

---

## Architectural Consideration: Removing the S&P 500 Pre-filter

The user is considering removing the S&P 500 pre-filter upstream and storing
**all** raw headlines first, then filtering downstream.

### Current state

- **GDELT**: Pre-filtered to S&P 500 companies server-side in BigQuery (saves
  transfer costs — ~436K articles/day unfiltered vs ~5K after S&P 500 filter)
- **Newswires**: Already store **all** raw headlines (no pre-filter). S&P 500
  filtering happens only in analysis notebooks.
- **RavenPack**: Pre-filtered to US companies with relevance >= 90.

### Implications for track one

**Newswire ingestion is already unfiltered.** The `pull_free_newswires.py`
scraper stores every press release it finds, regardless of company. The S&P 500
filter is applied only in `01_data_sources_overview_ipynb.py` for display
purposes. So for track one, the "remove pre-filter" change is a **no-op** for
newswire sources.

**GDELT is the only filtered source**, but that's a track-two concern (GDELT
optimization). Removing the BigQuery-side S&P 500 filter would increase data
transfer ~90x and BigQuery costs proportionally. The better approach is to keep
the server-side filter for GDELT but broaden it (e.g., to all CRSP-listed firms,
or to a keyword list).

### Impact on crosswalking

The crosswalk (`create_newswire_ravenpack_crosswalk.py`) is already
universe-agnostic — it matches **all** newswire headlines against **all**
RavenPack headlines (within the date range). Expanding upstream sources feeds
directly into the crosswalk with no code changes.

If the RavenPack pull were broadened (e.g., dropping the `relevance >= 90`
filter, or including non-US companies), the crosswalk would automatically
find more matches. But that's a separate decision.

---

## Files Changed

- `src/pull_free_newswires.py` — Added `GlobeNewswireScraper`, `NewswireCaScraper`
  classes. Updated `_crawl_scraper_for_month` to support scraper-specific
  `parse_entries_from_xml()` fast paths. `ALL_SCRAPERS` now includes 3 sources.
- `src/discover_wayback_timestamps.py` — Standalone Wayback CDX timestamp discovery
  for the PR Newswire 2012–2019 gap (new)
- `src/enumerate_businesswire_urls.py` — Wayback CDX URL enumeration for Business
  Wire articles by date (new, phase 3a)
- `src/fetch_businesswire_headlines.py` — Wayback page fetcher + headline extraction
  for Business Wire (new, phase 3b)
- `docs_src/moredata_source_expansion_plan.md` — this document (new, updated)
