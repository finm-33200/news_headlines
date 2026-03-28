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
| **GlobeNewswire** | (attempted, removed) | 2 days only | 2 day-parquets (2026-02) |

### Known gaps

1. **PR Newswire 2012-07 through 2019-12**: Live sitemaps 404; only 9 Wayback
   timestamps populated. See `docs_src/todo_backfill_newswire_coverage.md`.
2. **Business Wire**: Removed from scraper — sitemaps consistently timeout.
3. **GlobeNewswire**: Removed — believed to have only ~2 days in sitemap.
   **This is wrong** — see findings below.

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
4. Re-run `--full --start 2012-07-01 --end 2020-01-01`

**Note**: On-disk data shows some coverage in 2012-2019 already (136-284
day-parquets/year vs 328+ for complete years), suggesting partial success on
prior runs. The remaining months with 0 parquets need the Wayback fallback.

**Estimated effort**: Small — the TODO doc has a complete implementation plan.

---

### 5. SEC EDGAR EFTS — MEDIUM PRIORITY (different content type)

The SEC's full-text search API is **fully working** and covers **2001 to present**.

```
https://efts.sec.gov/LATEST/search-index?q="press release"&forms=8-K&startdt=YYYY-MM-DD&enddt=YYYY-MM-DD&from=0
```

- Returns up to 100 results per request; paginate with `from` parameter
- A single day (Jan 2, 2024) returned **179 hits** for `q="press release"&forms=8-K`
- Each hit includes: CIK, company name, filing date, accession number, file type
- The actual press release text is in EX-99.1 exhibits attached to 8-K filings

**Acquisition methods:**

| Method | Feasibility | Historical depth | Speed |
|--------|------------|-----------------|-------|
| EFTS search API | Excellent | 2001–present | Fast (100/req) |
| Atom feed per CIK | Working | Per-company | Real-time |
| Filing index (daily) | Available | 1993+ | Moderate |

**Recommended approach:**
1. Query EFTS for `forms=8-K` + `q="press release"` by date range
2. For each hit, fetch the EX-99.1 exhibit and extract the headline
3. Headline is typically the first `<h1>` or the exhibit's first line
4. Requires SEC-compliant User-Agent header

**Content character**: EDGAR press releases are the **same releases** distributed
by PR Newswire, Business Wire, and GlobeNewswire — companies file them as 8-K
exhibits. This makes EDGAR a **complementary path to the same content**, with
different coverage characteristics (better coverage of SEC-filing companies,
potentially different time lag).

**Estimated effort**: Medium — new scraper class + exhibit text parsing. The
EFTS API is well-behaved and doesn't require rate-limit gymnastics.

**Expected RavenPack overlap**: High for US public companies. EDGAR only covers
SEC filers, so non-US and private-company press releases won't appear here.

---

### 6. AccessWire — NOT VIABLE

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
| **P0** | GlobeNewswire (sitemap) | Major discovery — 3 years of monthly sitemaps with titles. High RP overlap. Minimal code. | Small | High |
| **P1** | PR Newswire backfill | Known gap, documented plan, just needs Wayback timestamp discovery | Small | High (fills 90-month gap) |
| **P2** | Newswire.ca / Cision | Same gz sitemap pattern as PRN — near-copy of existing scraper | Small | Moderate |
| **P3** | Business Wire (Wayback) | Huge RP overlap but requires new Wayback CDX enumeration pattern | Medium | High |
| **P4** | SEC EDGAR EFTS | Different content path, deep history (2001+), well-documented API | Medium | Moderate-High |

---

## Experiment Plan

### Phase 1: Quick wins (P0 + P1)

**GlobeNewswire scraper:**
1. Add `GlobeNewswireScraper` class to `pull_free_newswires.py`
2. Fetch `sitemaps.globenewswire.com/news-en.xml` → parse monthly sitemap URLs
3. For each month, fetch `sitemaps.globenewswire.com/news/en/YYYY-MM.xml`
4. Extract title, URL, date from sitemap entries (fast path — no page fetches)
5. Save to `newswire_headlines/source=globenewswire/year=.../month=.../day=.../data.parquet`
6. Run crosswalk and measure impact on RP match rate

**PR Newswire Wayback backfill:**
1. Implement `src/discover_wayback_timestamps.py` per the existing TODO doc
2. Run against 90 missing months
3. Update `_WAYBACK_TIMESTAMPS` dict
4. Run `pull_free_newswires.py --full --start 2012-07-01 --end 2020-01-01`

**Validation**: Re-run `save_coverage_chart.py` and `03_crosswalk_quality_ipynb.py`
to visualize the before/after impact on RP match rate.

### Phase 2: Cision/CNW (P2)

1. Add `NewswireCaScraper` class — reuse PRN gz sitemap logic with `newswire.ca` base URL
2. Full crawl of 185 monthly sitemaps (2010–2026)
3. Measure incremental RP overlap

### Phase 3: Wayback-based sources (P3)

1. Build a `WaybackCDXEnumerator` utility class for discovering article URLs
2. Implement `BusinessWireScraper` using CDX URL enumeration + page title extraction
3. Rate-limit to 1-2 req/s against archive.org
4. This is a long-running crawl (days/weeks for full history)

### Phase 4: EDGAR (P4)

1. Add `EdgarPressReleaseScraper` using EFTS search API
2. Query `forms=8-K, q="press release"` by date
3. For each hit, fetch EX-99.1 exhibit and extract headline
4. Requires SEC User-Agent compliance

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

- `docs_src/moredata_source_expansion_plan.md` — this document (new)
