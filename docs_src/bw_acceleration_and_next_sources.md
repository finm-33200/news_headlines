# Business Wire Acceleration & Next Sources

*Branch: `moredata` | Created: 2026-03-28*

## TL;DR

1. **BW slug extraction eliminates ~90% of Wayback page fetches.** CDX URLs
   contain the headline in the URL slug (`/en/Some-Headline-Here`). The
   current enumerator discards these. Fix: capture slugs at enumeration
   time, skip Wayback fetch for any URL that already has a headline.
   Estimated reduction: 18-day crawl → 2–3 days (for slug-less remainders).

2. **PRWeb is the highest-impact new source.** 1.05M releases (Jul 2013–
   present), same gz sitemap format as PR Newswire, slug-based fast-path
   with 0 page fetches. Trivial to add — reuse existing scraper pattern.

3. **Newswire.com adds ~200K releases** (2009–present) via yearly XML
   sitemaps with slug-based headline extraction.

---

## Part 1: Business Wire Acceleration

### Problem

Phase 3b (headline extraction) fetches ~1.6M archived pages from Wayback at
~1 req/s, taking an estimated 18 days. The bottleneck is archive.org's
per-request rate limit — bandwidth and parsing are trivial.

### Finding: Headlines are in the URL slugs

Business Wire CDX entries contain two URL variants for most articles:

```
/news/home/20240115003166/en/                                        (bare)
/news/home/20240115003166/en/First-Trust-Global-Funds-PLC-UK-...     (slug)
```

Empirical check (CDX queries for two sample dates):

| Date       | Unique article IDs | Have slug variant | Bare only (no slug) |
|------------|-------------------|-------------------|---------------------|
| 2024-01-15 | 81                | **81 (100%)**     | 0                   |
| 2015-06-15 | 103               | **101 (98%)**     | 2                   |

The current `_filter_english_articles()` requires `url_clean.endswith("/en")`,
which **discards all slug-bearing URLs**. Worse, 41/81 articles on the 2024
sample date *only* appear as slug URLs — the bare `/en` variant was never
captured by Wayback. Those articles are completely missed today.

### Recommended changes (priority order)

#### 1. Capture slugs in enumeration (DONE — see code change below)

Modify `enumerate_businesswire_urls.py`:
- Accept URLs matching `/en` OR `/en/{slug}`
- For each article ID, prefer the slug variant
- Store `headline_from_slug` column in inventory parquets

This is safe to apply now — the running crawl reads from *existing* inventory
parquets; re-enumerating produces new parquets that the fetcher will pick up
on its next day boundary.

#### 2. Skip Wayback fetch when slug headline exists

Modify `fetch_businesswire_headlines.py`:
- If `headline_from_slug` is present and non-empty, emit it directly
- Only fetch the Wayback page for URLs without a slug
- Expected: ~90–98% of URLs skip the fetch entirely

#### 3. Stream + early-close for remaining fetches

For the 2–10% of URLs that still need page fetches:
```python
with session.get(wb_url, stream=True, timeout=30) as resp:
    chunk = resp.raw.read(8192)   # <h1> is in first ~5KB
    resp.close()                   # abort the rest
    # parse chunk for <h1>
```
Saves bandwidth (80-90% per page) but does NOT increase request throughput
since archive.org rate-limits by request count, not bytes.

#### 4. Digest deduplication in CDX queries

Add `digest` to CDX `fl=` parameter. Multiple Wayback captures of the same
page (identical SHA-1 digest) can be collapsed — only fetch one.

#### 5. Monthly CDX batches (optional)

Query `businesswire.com/news/home/YYYYMM*` instead of per-day to reduce CDX
round-trips from ~7,300 to ~240. Per-day granularity is fine for the output
parquets — just partition the results after the query.

### Estimated impact

| Scenario | Pages to fetch | Time estimate |
|----------|---------------|---------------|
| Current (all pages) | ~1.6M | ~18 days |
| Slug extraction (98% hit) | ~32K | ~9 hours |
| Slug + stream + digest | ~20K | ~6 hours |

---

## Part 2: Additional Wire Sources

### P1: PRWeb (Cision budget tier) — 1.05M releases, 0 page fetches

PRWeb is Cision's self-service press release distribution platform. Separate
content from PR Newswire (SMB-focused).

**Infrastructure** (verified):
- Sitemap index: `https://www.prweb.com/sitemap-gz.xml`
- 156 monthly gzipped sitemaps: `Sitemap_Index_{Mon}_{YYYY}.xml.gz`
- **Identical format to PR Newswire and Newswire.ca**
- Coverage: July 2013 through December 2025

**URL pattern**: `/releases/{headline-slug}-{numeric-id}.html`
- Slug contains full headline text (verified against `news:title` in rolling
  news sitemap)
- Same extraction approach as Newswire.ca's `_headline_from_slug()`

**Volume**: ~1,046,535 total URLs. Peak ~27K/month in 2013, declining to
~1.3K/month in 2025.

**Implementation**: Add `PRWebScraper` class to `pull_free_newswires.py`.
Structurally identical to `NewswireCaScraper` — same gz sitemap pattern, same
slug-based fast-path. Estimated effort: 1–2 hours.

**RavenPack overlap**: Moderate. PRWeb distributes mostly SMB press releases,
some of which appear in RP's feed. The sheer volume (1M+ releases) means even
a modest match rate yields significant crosswalk hits.

---

### P2: Newswire.com — ~200K releases, 0 page fetches

Separate from Newswire.ca. US-focused wire distribution service.

**Infrastructure** (verified):
- Sitemap index: `https://www.newswire.com/sitemap/sitemap-index.xml`
- Yearly sitemaps: `sitemap-news-{YYYY}.xml` (2009–2026)
- Rolling news sitemap has `<news:title>` metadata (last ~3 days only)

**URL pattern**: `/news/{headline-slug}-{numeric-id}`

**Volume by year**: 11K (2009), 33K (2012), 25K (2015), 19K (2018), 14K (2024)

**Implementation**: New scraper class with yearly (not monthly) sitemap
parsing and slug-based headline extraction. Moderate effort — different
sitemap structure from the gz pattern.

**Caveat**: 5-second crawl-delay in robots.txt, observed 429 rate limiting.
Sitemap fetching should be polite.

---

### P3: EIN Presswire — forward collection only, ~300/day

**Infrastructure**: Rolling news sitemaps with `<n:title>` metadata (best
headline quality of any source). No historical sitemaps — current window only
(last 2–3 days).

**Assessment**: Useful for incremental daily collection going forward, not for
historical backfill. Lower priority than PRWeb/Newswire.com.

---

### Sources evaluated and rejected

| Source | Reason |
|--------|--------|
| AccessWire/ACCESS Newswire | Sitemaps return 403, no public press release index |
| News Direct (newsdirect.com) | Content farm (gambling/crypto spam), not real wire |
| Benzinga | `Disallow: /pressreleases/` in robots.txt |
| MarketWatch | DataDome bot protection, content from upstream wires |
| WebWire | 285K releases but no slugs, per-page fetching required |
| 24-7 Press Release | Low volume (~51 current), press releases path disallowed |

---

## Recommended Next Steps

| # | Action | Effort | Impact |
|---|--------|--------|--------|
| 1 | **Re-enumerate BW with slug capture** | 30 min (script already updated) | Unlocks slug fast-path |
| 2 | **Update BW fetcher to skip slug URLs** | 1 hour | 90%+ fetch elimination |
| 3 | **Add PRWeb scraper** | 1–2 hours | +1.05M headlines, 0 fetches |
| 4 | **Add Newswire.com scraper** | 2–3 hours | +200K headlines, 0 fetches |
| 5 | **Stream + early-close for BW remainders** | 30 min | Bandwidth savings on remaining fetches |
| 6 | **EIN Presswire forward collector** | 1 hour | +300/day going forward |

Items 1–2 should be done ASAP — they directly accelerate the current BW crawl.
Item 3 is the highest-volume new source with the lowest implementation effort.

---

## Technical References

- Wayback CDX API: `fl=timestamp,original,statuscode,digest`, `collapse=urlkey`
- `id_` URL modifier returns raw HTML (no toolbar) — already optimal
- Wayback does NOT support HTTP Range requests
- Streaming + early-close saves bandwidth but not request rate
- CDX does not store page titles — only URL metadata
