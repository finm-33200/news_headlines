
## Description

Free newswire press-release headlines scraped from PR Newswire and GlobeNewswire
via sitemap crawling.

- **PR Newswire**: Gzipped monthly sitemaps with Wayback Machine fallback for
  missing months. Headlines extracted from `<news:title>` metadata or by
  fetching individual pages. Coverage: 2010–present (gaps in 2012–2019 being
  backfilled via Wayback).
- **GlobeNewswire**: Plain monthly sitemaps on `sitemaps.globenewswire.com`.
  Titles are embedded in the sitemap XML — fast-path extraction with no
  per-page HTTP. Coverage: April 2023–present (~10K articles/month).

The data lake uses Hive-style partitioning
(`source={key}/year=YYYY/month=MM/day=DD/data.parquet`). Completed days are
skipped on re-run, making the crawl resumable and safe to interrupt.

The raw headlines contain no company identifiers. Company attribution is
available via the separate `newswire_ravenpack_crosswalk` dataframe, which
fuzzy-matches these headlines against RavenPack to transfer entity metadata.

## Data Dictionary

- **headline**: `String` Press release headline text, extracted from the sitemap `<news:title>` element or the page's `<h1>` tag
- **source_url**: `String` Full URL of the press release page (serves as a unique row identifier)
- **date**: `String` Publication date in `YYYY-MM-DD` format, extracted from sitemap metadata or the page's `<meta name="date">` tag
- **source**: `String` *(Hive partition column)* Wire service key identifying the data source ("prnewswire" or "globenewswire")
- **year**: `Int64` *(Hive partition column)* Publication year, inferred from directory structure
- **month**: `Int64` *(Hive partition column)* Publication month, inferred from directory structure
- **day**: `Int64` *(Hive partition column)* Publication day, inferred from directory structure
