
## Description

Free newswire press-release headlines scraped from PR Newswire via sitemap
crawling. The scraper downloads gzipped monthly sitemaps from PR Newswire and
extracts headlines either directly from Google News sitemap metadata
(`<news:title>` tags) or, when metadata is unavailable, by fetching individual
press-release pages and parsing the `<h1>` tag.

The data lake uses Hive-style partitioning
(`source={key}/year=YYYY/month=MM/day=DD/data.parquet`). Completed days are
skipped on re-run, making the crawl resumable and safe to interrupt. Coverage
begins January 2010 (PR Newswire).

The raw headlines contain no company identifiers. Company attribution is
available via the separate `newswire_ravenpack_crosswalk` dataframe, which
fuzzy-matches these headlines against RavenPack to transfer entity metadata.

## Data Dictionary

- **headline**: `String` Press release headline text, extracted from the sitemap `<news:title>` element or the page's `<h1>` tag
- **source_url**: `String` Full URL of the press release page (serves as a unique row identifier)
- **date**: `String` Publication date in `YYYY-MM-DD` format, extracted from sitemap metadata or the page's `<meta name="date">` tag
- **source**: `String` *(Hive partition column)* Wire service key identifying the data source (e.g., "prnewswire")
- **year**: `Int64` *(Hive partition column)* Publication year, inferred from directory structure
- **month**: `Int64` *(Hive partition column)* Publication month, inferred from directory structure
- **day**: `Int64` *(Hive partition column)* Publication day, inferred from directory structure
