
## Description

GDELT Global Knowledge Graph (GKG) headlines filtered to source domains
that overlap with the RavenPack DJ Press Release feed (PR Newswire,
Business Wire, GlobeNewswire, MarketWatch, WSJ, Barron's, etc.). The
query runs in Google BigQuery and restricts to English-language articles
with a `<PAGE_TITLE>` tag.

Filtering steps:
1. English-language articles only (`TranslationInfo IS NULL`)
2. Must contain a `<PAGE_TITLE>` tag in the GKG Extras field
3. Source domain must be in the `RP_SOURCE_DOMAINS` list
4. Headlines are HTML-unescaped, empty headlines are dropped, and duplicates
   are removed (keeping the earliest occurrence by `gkg_date`)

The data lake uses Hive-style partitioning (`year=YYYY/month=MM/data.parquet`).
Coverage begins October 2019 (when PAGE_TITLE was added to GKG). Polars
auto-detects the Hive partition columns (`year`, `month`) when scanning
the directory.

## Data Dictionary

- **gkg_date**: `Datetime(us, UTC)` GKG record timestamp, parsed from the raw GDELT integer `YYYYMMDDHHmmSS` DATE field. Represents when GDELT processed the article.
- **source_url**: `String` Full URL of the source article (e.g., `https://www.prnewswire.com/...`)
- **source_name**: `String` Domain or common name of the news source as reported by GDELT (e.g., "prnewswire.com", "marketwatch.com")
- **headline**: `String` Article headline extracted from the GKG Extras field's `<PAGE_TITLE>` tag, HTML-unescaped
- **year**: `Int64` *(Hive partition column)* Publication year, inferred from directory structure
- **month**: `Int64` *(Hive partition column)* Publication month, inferred from directory structure
