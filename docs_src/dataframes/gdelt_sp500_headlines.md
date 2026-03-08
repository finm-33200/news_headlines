
## Description

GDELT Global Knowledge Graph (GKG) headlines filtered to S&P 500 companies
via a server-side JOIN in Google BigQuery. The pipeline uploads a normalized
S&P 500 company names lookup table to BigQuery, then JOINs it against GDELT's
`V2Organizations` field. This filters ~436K global headlines/day down to only
those mentioning S&P 500 companies.

Filtering steps:
1. English-language articles only (`TranslationInfo IS NULL`)
2. Must contain a `<PAGE_TITLE>` tag in the GKG Extras field
3. Each organization mention in `V2Organizations` is unnested and normalized
   (lowercased, punctuation removed, corporate suffixes stripped)
4. INNER JOIN against the S&P 500 names lookup on normalized name
5. Headlines are HTML-unescaped, empty headlines are dropped, and duplicates
   are removed (keeping the earliest occurrence by `gkg_date`)

The data lake uses Hive-style partitioning (`year=YYYY/month=MM/data.parquet`).
Coverage begins February 2015. Polars auto-detects the Hive partition columns
(`year`, `month`) when scanning the directory.

## Data Dictionary

- **gkg_date**: `Datetime(us, UTC)` GKG record timestamp, parsed from the raw GDELT integer `YYYYMMDDHHmmSS` DATE field. Represents when GDELT processed the article.
- **source_url**: `String` Full URL of the source article (e.g., `https://www.nytimes.com/...`)
- **source_name**: `String` Domain or common name of the news source as reported by GDELT (e.g., "nytimes.com", "yahoo.com")
- **V2Tone**: `String` Raw comma-delimited tone measures from the GKG: average tone, positive score, negative score, polarity, activity reference density, self/group reference density. Requires parsing.
- **V2Organizations**: `String` Full semicolon-delimited list of all organizations mentioned in the article, each with a character offset suffix (e.g., `"apple inc,123;microsoft corp,456"`). Raw GKG field, not filtered.
- **matched_org_raw**: `String` The specific organization name from `V2Organizations` that triggered the S&P 500 match, with the character offset stripped (e.g., "apple inc")
- **matched_company**: `String` Company name from the CRSP stocknames table that matched via normalized name comparison (e.g., "APPLE INC", "JPMORGAN CHASE & CO")
- **permno**: `Int64` CRSP permanent security identifier, linking to standard financial databases
- **ticker**: `String` Stock ticker symbol (e.g., "AAPL", "MSFT")
- **headline**: `String` Article headline extracted from the GKG Extras field's `<PAGE_TITLE>` tag, HTML-unescaped
- **year**: `Int64` *(Hive partition column)* Publication year, inferred from directory structure
- **month**: `Int64` *(Hive partition column)* Publication month, inferred from directory structure
