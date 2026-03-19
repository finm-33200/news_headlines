
## Description

This dataframe is a crosswalk linking GDELT S&P 500 headlines to RavenPack DJ
Press Release headlines via date-blocked fuzzy matching. For each calendar date,
every GDELT headline is compared against all RavenPack headlines from the same
date using `token_sort_ratio` from rapidfuzz. Only high-quality matches (score
>= 80) are retained.

GDELT and RavenPack draw from different source ecosystems (GDELT covers the
open web while RavenPack is primarily wire services), so the expected match
rate is lower than the newswire crosswalk (~7% overlap vs ~95%).

## Coverage Note

Although the crosswalk date range may extend back to 2015, **reliable combined
coverage (newswire + GDELT) begins around late 2019**. Before that, scraped
headline volume is sparse relative to RavenPack, so the daily match rate is
very low. Analyses that depend on representative headline coverage should
filter to dates from late 2019 onward.

## Data Dictionary

- **date**: `Date` Calendar date of the matched headlines
- **gdelt_source_url**: `Utf8` URL of the GDELT article
- **gdelt_headline**: `Utf8` Original headline text from GDELT (extracted from GKG PAGE_TITLE)
- **gdelt_source_name**: `Utf8` Domain name of the news source as reported by GDELT (e.g., "nytimes.com")
- **gdelt_matched_company**: `Utf8` Company name from the CRSP stocknames table that triggered the S&P 500 match
- **gdelt_permno**: `Int64` CRSP permanent security identifier
- **gdelt_ticker**: `Utf8` Stock ticker symbol
- **rp_story_id**: `Utf8` RavenPack unique story identifier (hexadecimal hash)
- **rp_entity_id**: `Utf8` RavenPack unique entity/company identifier
- **rp_entity_name**: `Utf8` Company name as recorded by RavenPack
- **rp_headline**: `Utf8` Original headline text from RavenPack
- **rp_source_name**: `Utf8` News source as classified by RavenPack
- **fuzzy_score**: `Float64` Best-match fuzzy similarity score (token_sort_ratio, range 80-100)
