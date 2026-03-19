
## Description

This dataframe is a crosswalk linking free newswire press-release headlines
(scraped from PR Newswire) to RavenPack DJ
Press Release headlines via date-blocked fuzzy matching. For each calendar date,
every newswire headline is compared against all RavenPack headlines from the same
date using `token_sort_ratio` from rapidfuzz. Only high-quality matches (score
>= 80) are retained.

The crosswalk enables transferring RavenPack metadata (entity identifiers,
company names, sentiment scores) onto the free newswire headlines, which lack
company identifiers in their raw form.

## Coverage Note

Although the crosswalk date range extends back to 2010, **reliable coverage
begins around late 2019**. Before that, newswire scrape volume is sparse
relative to RavenPack, so the daily match rate is very low. Analyses that
depend on representative headline coverage should filter to dates from
late 2019 onward.

## Data Dictionary

- **date**: `Date` Calendar date of the matched headlines
- **nw_source_url**: `Utf8` URL of the newswire press release (serves as unique row identifier)
- **nw_headline**: `Utf8` Original headline text from the newswire source
- **nw_source**: `Utf8` Wire service the headline was scraped from (e.g., "prnewswire")
- **rp_story_id**: `Utf8` RavenPack unique story identifier (hexadecimal hash)
- **rp_entity_id**: `Utf8` RavenPack unique entity/company identifier
- **rp_entity_name**: `Utf8` Company name as recorded by RavenPack (e.g., "Apple Inc.", "Ford Motor Co.")
- **rp_headline**: `Utf8` Original headline text from RavenPack
- **rp_source_name**: `Utf8` News source as classified by RavenPack (e.g., "PR Newswire", "Business Wire", "Dow Jones Newswires")
- **fuzzy_score**: `Float64` Best-match fuzzy similarity score (token_sort_ratio, range 80-100)
