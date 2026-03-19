
## Description

Merged dataset joining independently scraped headlines (from newswire and GDELT
sources) with full RavenPack DJ Press Release metadata. This is the primary
downstream-ready dataset for NLP and sentiment analysis.

**Key design principle:** The `headline` column contains text scraped from free
sources (PR Newswire, GDELT) and is safe to send to external APIs like OpenAI.
The `rp_headline` column contains the original RavenPack headline text, retained
for quality assurance and match verification.

The dataset is anchored on `ravenpack_djpr`: every RavenPack row is present,
with scraped headline columns (`headline`, `headline_source`, `headline_source_url`,
`fuzzy_score`) populated only when a fuzzy match exists. Rows without a scraped
match have null values in these columns.

When both a newswire and GDELT headline match the same RavenPack story, the
match with the higher fuzzy score is kept.

## Data Dictionary

### Scraped headline columns

- **headline**: `Utf8` Scraped headline text from newswire or GDELT — use this column for downstream NLP. Null if no scraped headline matched this RavenPack story.
- **headline_source**: `Utf8` Provenance of the scraped headline: `"newswire"` or `"gdelt"`. Null if unmatched.
- **headline_source_url**: `Utf8` URL of the scraped article. Null if unmatched.
- **fuzzy_score**: `Float64` Fuzzy match quality score (token_sort_ratio, range 80-100). Null if unmatched.

### RavenPack metadata columns

- **timestamp_utc**: `Datetime(ns)` UTC timestamp of the story
- **rp_story_id**: `String` RavenPack unique story identifier
- **rp_entity_id**: `String` RavenPack unique entity/company identifier
- **entity_type**: `String` Entity type (always "COMP")
- **entity_name**: `String` Company name as recorded by RavenPack
- **country_code**: `String` ISO country code (always "US")
- **relevance**: `Float64` Relevance score (filtered >= 90)
- **event_sentiment_score**: `Float64` Event-level sentiment (-1.0 to +1.0)
- **event_relevance**: `Float64` Event relevance score (0-100)
- **event_similarity_key**: `String` Groups similar events for novelty detection
- **event_similarity_days**: `Float64` Days since most recent similar event
- **topic**: `String` Topic classification (e.g., "EARNINGS", "PARTNERSHIPS")
- **rp_group**: `String` Broad event category (e.g., "revenue", "labor-issues")
- **rp_type**: `String` Specific event type within group
- **sub_type**: `String` Finer-grained event classification
- **property**: `String` Event property classification
- **fact_level**: `String` Fact, forecast, or opinion
- **category**: `String` Story category
- **news_type**: `String` Type of news (e.g., "PRESS-RELEASE")
- **rp_source_id**: `String` RavenPack news source identifier
- **source_name**: `String` News source name
- **provider_id**: `String` Content provider identifier
- **provider_story_id**: `String` Provider's unique story identifier
- **rp_headline**: `String` Original RavenPack headline text (for reference/QA only)
- **css**: `Float64` Composite Sentiment Score (-1.0 to +1.0)
