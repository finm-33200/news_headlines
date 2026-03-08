
## Description

RavenPack Dow Jones Press Release headlines for US companies, pulled from WRDS
(Wharton Research Data Services). Tables are partitioned by year on WRDS
(`ravenpack_dj.rpa_djpr_equities_YYYY`), and the pipeline pulls from 2000
through the present.

Key filters (following Chen, Kelly, and Xiu 2022):
- `entity_type = 'COMP'` (companies only)
- `country_code = 'US'` (US companies only)
- `relevance >= 90` (high relevance to the matched entity)
- Single-firm stories only: each `provider_story_id` must reference exactly
  one distinct `rp_entity_id`, ensuring unambiguous company attribution

## Data Dictionary

- **timestamp_utc**: `Datetime(ns)` UTC timestamp of the story
- **rp_story_id**: `String` RavenPack unique story identifier (hexadecimal hash)
- **rp_entity_id**: `String` RavenPack unique entity/company identifier
- **entity_type**: `String` Entity type classification (always "COMP" in this dataset)
- **entity_name**: `String` Company name as recorded by RavenPack (e.g., "Apple Inc.", "Ford Motor Co.")
- **country_code**: `String` ISO country code (always "US" in this dataset)
- **relevance**: `Float64` Relevance score (0-100) indicating how central the entity is to the story. Filtered to >= 90.
- **event_sentiment_score**: `Float64` RavenPack event-level sentiment score (-1.0 to +1.0)
- **event_relevance**: `Float64` Event relevance score (0-100) measuring how relevant the detected event is
- **event_similarity_key**: `String` Key grouping similar events together for novelty detection
- **event_similarity_days**: `Float64` Number of days since the most recent similar event (for the same entity and event type)
- **topic**: `String` RavenPack topic classification (e.g., "EARNINGS", "PARTNERSHIPS")
- **rp_group**: `String` RavenPack event group (broad category, e.g., "revenue", "labor-issues")
- **rp_type**: `String` RavenPack event type (specific category within group)
- **sub_type**: `String` Event sub-type for finer-grained classification
- **property**: `String` Property classification of the event
- **fact_level**: `String` Whether the story reports a fact, forecast, or opinion
- **category**: `String` Category classification of the story
- **news_type**: `String` Type of news item (e.g., "PRESS-RELEASE", "ARTICLE")
- **rp_source_id**: `String` RavenPack identifier for the news source
- **source_name**: `String` Name of the news source (e.g., "PR Newswire", "Business Wire", "Dow Jones Newswires")
- **provider_id**: `String` Content provider identifier
- **provider_story_id**: `String` Provider's unique story identifier (used for single-firm filtering)
- **headline**: `String` Headline text of the story
- **css**: `Float64` Composite Sentiment Score, RavenPack's aggregate sentiment measure (-1.0 to +1.0) combining multiple sentiment signals
