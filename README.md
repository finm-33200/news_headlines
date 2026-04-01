News Headlines
==============

## About this project

This pipeline curates firm-level news headlines for S&P 500 companies from
multiple sources.

**The problem.** RavenPack provides best-in-class entity-tagged financial
headlines (with company identifiers, sentiment scores, and relevance ratings),
but its terms of use prohibit uploading headline text to LLMs like ChatGPT for
NLP analysis.

**The strategy.** Source headlines independently from free sources, then
fuzzy-match them to RavenPack to transfer its entity metadata. Only the
independently-sourced headlines are uploaded to LLMs — RavenPack's text stays
local, but its metadata travels via the crosswalk.

**Why scraped newswires work best.** RavenPack's content is ~95% wire services
(Dow Jones, PR Newswire, Business Wire, GlobeNewswire). GDELT crawls the open
web and shares only ~1–2% of its sources with RavenPack, yielding ~7%
fuzzy-match overlap. Scraping wire services directly produces much higher match
rates because we're matching the same underlying press releases.

**Pipeline outputs:**
- Hive-partitioned headline data lakes from RavenPack, GDELT, and scraped
  newswires
- A fuzzy-match crosswalk linking newswire headlines to RavenPack metadata
  (entity IDs, sentiment, topic classification)

## Quick Start

First, create a virtual environment and activate it:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```
Then install the dependencies:
```bash
pip install -r requirements.txt
```

Set up your `.env` file with WRDS credentials (copy from `.env.example`):
```bash
cp .env.example .env
# Edit .env and set WRDS_USERNAME=your_username
```

Finally, run the project tasks:
```bash
doit
```

By default, `doit` downloads the pre-scraped GDELT and newswire headline
archives from Dropbox instead of running the long scraping pipeline. This
is controlled by the `USE_CACHED_SCRAPES` setting (default: `True`). The
only credentials you need are for WRDS (RavenPack and S&P 500 constituents).

### Advanced: Scraping from Source

To run the GDELT and newswire scrapes yourself instead of using the cached
Dropbox data, set `USE_CACHED_SCRAPES=0` in your `.env` file. This requires
additional setup described below.

#### BigQuery Setup (GDELT Data)

The GDELT headline pull requires Google Cloud BigQuery access.

**1. Install the Google Cloud SDK:**

macOS:
```bash
brew install --cask google-cloud-sdk
```

Windows:
Download and run the installer from https://cloud.google.com/sdk/docs/install

Linux (Debian/Ubuntu):
```bash
sudo apt-get install apt-transport-https ca-certificates gnupg curl
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
sudo apt-get update && sudo apt-get install google-cloud-cli
```

**2. Authenticate:**
```bash
gcloud auth application-default login
```

**3. Set your GCP project in `.env`:**
```
GCP_PROJECT=your-gcp-project-id
```
The project must have the BigQuery API enabled. A free Google Cloud account
works — BigQuery's free tier includes 1 TB of queries per month, which is
more than sufficient for this project.

#### Full Newswire Pull

With `USE_CACHED_SCRAPES=0`, the default `doit` pipeline pulls a single sample
month of newswire headlines. To crawl the full history, run the script directly:

```bash
python ./src/pull_free_newswires.py --full
```

**Sources crawled:**

| Source | Method | Coverage |
|--------|--------|----------|
| PR Newswire | Gzipped monthly sitemaps + Wayback fallback | 2010–present |
| GlobeNewswire | Plain monthly sitemaps (sitemaps.globenewswire.com) | Apr 2023–present |

GlobeNewswire headlines are extracted directly from sitemap XML metadata
(fast-path — no per-page HTTP), yielding ~10K articles/month.

This is a long-running crawl (days/weeks depending on network speed). It is
**resumable** — completed days are saved as daily Hive-partitioned parquets and
skipped on re-run. Safe to `Ctrl+C` and restart.

Common options:
```bash
# Start from a specific date instead of 2010-01-01
python ./src/pull_free_newswires.py --full --start 2023-01-01

# Pull a single specific month
python ./src/pull_free_newswires.py --month 2024-06

# Check crawl progress
python ./src/pull_free_newswires.py --status
```

#### PR Newswire Backfill (2012–2019)

PR Newswire's live monthly sitemap path has a historical gap across
`2012-07` through `2019-12`. The current `_WAYBACK_TIMESTAMPS` block covers
79 of those 90 months; the remaining 11 months currently have no validated
Wayback archive. Use the standalone discovery script to validate or extend the
timestamp block:

```bash
# Validate the existing timestamp block without calling Wayback
python ./src/discover_wayback_timestamps.py --validate-existing

# Dry-run: see which months have Wayback archives
python ./src/discover_wayback_timestamps.py --dry-run

# Full discovery with local cache for resumability
python ./src/discover_wayback_timestamps.py --cache-dir _data/wayback_cache

# Discover a specific date range
python ./src/discover_wayback_timestamps.py --start 2015-01 --end 2016-12
```

The script outputs a Python dict literal to paste into `_WAYBACK_TIMESTAMPS` in
`pull_free_newswires.py`. After updating timestamps, re-run the backfill:

```bash
python ./src/pull_free_newswires.py --full --start 2012-07-01 --end 2020-01-01
```

#### Business Wire (Wayback Machine)

Business Wire's live sitemaps timeout, so headlines are collected via a
two-phase Wayback Machine pipeline. Both scripts are resumable and safe to
`Ctrl+C`.

**Phase 1 — URL enumeration** queries the Wayback CDX API for archived
Business Wire article URLs, one day at a time:

```bash
# Enumerate a date range (~3 hours for 20 years)
python ./src/enumerate_businesswire_urls.py --start 2004-01-01 --end 2026-04-01

# Check inventory progress
python ./src/enumerate_businesswire_urls.py --status

# Inspect slug-headline coverage for one day
python ./src/enumerate_businesswire_urls.py --spot-check 2024-01-02
```

**Phase 2 — headline extraction** reads the URL inventory produced above,
uses slug-derived headlines where available (~90–98% of URLs), and fetches
the remaining pages from Wayback:

```bash
# Fetch headlines for enumerated URLs
python ./src/fetch_businesswire_headlines.py --start 2024-01-01 --end 2024-02-01

# Check progress
python ./src/fetch_businesswire_headlines.py --status
```

Output lands in `newswire_headlines/source=businesswire/...` and is
automatically picked up by the crosswalk.

### Formatting

This project uses [Ruff](https://docs.astral.sh/ruff/) for linting and formatting Python code.

```bash
# Auto-fix linting issues (e.g., unused imports, undefined names)
ruff check . --fix

# Format code (consistent style, spacing, line length)
ruff format .

# Sort imports, then fix linting issues, then format
ruff format . && ruff check --select I --fix . && ruff check --fix .
```
