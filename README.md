News Headlines
==============

## About this project

Curate news headlines data from various sources

## Quick Start

The quickest way to run code in this repo is to use the following steps.


First, create a virtual environment and activate it:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```
Then install the dependencies:
```bash
pip install -r requirements.txt
```

Finally, run the project tasks:
```bash
doit
```
And that's it!


### BigQuery Setup (GDELT Data)

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

**4. Install Python dependencies** (if not already done):
```bash
pip install -r requirements.txt
```

### Full Newswire Pull

The default `doit` pipeline pulls a single sample month of newswire headlines.
To crawl the full history (2020 to present) across PR Newswire, Business Wire,
and GlobeNewswire, run the script directly:

```bash
cd src
python pull_free_newswires.py --full
```

This is a long-running crawl (days/weeks depending on network speed). It is
**resumable** — completed days are saved as daily Hive-partitioned parquets and
skipped on re-run. Safe to `Ctrl+C` and restart.

Common options:
```bash
# Start from a specific date instead of 2020-01-01
python pull_free_newswires.py --full --start 2023-01-01

# Pull a single specific month
python pull_free_newswires.py --month 2024-06

# Check crawl progress
python pull_free_newswires.py --status
```

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

