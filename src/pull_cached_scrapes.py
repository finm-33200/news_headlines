"""Download pre-scraped GDELT and newswire headline data from Dropbox.

The instructor maintains a Dropbox folder with the full archive of scraped
headlines. This script downloads that archive as a ZIP and extracts it into
DATA_DIR, giving students the same hive-partitioned parquet files that the
long-running scraping pipeline would produce.

Usage
-----
Standalone::

    python src/pull_cached_scrapes.py          # download if not already present
    python src/pull_cached_scrapes.py --force   # re-download even if present
    python src/pull_cached_scrapes.py --status  # check what's already downloaded

Called by dodo.py when USE_CACHED_SCRAPES=1 (the default).
"""

import argparse
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path

import requests

from settings import config

# Dropbox shared folder — dl=1 triggers a ZIP download of the entire folder.
DROPBOX_FOLDER_URL = (
    "https://www.dropbox.com/scl/fo/xrwngh9remblw2s4brv4w/"
    "APqvc68oGenbeyB38CYrGvg?rlkey=qsc2xlfxjl2o4vuwjib3ayzob&dl=1"
)

# Directories we expect to find inside the ZIP (after removing any wrapper).
EXPECTED_DIRS = [
    "gdelt_sp500_headlines",
    "newswire_headlines",
]

CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB chunks for streaming download


def _targets_exist(data_dir: Path) -> bool:
    """Return True if all expected directories exist and are non-empty."""
    for name in EXPECTED_DIRS:
        d = data_dir / name
        if not d.is_dir() or not any(d.iterdir()):
            return False
    return True


def _print_status(data_dir: Path) -> None:
    """Print what cached data is present in DATA_DIR."""
    print(f"DATA_DIR: {data_dir}\n")
    for name in EXPECTED_DIRS:
        d = data_dir / name
        if d.is_dir():
            parquets = list(d.rglob("*.parquet"))
            print(f"  {name}/  — {len(parquets)} parquet file(s)")
        else:
            print(f"  {name}/  — NOT FOUND")


def _download_zip(dest: Path) -> None:
    """Stream-download the Dropbox ZIP to *dest*."""
    print(f"Downloading cached scrapes from Dropbox...")
    resp = requests.get(DROPBOX_FOLDER_URL, stream=True, timeout=60)
    resp.raise_for_status()

    total = int(resp.headers.get("content-length", 0))
    downloaded = 0
    with open(dest, "wb") as f:
        for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
            f.write(chunk)
            downloaded += len(chunk)
            mb = downloaded / (1024 * 1024)
            if total:
                pct = downloaded / total * 100
                print(f"\r  {mb:,.0f} MB / {total / 1024 / 1024:,.0f} MB ({pct:.0f}%)", end="", flush=True)
            else:
                print(f"\r  {mb:,.0f} MB downloaded", end="", flush=True)
    print()  # newline after progress


def _extract_zip(zip_path: Path, data_dir: Path) -> None:
    """Extract the ZIP into *data_dir*, handling Dropbox's wrapper directory."""
    print(f"Extracting to {data_dir} ...")
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(tmpdir)

        # Dropbox may wrap contents in a single top-level directory.
        top_level = [p for p in tmpdir.iterdir() if not p.name.startswith(".")]
        if len(top_level) == 1 and top_level[0].is_dir():
            # Check if the expected dirs are inside the wrapper
            wrapper = top_level[0]
            if any((wrapper / name).exists() for name in EXPECTED_DIRS):
                source = wrapper
            else:
                source = tmpdir
        else:
            source = tmpdir

        # Move each expected directory into DATA_DIR
        for name in EXPECTED_DIRS:
            src = source / name
            dst = data_dir / name
            if src.exists():
                if dst.exists():
                    shutil.rmtree(dst)
                shutil.move(str(src), str(dst))
                print(f"  {name}/  — extracted")
            else:
                print(f"  WARNING: {name}/ not found in ZIP")

        # Move any other parquet files at the root level (e.g., crosswalk)
        for f in source.glob("*.parquet"):
            dst = data_dir / f.name
            shutil.move(str(f), str(dst))
            print(f"  {f.name}  — extracted")


def download_cached_scrapes(data_dir: Path = None, force: bool = False) -> None:
    """Download and extract cached scrapes into *data_dir*.

    Skips download if target directories already exist, unless *force* is True.
    """
    if data_dir is None:
        data_dir = config("DATA_DIR")
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    if not force and _targets_exist(data_dir):
        print("Cached scrapes already present — skipping download.")
        print("  (use --force to re-download)")
        return

    zip_path = data_dir / "_cached_scrapes.zip"
    try:
        _download_zip(zip_path)
        _extract_zip(zip_path, data_dir)
    finally:
        # Clean up the ZIP regardless of success/failure
        if zip_path.exists():
            zip_path.unlink()
            print("  Cleaned up temporary ZIP file.")

    # Verify extraction
    if _targets_exist(data_dir):
        print("\nCached scrapes downloaded and extracted successfully.")
    else:
        missing = [n for n in EXPECTED_DIRS if not (data_dir / n).is_dir()]
        print(f"\nWARNING: Missing directories after extraction: {missing}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true", help="Re-download even if data exists")
    parser.add_argument("--status", action="store_true", help="Show what cached data is present")
    args = parser.parse_args()

    if args.status:
        _print_status(config("DATA_DIR"))
    else:
        download_cached_scrapes(force=args.force)
