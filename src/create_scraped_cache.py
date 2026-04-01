"""Create a ZIP archive of all scraped headline data for distribution.

Bundles ``newswire_headlines/`` and ``gdelt_headlines/`` from DATA_DIR
into a single ZIP file that can be uploaded to Google Drive or Dropbox.
Students download this ZIP via ``pull_cached_scrapes.py``.

Usage
-----
Standalone::

    python src/create_scraped_cache.py             # create the cache ZIP
    python src/create_scraped_cache.py --status     # show what would be included
    python src/create_scraped_cache.py --output /tmp/cache.zip  # custom path

Called by dodo.py when USE_CACHED_SCRAPES=0 (instructor mode).
"""

import argparse
import sys
import zipfile
from pathlib import Path

from settings import config

CACHE_DIRS = [
    "newswire_headlines",
    "gdelt_headlines",
]


def _collect_parquets(data_dir: Path) -> list[tuple[Path, str]]:
    """Return list of (absolute_path, arcname) for all parquet files to include."""
    files = []
    for dirname in CACHE_DIRS:
        src = data_dir / dirname
        if not src.is_dir():
            continue
        for pq in sorted(src.rglob("*.parquet")):
            arcname = str(pq.relative_to(data_dir))
            files.append((pq, arcname))
    return files


def print_status(data_dir: Path) -> None:
    """Show what data would be included in the cache ZIP."""
    print(f"DATA_DIR: {data_dir}\n")
    files = _collect_parquets(data_dir)
    if not files:
        print("  No parquet files found in expected directories.")
        return

    # Group by top-level directory and source partition
    from collections import Counter

    sources = Counter()
    total_bytes = 0
    for abs_path, arcname in files:
        top = arcname.split("/")[0]
        # Try to extract source= partition if present
        parts = arcname.split("/")
        source_part = next((p for p in parts if p.startswith("source=")), None)
        key = f"{top}/{source_part}" if source_part else top
        sources[key] += 1
        total_bytes += abs_path.stat().st_size

    for key in sorted(sources):
        print(f"  {key}  — {sources[key]} parquet file(s)")
    print(
        f"\n  Total: {len(files)} files, {total_bytes / 1024 / 1024:.1f} MB uncompressed"
    )


def create_cache(data_dir: Path, output_path: Path) -> Path:
    """Create a ZIP archive of scraped headline data.

    Parameters
    ----------
    data_dir : Path
        The DATA_DIR containing ``newswire_headlines/`` and
        ``gdelt_headlines/`` subdirectories.
    output_path : Path
        Where to write the ZIP file.

    Returns
    -------
    Path
        The path to the created ZIP file.
    """
    files = _collect_parquets(data_dir)
    if not files:
        print("ERROR: No parquet files found to bundle.", file=sys.stderr)
        sys.exit(1)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Creating cache ZIP: {output_path}")
    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for abs_path, arcname in files:
            zf.write(abs_path, arcname)

    size_mb = output_path.stat().st_size / 1024 / 1024
    print(f"  {len(files)} parquet files archived ({size_mb:.1f} MB compressed)")
    return output_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output ZIP path (default: DATA_DIR/scraped_headlines_cache.zip)",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show what data would be included without creating the ZIP",
    )
    args = parser.parse_args()

    data_dir = Path(config("DATA_DIR"))

    if args.status:
        print_status(data_dir)
    else:
        out = args.output or (data_dir / "scraped_headlines_cache.zip")
        create_cache(data_dir, out)
