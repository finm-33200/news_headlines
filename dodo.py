"""Run or update the project. This file uses the `doit` Python package. It works
like a Makefile, but is Python-based

"""

#######################################
## Configuration and Helpers for PyDoit
#######################################
## Make sure the src folder is in the path
import sys

sys.path.insert(1, "./src/")

import shutil
from os import environ
from pathlib import Path

from settings import config

DOIT_CONFIG = {"backend": "sqlite3", "dep_file": "./.doit-db.sqlite"}


BASE_DIR = config("BASE_DIR")
DATA_DIR = config("DATA_DIR")
MANUAL_DATA_DIR = config("MANUAL_DATA_DIR")
OUTPUT_DIR = config("OUTPUT_DIR")


def _cast_bool(val):
    if isinstance(val, bool):
        return val
    return str(val).lower() in ("true", "1", "yes")


USE_CACHED_SCRAPES = config("USE_CACHED_SCRAPES", cast=_cast_bool)

## Helpers for handling Jupyter Notebook tasks
environ["PYDEVD_DISABLE_FILE_VALIDATION"] = "1"


# fmt: off
## Helper functions for automatic execution of Jupyter notebooks
def jupyter_execute_notebook(notebook_path):
    return f"jupyter nbconvert --execute --to notebook --ClearMetadataPreprocessor.enabled=True --inplace {notebook_path}"
def jupyter_to_html(notebook_path, output_dir=OUTPUT_DIR):
    return f"jupyter nbconvert --to html --output-dir={output_dir} {notebook_path}"
def jupyter_to_md(notebook_path, output_dir=OUTPUT_DIR):
    """Requires jupytext"""
    return f"jupytext --to markdown --output-dir={output_dir} {notebook_path}"
def jupyter_clear_output(notebook_path):
    """Clear the output of a notebook"""
    return f"jupyter nbconvert --ClearOutputPreprocessor.enabled=True --ClearMetadataPreprocessor.enabled=True --inplace {notebook_path}"
# fmt: on


def mv(from_path, to_path):
    """Create a Python action for moving a file to a folder."""

    def _mv():
        src = Path(from_path)
        dst = Path(to_path)
        dst.mkdir(parents=True, exist_ok=True)
        final = dst / src.name
        if final.exists():
            final.unlink()
        shutil.move(str(src), str(dst))

    return _mv


def copy_file(origin_path, destination_path, mkdir=True):
    """Create a Python action for copying a file."""

    def _copy_file():
        origin = Path(origin_path)
        dest = Path(destination_path)
        if mkdir:
            dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(origin, dest)

    return _copy_file


##################################
## Begin rest of PyDoit tasks here
##################################


def task_config():
    """Create empty directories for data and output if they don't exist"""
    return {
        "actions": ["python ./src/settings.py"],
        "targets": [DATA_DIR, OUTPUT_DIR],
        "file_dep": ["./src/settings.py"],
        "clean": [],
    }


def task_pull():
    """Pull data from external sources"""
    yield {
        "name": "ravenpack",
        "doc": "Pull RavenPack DJ Press Release headlines from WRDS",
        "actions": [
            "python ./src/settings.py",
            "python ./src/pull_ravenpack.py",
        ],
        "targets": [DATA_DIR / "ravenpack_djpr.parquet"],
        "file_dep": ["./src/settings.py", "./src/pull_ravenpack.py"],
        "clean": [],
        "verbosity": 2,
    }

    yield {
        "name": "sp500_constituents",
        "doc": "Pull historical S&P 500 constituents from WRDS",
        "actions": [
            "python ./src/settings.py",
            "python ./src/pull_sp500_constituents.py",
        ],
        "targets": [
            DATA_DIR / "sp500_constituents.parquet",
            DATA_DIR / "sp500_names_lookup.parquet",
        ],
        "file_dep": ["./src/settings.py", "./src/pull_sp500_constituents.py"],
        "clean": [],
        "verbosity": 2,
    }

    if USE_CACHED_SCRAPES:
        yield {
            "name": "cached_scrapes",
            "doc": "Download pre-scraped GDELT and newswire data from Dropbox",
            "actions": [
                "python ./src/settings.py",
                "python ./src/pull_cached_scrapes.py",
            ],
            "targets": [
                DATA_DIR
                / "gdelt_sp500_headlines"
                / "year=2025"
                / "month=01"
                / "data.parquet",
                DATA_DIR
                / "newswire_headlines"
                / "source=prnewswire"
                / "year=2025"
                / "month=01"
                / "day=01"
                / "data.parquet",
            ],
            "file_dep": ["./src/settings.py", "./src/pull_cached_scrapes.py"],
            "clean": [],
        }
    else:
        yield {
            "name": "gdelt_sp500_sample",
            "doc": "Pull GDELT GKG headlines filtered to S&P 500 companies (sample month)",
            "actions": [
                "python ./src/settings.py",
                "python ./src/pull_gdelt_sp500_headlines.py",
            ],
            "targets": [
                DATA_DIR
                / "gdelt_sp500_headlines"
                / "year=2025"
                / "month=01"
                / "data.parquet"
            ],
            "file_dep": [
                "./src/settings.py",
                "./src/pull_gdelt_sp500_headlines.py",
                DATA_DIR / "sp500_names_lookup.parquet",
            ],
            "clean": [],
        }

        yield {
            "name": "newswire_sample",
            "doc": "Pull free newswire headlines for sample month via sitemap crawling",
            "actions": [
                "python ./src/settings.py",
                "python ./src/pull_free_newswires.py",
            ],
            "targets": [
                DATA_DIR
                / "newswire_headlines"
                / "source=prnewswire"
                / "year=2025"
                / "month=01"
                / "day=01"
                / "data.parquet",
            ],
            "file_dep": [
                "./src/settings.py",
                "./src/pull_free_newswires.py",
            ],
            "clean": [],
        }


def task_create_crosswalk():
    """Fuzzy-match crosswalks between scraped headlines and RavenPack"""
    # Newswire crosswalk
    nw_task_dep = (
        ["pull:cached_scrapes"] if USE_CACHED_SCRAPES else ["pull:newswire_sample"]
    )
    yield {
        "name": "newswire",
        "doc": "Fuzzy-match newswire headlines to RavenPack",
        "actions": [
            "python ./src/settings.py",
            # Full rebuild: source coverage can expand over time (e.g. Business Wire),
            # so resume-only chunk reuse can silently omit newly added sources.
            "python ./src/create_newswire_ravenpack_crosswalk.py",
        ],
        "targets": [DATA_DIR / "newswire_ravenpack_crosswalk.parquet"],
        "file_dep": [
            "./src/settings.py",
            "./src/create_newswire_ravenpack_crosswalk.py",
            DATA_DIR / "ravenpack_djpr.parquet",
        ],
        "task_dep": nw_task_dep,
        "clean": [],
        "verbosity": 2,
    }

    # GDELT crosswalk
    gdelt_task_dep = (
        ["pull:cached_scrapes"] if USE_CACHED_SCRAPES else ["pull:gdelt_sp500_sample"]
    )
    yield {
        "name": "gdelt",
        "doc": "Fuzzy-match GDELT S&P 500 headlines to RavenPack",
        "actions": [
            "python ./src/settings.py",
            "python ./src/create_gdelt_ravenpack_crosswalk.py",
        ],
        "targets": [DATA_DIR / "gdelt_ravenpack_crosswalk.parquet"],
        "file_dep": [
            "./src/settings.py",
            "./src/create_gdelt_ravenpack_crosswalk.py",
            "./src/create_newswire_ravenpack_crosswalk.py",  # imports normalize_headline
            DATA_DIR / "ravenpack_djpr.parquet",
        ],
        "task_dep": gdelt_task_dep,
        "clean": [],
        "verbosity": 2,
    }


def task_create_merged_dataset():
    """Merge scraped headlines with full RavenPack metadata"""
    return {
        "actions": [
            "python ./src/settings.py",
            "python ./src/create_scraped_headlines_with_rp_metadata.py",
        ],
        "targets": [DATA_DIR / "scraped_headlines_with_rp_metadata.parquet"],
        "file_dep": [
            "./src/settings.py",
            "./src/create_scraped_headlines_with_rp_metadata.py",
            DATA_DIR / "ravenpack_djpr.parquet",
            DATA_DIR / "newswire_ravenpack_crosswalk.parquet",
            DATA_DIR / "gdelt_ravenpack_crosswalk.parquet",
        ],
        "clean": [],
    }


notebook_tasks = {
    "01_data_sources_overview_ipynb": {
        "path": "./src/01_data_sources_overview_ipynb.py",
        "file_dep": [
            DATA_DIR / "ravenpack_djpr.parquet",
            DATA_DIR
            / "gdelt_sp500_headlines"
            / "year=2025"
            / "month=01"
            / "data.parquet",
            DATA_DIR / "sp500_constituents.parquet",
            DATA_DIR
            / "newswire_headlines"
            / "source=prnewswire"
            / "year=2025"
            / "month=01"
            / "day=01"
            / "data.parquet",
            DATA_DIR / "newswire_ravenpack_crosswalk.parquet",
        ],
        "targets": [],
    },
    "02_gdelt_sp500_filtering_ipynb": {
        "path": "./src/02_gdelt_sp500_filtering_ipynb.py",
        "file_dep": [
            DATA_DIR
            / "gdelt_sp500_headlines"
            / "year=2025"
            / "month=01"
            / "data.parquet",
            DATA_DIR / "sp500_names_lookup.parquet",
            DATA_DIR / "ravenpack_djpr.parquet",
        ],
        "targets": [],
    },
    "03_crosswalk_quality_ipynb": {
        "path": "./src/03_crosswalk_quality_ipynb.py",
        "file_dep": [
            DATA_DIR / "newswire_ravenpack_crosswalk.parquet",
            DATA_DIR / "ravenpack_djpr.parquet",
            DATA_DIR
            / "newswire_headlines"
            / "source=prnewswire"
            / "year=2025"
            / "month=01"
            / "day=01"
            / "data.parquet",
        ],
        "targets": [],
    },
}


# fmt: off
def task_run_notebooks():
    """Preps the notebooks for presentation format.
    Execute notebooks if the script version of it has been changed.
    """
    for notebook in notebook_tasks.keys():
        pyfile_path = Path(notebook_tasks[notebook]["path"])
        notebook_path = pyfile_path.with_suffix(".ipynb")
        yield {
            "name": notebook,
            "actions": [
                """python -c "import sys; from datetime import datetime; print(f'Start """ + notebook + """: {datetime.now()}', file=sys.stderr)" """,
                f"jupytext --to notebook --output {notebook_path} {pyfile_path}",
                jupyter_execute_notebook(notebook_path),
                jupyter_to_html(notebook_path),
                mv(notebook_path, OUTPUT_DIR),
                """python -c "import sys; from datetime import datetime; print(f'End """ + notebook + """: {datetime.now()}', file=sys.stderr)" """,
            ],
            "file_dep": [
                pyfile_path,
                *notebook_tasks[notebook]["file_dep"],
            ],
            "targets": [
                OUTPUT_DIR / f"{notebook}.html",
                *notebook_tasks[notebook]["targets"],
            ],
            "clean": True,
        }
# fmt: on

sphinx_targets = [
    "./docs/index.html",
]


def task_save_coverage_chart():
    """Generate the Plotly HTML coverage chart for the chartbook"""
    return {
        "actions": [
            "python ./src/save_coverage_chart.py",
        ],
        "targets": [OUTPUT_DIR / "rp_match_rate_coverage.html"],
        "file_dep": [
            "./src/save_coverage_chart.py",
            DATA_DIR / "newswire_ravenpack_crosswalk.parquet",
            DATA_DIR / "gdelt_ravenpack_crosswalk.parquet",
            DATA_DIR / "ravenpack_djpr.parquet",
        ],
        "clean": True,
    }


def task_build_chartbook_site():
    """Compile Sphinx Docs"""
    notebook_scripts = [
        Path(notebook_tasks[notebook]["path"]) for notebook in notebook_tasks.keys()
    ]
    file_dep = [
        "./README.md",
        "./chartbook.toml",
        *notebook_scripts,
    ]

    return {
        "actions": [
            "chartbook build -f",
        ],  # Use docs as build destination
        "targets": sphinx_targets,
        "file_dep": file_dep,
        "task_dep": [
            "run_notebooks",
            "save_coverage_chart",
        ],
        "clean": True,
    }


def task_run_pytest():
    """Run pytest and save results to OUTPUT_DIR"""
    src_py_files = list(Path("./src").glob("*.py"))
    test_output = OUTPUT_DIR / "pytest_results.xml"

    def run_pytest():
        import subprocess

        result = subprocess.run(
            ["pytest", f"--junitxml={test_output}"],
        )
        if result.returncode != 0:
            # Remove the XML so doit won't consider the target up-to-date
            Path(test_output).unlink(missing_ok=True)
            raise RuntimeError(f"pytest failed with exit code {result.returncode}")

    return {
        "actions": [run_pytest],
        "targets": [test_output],
        "file_dep": src_py_files,
        "clean": True,
        "verbosity": 2,
    }
