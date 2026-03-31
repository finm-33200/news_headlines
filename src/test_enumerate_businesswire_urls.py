from pathlib import Path

import polars as pl

from enumerate_businesswire_urls import (
    _filter_english_articles,
    _inventory_file_summary,
    _inventory_spot_check,
    _inventory_status_summary,
)


def test_inventory_spot_check_summarizes_slug_coverage():
    df = pl.DataFrame(
        {
            "source_url": [
                "https://www.businesswire.com/news/home/20240102000123/en/Acme-Launches-Product",
                "https://www.businesswire.com/news/home/20240102000456/en",
                "https://www.businesswire.com/news/home/20240102000789/en/Board-Approves-Merger",
            ],
            "wayback_timestamp": ["20240102120000", "20240102121000", "20240102122000"],
            "date": ["2024-01-02", "2024-01-02", "2024-01-02"],
            "headline_from_slug": [
                "Acme Launches Product",
                None,
                "Board Approves Merger",
            ],
        }
    )

    summary = _inventory_spot_check(df, sample_size=2)

    assert summary["total_urls"] == 3
    assert summary["slug_headline_count"] == 2
    assert summary["missing_slug_count"] == 1
    assert summary["slug_coverage_pct"] == 200 / 3
    assert summary["slug_samples"] == [
        {
            "source_url": "https://www.businesswire.com/news/home/20240102000123/en/Acme-Launches-Product",
            "headline_from_slug": "Acme Launches Product",
        },
        {
            "source_url": "https://www.businesswire.com/news/home/20240102000789/en/Board-Approves-Merger",
            "headline_from_slug": "Board Approves Merger",
        },
    ]
    assert summary["missing_slug_samples"] == [
        "https://www.businesswire.com/news/home/20240102000456/en"
    ]


def test_filter_english_articles_prefers_slug_variants_and_skips_non_articles():
    rows = [
        [
            "20240102120000",
            "https://www.businesswire.com/news/home/20240102000123/en",
            "200",
        ],
        [
            "20240102120100",
            "https://www.businesswire.com/news/home/20240102000123/en/Acme-Launches-Product",
            "200",
        ],
        [
            "20240102120200",
            "https://www.businesswire.com/news/home/20240102000456/en/de/",
            "200",
        ],
        [
            "20240102120300",
            "https://www.businesswire.com/news/home/20240102000789/fr/Acme-Lance-Produit",
            "200",
        ],
        [
            "20240102120400",
            "https://www.businesswire.com/news/home/20240102000999/en/Board-Approves-Merger",
            "200",
        ],
        [
            "20240102120500",
            "https://www.businesswire.com/news/home/20240102001000/en/logo.png",
            "200",
        ],
    ]

    articles = _filter_english_articles(rows, "20240102")

    assert articles == [
        {
            "source_url": "https://www.businesswire.com/news/home/20240102000123/en/Acme-Launches-Product",
            "wayback_timestamp": "20240102120100",
            "date": "2024-01-02",
            "headline_from_slug": "Acme Launches Product",
        },
        {
            "source_url": "https://www.businesswire.com/news/home/20240102000999/en/Board-Approves-Merger",
            "wayback_timestamp": "20240102120400",
            "date": "2024-01-02",
            "headline_from_slug": "Board Approves Merger",
        },
    ]


def test_inventory_file_summary_treats_legacy_inventory_as_wayback_only():
    legacy_df = pl.DataFrame(
        {
            "source_url": [
                "https://www.businesswire.com/news/home/20240102000123/en",
                "https://www.businesswire.com/news/home/20240102000456/en",
            ],
            "wayback_timestamp": ["20240102120000", "20240102121000"],
            "date": ["2024-01-02", "2024-01-02"],
        }
    )

    summary = _inventory_file_summary(legacy_df)

    assert summary == {
        "total_urls": 2,
        "has_slug_column": False,
        "slug_headline_count": 0,
        "missing_slug_count": 2,
    }


def test_inventory_status_summary_reports_slug_coverage_and_legacy_days(tmp_path: Path):
    slug_ready_path = tmp_path / "year=2024" / "month=01" / "day=02" / "urls.parquet"
    slug_ready_path.parent.mkdir(parents=True)
    pl.DataFrame(
        {
            "source_url": [
                "https://www.businesswire.com/news/home/20240102000123/en/Acme-Launches-Product",
                "https://www.businesswire.com/news/home/20240102000456/en",
            ],
            "wayback_timestamp": ["20240102120000", "20240102121000"],
            "date": ["2024-01-02", "2024-01-02"],
            "headline_from_slug": ["Acme Launches Product", None],
        }
    ).write_parquet(slug_ready_path)

    legacy_path = tmp_path / "year=2024" / "month=01" / "day=03" / "urls.parquet"
    legacy_path.parent.mkdir(parents=True)
    pl.DataFrame(
        {
            "source_url": ["https://www.businesswire.com/news/home/20240103000123/en"],
            "wayback_timestamp": ["20240103120000"],
            "date": ["2024-01-03"],
        }
    ).write_parquet(legacy_path)

    summary = _inventory_status_summary(tmp_path)

    assert summary["day_files"] == 2
    assert summary["total_urls"] == 3
    assert summary["slug_headline_count"] == 1
    assert summary["missing_slug_count"] == 2
    assert summary["slug_ready_day_files"] == 1
    assert summary["legacy_day_files"] == 1
    assert summary["legacy_dates"] == ["2024-01-03"]
    assert summary["date_range"] == ("2024-01-02", "2024-01-03")
    assert summary["slug_coverage_pct"] == 100 / 3
