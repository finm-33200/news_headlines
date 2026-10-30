from pathlib import Path

import pull_free_newswires as module
from pull_free_newswires import GlobeNewswireScraper, NewswireCaScraper


def test_newswireca_headline_from_slug_decodes_encoded_characters():
    url = (
        "https://www.newswire.ca/news-releases/"
        "acme-r-d-%26-innovation-team-wins-canada-s-top-r%26d-award-829648686.html"
    )

    headline = NewswireCaScraper._headline_from_slug(url)

    assert headline == "Acme R D & Innovation Team Wins Canada S Top R&D Award"


def test_newswireca_parse_entries_from_xml_uses_slug_and_lastmod():
    xml_bytes = b"""
    <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
      <url>
        <loc>https://www.newswire.ca/news-releases/acme-launches-new-platform-829648686.html</loc>
        <lastmod>2026-03-29T14:00:00Z</lastmod>
      </url>
      <url>
        <loc>https://www.newswire.ca/news-releases/board-approves-r-d-plan-%26-budget-829648687.html?foo=bar</loc>
        <lastmod>2026-03-30T09:15:00Z</lastmod>
      </url>
      <url>
        <loc>https://www.newswire.ca/about-us</loc>
        <lastmod>2026-03-30T09:15:00Z</lastmod>
      </url>
    </urlset>
    """

    entries = NewswireCaScraper().parse_entries_from_xml(xml_bytes)

    assert entries == [
        {
            "headline": "Acme Launches New Platform",
            "source_url": "https://www.newswire.ca/news-releases/acme-launches-new-platform-829648686.html",
            "date": "2026-03-29",
        },
        {
            "headline": "Board Approves R D Plan & Budget",
            "source_url": "https://www.newswire.ca/news-releases/board-approves-r-d-plan-%26-budget-829648687.html?foo=bar",
            "date": "2026-03-30",
        },
    ]


def test_newswireca_parse_entries_from_xml_skips_malformed_lastmod():
    xml_bytes = b"""
    <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
      <url>
        <loc>https://www.newswire.ca/news-releases/no-day-in-lastmod-829648686.html</loc>
        <lastmod>2026-03</lastmod>
      </url>
      <url>
        <loc>https://www.newswire.ca/news-releases/missing-lastmod-829648687.html</loc>
      </url>
      <url>
        <loc>https://www.newswire.ca/news-releases/valid-lastmod-829648688.html</loc>
        <lastmod>2026-03-30T09:15:00Z</lastmod>
      </url>
    </urlset>
    """

    entries = NewswireCaScraper().parse_entries_from_xml(xml_bytes)

    assert entries == [
        {
            "headline": "Valid Lastmod",
            "source_url": "https://www.newswire.ca/news-releases/valid-lastmod-829648688.html",
            "date": "2026-03-30",
        },
    ]


def test_pull_newswire_full_skips_newswireca_months_before_coverage(
    monkeypatch, tmp_path
):
    scraper = NewswireCaScraper()
    calls = []

    monkeypatch.setattr(module, "ALL_SCRAPERS", [scraper])
    monkeypatch.setattr(
        module,
        "_generate_month_ranges",
        lambda start_date, end_date: [
            ("2010-12-01", "2011-01-01"),
            ("2011-01-01", "2011-02-01"),
        ],
    )
    monkeypatch.setattr(module, "_setup_file_logging", lambda: tmp_path / "crawl.log")

    def fake_crawl(scraper_obj, year, month, output_dir, shutdown):
        calls.append((scraper_obj.SOURCE_KEY, year, month, Path(output_dir)))
        return (0, 0)

    monkeypatch.setattr(module, "_crawl_scraper_for_month", fake_crawl)

    module.pull_newswire_full(
        start_date="2010-12-01",
        end_date="2011-02-01",
        output_dir=tmp_path / "newswire_headlines",
    )

    assert calls == [("newswireca", 2011, 1, tmp_path / "newswire_headlines")]


def test_is_month_complete_ignores_legacy_zero_day_marker(tmp_path):
    module._mark_month_complete(tmp_path, "prnewswire", 2015, 6)

    assert module._is_month_complete(tmp_path, "prnewswire", 2015, 6) is False


def test_is_month_complete_trusts_explicit_zero_day_reasons(tmp_path):
    module._mark_month_complete(tmp_path, "prnewswire", 2015, 6, reason="no_sitemap")

    assert module._is_month_complete(tmp_path, "prnewswire", 2015, 6) is True


# ---------------------------------------------------------------------------
# _marker_summary
# ---------------------------------------------------------------------------


def _make_source_dir(tmp_path, source_key="prnewswire"):
    """Create a source=... directory and return its Path."""
    source_dir = tmp_path / f"source={source_key}"
    source_dir.mkdir(parents=True, exist_ok=True)
    return source_dir


def test_marker_summary_classifies_ok_terminal_and_legacy(tmp_path):
    """A source with a mix of marker types should be classified correctly."""
    source_dir = _make_source_dir(tmp_path)

    # 1) Healthy marker: .complete + a day-parquet
    module._mark_month_complete(tmp_path, "prnewswire", 2023, 1)
    module._save_day_parquet(
        [{"headline": "h", "source_url": "http://x", "date": "2023-01-15"}],
        tmp_path, "prnewswire", 2023, 1, 15,
    )

    # 2) Terminal marker: no_sitemap, no day-parquets
    module._mark_month_complete(tmp_path, "prnewswire", 2015, 6, reason="no_sitemap")

    # 3) Legacy marker: old-style "crawled" reason, no day-parquets
    module._mark_month_complete(tmp_path, "prnewswire", 2016, 3)

    summary = module._marker_summary(source_dir, "prnewswire")

    assert len(summary["with_days"]) == 1
    assert len(summary["terminal"]) == 1
    assert len(summary["legacy"]) == 1
    assert "year=2016/month=03" in summary["legacy"]


def test_marker_summary_empty_source_returns_empty_lists(tmp_path):
    source_dir = _make_source_dir(tmp_path)

    summary = module._marker_summary(source_dir, "prnewswire")

    assert summary == {"with_days": [], "terminal": [], "legacy": []}


def test_marker_summary_no_urls_is_terminal(tmp_path):
    source_dir = _make_source_dir(tmp_path)
    module._mark_month_complete(tmp_path, "prnewswire", 2014, 8, reason="no_urls")

    summary = module._marker_summary(source_dir, "prnewswire")

    assert len(summary["terminal"]) == 1
    assert summary["with_days"] == []
    assert summary["legacy"] == []


def test_globenewswire_headline_from_slug_handles_org_id_and_encoding():
    url = (
        "https://www.globenewswire.com/news-release/2026/02/26/3246152/18120/en/"
        "dillard-s-inc-announces-0-30-cash-dividend-%26-buyback.html"
    )

    headline = GlobeNewswireScraper._headline_from_slug(url)

    assert headline == "Dillard S Inc Announces 0 30 Cash Dividend & Buyback"
    assert GlobeNewswireScraper._headline_from_slug(
        "https://www.globenewswire.com/search/keyword/earnings"
    ) is None


def test_globenewswire_parse_entries_from_xml_falls_back_to_slug_and_url_date():
    xml_bytes = b"""
    <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
      <url><loc>https://www.globenewswire.com/news-release/2026/02/26/3246149/0/en/cubesmart-reports-fourth-quarter-and-annual-2025-results.html</loc></url>
      <url><loc>https://www.globenewswire.com/news-release/2026/02/27/3246500/0/en/hub-group-declares-quarterly-dividend.html</loc></url>
      <url><loc>https://www.globenewswire.com/about</loc></url>
    </urlset>
    """

    entries = GlobeNewswireScraper().parse_entries_from_xml(xml_bytes)

    assert entries == [
        {
            "headline": "Cubesmart Reports Fourth Quarter And Annual 2025 Results",
            "source_url": "https://www.globenewswire.com/news-release/2026/02/26/3246149/0/en/cubesmart-reports-fourth-quarter-and-annual-2025-results.html",
            "date": "2026-02-26",
        },
        {
            "headline": "Hub Group Declares Quarterly Dividend",
            "source_url": "https://www.globenewswire.com/news-release/2026/02/27/3246500/0/en/hub-group-declares-quarterly-dividend.html",
            "date": "2026-02-27",
        },
    ]


def test_globenewswire_parse_entries_from_xml_prefers_news_title_when_present():
    xml_bytes = b"""
    <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"
            xmlns:news="http://www.google.com/schemas/sitemap-news/0.9">
      <url>
        <loc>https://www.globenewswire.com/news-release/2025/06/30/3107952/0/en/achieve-life-sciences-announces-closing.html</loc>
        <news:news>
          <news:publication_date>2025-06-30T20:05:00Z</news:publication_date>
          <news:title>Achieve Life Sciences Announces Closing of Public Offering</news:title>
        </news:news>
      </url>
    </urlset>
    """

    entries = GlobeNewswireScraper().parse_entries_from_xml(xml_bytes)

    assert entries == [
        {
            "headline": "Achieve Life Sciences Announces Closing of Public Offering",
            "source_url": "https://www.globenewswire.com/news-release/2025/06/30/3107952/0/en/achieve-life-sciences-announces-closing.html",
            "date": "2025-06-30",
        },
    ]
