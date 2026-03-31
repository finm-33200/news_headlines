from pathlib import Path

import pull_free_newswires as module
from pull_free_newswires import NewswireCaScraper


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
