from pull_edgar_press_releases import EDGAR_ARCHIVE, _extract_headline_from_exhibit, _summarize_validation


def test_extract_headline_prefers_substantive_bold_text():
    html = """
    <html>
      <body>
        <p><strong>Exhibit 99.1</strong></p>
        <p><strong>FOR IMMEDIATE RELEASE</strong></p>
        <p><strong>Acme Launches New AI Platform</strong></p>
      </body>
    </html>
    """

    assert _extract_headline_from_exhibit(html) == "Acme Launches New AI Platform"


def test_extract_headline_falls_back_to_first_substantive_paragraph():
    html = """
    <html>
      <body>
        <p>CONTACT: Investor Relations</p>
        <p>Acme Reports Record First Quarter Revenue</p>
        <p>Chicago, IL, March 30, 2026 ...</p>
      </body>
    </html>
    """

    assert _extract_headline_from_exhibit(html) == "Acme Reports Record First Quarter Revenue"


def test_summarize_validation_reports_yield_and_sample_failures():
    filings = [
        {
            "cik": "123456",
            "adsh": "0001234567-24-000001",
            "filename": "ex99-1.htm",
            "file_type": "EX-99.1",
        },
        {
            "cik": "123456",
            "adsh": "0001234567-24-000002",
            "filename": "ex99-2.htm",
            "file_type": "EX-99.2",
        },
        {
            "cik": "123456",
            "adsh": "0001234567-24-000003",
            "filename": "main8k.htm",
            "file_type": "8-K",
        },
    ]
    headlines = [
        {
            "headline": "Acme Launches New AI Platform",
            "source_url": "https://www.sec.gov/Archives/edgar/data/123456/foo/ex99-1.htm",
            "date": "2024-01-02",
        }
    ]
    failed_exhibits = [filings[1]]

    summary = _summarize_validation(
        "2024-01-02",
        filings=filings,
        headlines=headlines,
        failed_exhibits=failed_exhibits,
        sample_size=3,
    )

    assert summary["date"] == "2024-01-02"
    assert summary["efts_hits"] == 3
    assert summary["ex99_hits"] == 2
    assert summary["headline_count"] == 1
    assert summary["failed_exhibit_count"] == 1
    assert summary["headline_yield_pct"] == 50.0
    assert summary["sample_headlines"] == ["Acme Launches New AI Platform"]
    assert summary["failed_exhibit_urls"] == [
        f"{EDGAR_ARCHIVE}/123456/000123456724000002/ex99-2.htm"
    ]
