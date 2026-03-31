from discover_wayback_timestamps import (
    _collapse_month_ranges,
    validate_existing_timestamps,
)


def test_collapse_month_ranges_groups_contiguous_months():
    months = ["2012-07", "2012-08", "2012-09", "2015-03", "2015-05", "2015-06"]

    collapsed = _collapse_month_ranges(months)

    assert collapsed == ["2012-07..2012-09", "2015-03", "2015-05..2015-06"]


def test_validate_existing_timestamps_flags_missing_and_malformed_entries():
    timestamps = {
        "2012-07": "20260102151437",
        "2012-08": "bad-timestamp",
        "2012-10": "20260102151450",
        "2021-03": "20250402102233",
    }

    results = validate_existing_timestamps(
        timestamps,
        start="2012-07",
        end="2012-10",
        known_no_archive={"2012-09"},
    )

    assert results["expected_months"] == 4
    assert results["timestamps_in_range"] == 3
    assert results["missing"] == []
    assert results["covered_no_archive"] == []
    assert results["malformed_keys"] == []
    assert results["malformed_timestamps"] == [("2012-08", "bad-timestamp")]
    assert results["outside_range"] == ["2021-03"]


def test_validate_existing_timestamps_detects_missing_months_not_in_no_archive():
    timestamps = {
        "2012-07": "20260102151437",
        "2012-10": "20260102151450",
    }

    results = validate_existing_timestamps(
        timestamps,
        start="2012-07",
        end="2012-10",
        known_no_archive={"2012-09"},
    )

    assert results["missing"] == ["2012-08"]
