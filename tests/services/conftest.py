import copy
import pytest

# Canonical analyzer CSV columns
CSV_COLUMNS = [
    "run_id",
    "sample_id",
    "patient_id",
    "panel_code",
    "test_code",
    "test_name",
    "analyte_type",
    "result",
    "units",
    "reference_range_low",
    "reference_range_high",
    "flag",
    "collection_timestamp",
    "instrument_id",
]


@pytest.fixture
def base_row(run_id: str, instrument_id: str) -> dict[str, str]:
    """
    Canonical row after CSV is parsed by parser to dict[rows]
    """
    return {
        "run_id": run_id,
        "sample_id": "SAM-6da2dc4d-b126-4012-b138-efc7c200ce9a",
        "patient_id": "PAT-eafeb37f-fd58-4763-bb13-b7299fb488ef",
        "panel_code": "LIPID",
        "test_code": "TC",
        "test_name": "",
        "analyte_type": "",
        "result": "151.68",
        "units": "mg/dL",
        "reference_range_low": "0",
        "reference_range_high": "200",
        "flag": "",
        "collection_timestamp": "2026-01-28T16:05:33+00:00",
        "instrument_id": instrument_id,
    }


@pytest.fixture
def make_row(base_row):
    """
    Factory fixture:
      row = make_row(result="<= 0.1")
    """

    def _make_row(**overrides: str) -> dict[str, str]:
        row = copy.deepcopy(base_row)
        for k, v in overrides.items():
            row[k] = v
        return row

    return _make_row


@pytest.fixture
def rows_same_panel_two_tests(make_row) -> list[dict[str, str]]:
    """
    Two CSV rows that belong to the same panel group (same panel_code/sample_id/timestamp),
    but represent two different tests.
    """
    return [
        make_row(
            test_code="TC", test_name="Total Cholesterol", result="151.68"
        ),
        make_row(
            test_code="HDL",
            test_name="High-Density Lipoprotein",
            result="= 55",
        ),
    ]


@pytest.fixture
def rows_two_panels(make_row) -> list[dict[str, str]]:
    """
    Two different panel groups (different panel_code).
    """
    return [
        make_row(panel_code="BMP", test_code="Na", result="139"),
        make_row(
            panel_code="LIPID", test_code="TC", result="271", flag="high"
        ),
    ]
