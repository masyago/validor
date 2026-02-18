import copy
import pytest

### FOR PARSING/VALIDATION ###

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


### FOR NORMALIZATION ###

import uuid
from datetime import datetime, timezone

from sqlalchemy import text


@pytest.fixture
def frozen_now() -> datetime:
    """Canonical 'now' used by normalization tests (aware UTC)."""
    return datetime(2026, 2, 17, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
# TODO: learn more about freezegun
def freeze_time(frozen_now):
    """
    Freeze time so normalized_at and serializer timestamps are stable.

    Requires: freezegun
    """
    freezegun = pytest.importorskip("freezegun")
    with freezegun.freeze_time(frozen_now):
        yield


@pytest.fixture
def fetch_events(db_session):
    """
    Fetch processing events for an ingestion_id in occurred_at order.

    Uses SQL to avoid coupling tests to ProcessingEvent ORM import paths.
    Adjust column names if your schema differs.
    """

    def _fetch_events(ingestion_id: uuid.UUID) -> list[dict]:
        rows = (
            db_session.execute(
                text(
                    """
                SELECT
                  event_id,
                  ingestion_id,
                  execution_id,
                  dedupe_key,
                  target_type,
                  target_id,
                  event_type,
                  occurred_at,
                  severity,
                  actor,
                  actor_version,
                  severity,
                  message,
                  details
                FROM processing_event
                WHERE ingestion_id = :ingestion_id
                ORDER BY occurred_at ASC
                """
                ),
                {"ingestion_id": ingestion_id},
            )
            .mappings()  # retrieve results as list of dicts-like objects
            .all()
        )
        return [dict(r) for r in rows]

    return _fetch_events


@pytest.fixture
def seed_ingestion(
    db_session,
    uploader_id,
    spec_version,
    instrument_id,
    run_id,
    server_sha256,
    status,
    source_filename,
):
    """
    Create an ingestion row and return the ORM object.
    """

    def _seed_ingestion(**overrides):
        from app.persistence.models.core import Ingestion

        ingestion = Ingestion(
            ingestion_id=overrides.get("ingestion_id", uuid.uuid4()),
            instrument_id=overrides.get("instrument_id", instrument_id),
            run_id=overrides.get("run_id", run_id),
            uploader_id=overrides.get("uploader_id", uploader_id),
            spec_version=overrides.get("spec_version", spec_version),
            uploader_received_at=overrides.get(
                "uploader_received_at",
                datetime(2026, 2, 15, 12, 0, 0, tzinfo=timezone.utc),
            ),
            api_received_at=overrides.get(
                "api_received_at",
                datetime(2026, 2, 15, 12, 10, 0, tzinfo=timezone.utc),
            ),
            server_sha256=overrides.get("server_sha256", server_sha256),
            status=overrides.get("status", status),
            source_filename=overrides.get("source_filename", source_filename),
        )
        db_session.add(ingestion)
        db_session.flush()
        return ingestion

    return _seed_ingestion


@pytest.fixture
def seed_panel(db_session):
    """
    Create a Panel row linked to ingestion_id.
    """

    def _seed_panel(*, ingestion_id: uuid.UUID, **overrides):
        from app.persistence.models.parsing import Panel

        panel = Panel(
            panel_id=overrides.get("panel_id", uuid.uuid4()),
            ingestion_id=ingestion_id,
            sample_id=overrides.get(
                "sample_id", "SAM-6da2dc4d-b126-4012-b138-efc7c200ce9a"
            ),
            patient_id=overrides.get(
                "patient_id", "PAT-eafeb37f-fd58-4763-bb13-b7299fb488ef"
            ),
            panel_code=overrides.get("panel_code", "LIPID"),
            collection_timestamp=overrides.get(
                "collection_timestamp",
                datetime(2026, 1, 28, 16, 5, 33, tzinfo=timezone.utc),
            ),
        )
        db_session.add(panel)
        db_session.flush()
        return panel

    return _seed_panel


@pytest.fixture
def seed_test(
    db_session,
    row_number,
):
    """
    Create a Test row linked to panel_id.
    """

    def _seed_test(*, panel_id: uuid.UUID, **overrides):
        from app.persistence.models.parsing import Test

        test = Test(
            test_id=overrides.get("test_id", uuid.uuid4()),
            panel_id=panel_id,
            row_number=overrides.get("row_number", row_number),
            test_code=overrides.get("test_code", "TC"),
            test_name=overrides.get("test_name", "Total Cholesterol"),
            result_raw=overrides.get("result_raw", "151.68"),
            units_raw=overrides.get("unit", "mg/dL"),
            result_value_num=overrides.get("result_value_num", 151.68),
            ref_low_raw=overrides.get("ref_low_raw", "0"),
            ref_high_raw=overrides.get("ref_high_raw", "200"),
            result_comparator=overrides.get("result_comparator", None),
            flag=overrides.get("flag_raw", ""),
        )
        db_session.add(test)
        db_session.flush()
        return test

    return _seed_test
