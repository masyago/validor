from __future__ import annotations

from datetime import datetime, timezone

from app.persistence.models.core import Ingestion
from metrics.record_validation_accuracy_results import (
    collect_validation_accuracy_results,
)


def test_collect_validation_accuracy_results_picks_latest_per_filename(
    db_session,
):
    t0 = datetime(2026, 3, 27, 12, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2026, 3, 27, 12, 5, 0, tzinfo=timezone.utc)

    # Two ingestions for the same filename; later one should win.
    db_session.add(
        Ingestion(
            instrument_id="i",
            run_id="r0",
            uploader_id="u",
            spec_version="v",
            uploader_received_at=t0,
            api_received_at=t0,
            submitted_sha256=None,
            server_sha256="s0",
            status="FAILED VALIDATION",
            error_code="csv_parse_error",
            error_detail={"message": "older"},
            source_filename="bad.csv",
        )
    )
    db_session.add(
        Ingestion(
            instrument_id="i",
            run_id="r1",
            uploader_id="u",
            spec_version="v",
            uploader_received_at=t1,
            api_received_at=t1,
            submitted_sha256=None,
            server_sha256="s1",
            status="COMPLETED",
            error_code=None,
            error_detail=None,
            source_filename="bad.csv",
        )
    )
    db_session.add(
        Ingestion(
            instrument_id="i",
            run_id="r2",
            uploader_id="u",
            spec_version="v",
            uploader_received_at=t0,
            api_received_at=t0,
            submitted_sha256=None,
            server_sha256="s2",
            status="FAILED",
            error_code="raw_data_not_found",
            error_detail={"message": "missing"},
            source_filename="missing.csv",
        )
    )
    db_session.commit()

    rows = collect_validation_accuracy_results(
        session=db_session,
        file_names=["bad.csv", "missing.csv", "not_uploaded.csv"],
        since=None,
    )

    by_name = {r.file_name: r for r in rows}

    assert by_name["bad.csv"].status == "COMPLETED"
    assert by_name["bad.csv"].error_code == ""
    assert by_name["bad.csv"].api_received_at == t1.isoformat()

    assert by_name["missing.csv"].status == "FAILED"
    assert by_name["missing.csv"].error_code == "raw_data_not_found"
    assert "missing" in by_name["missing.csv"].error_detail

    assert by_name["not_uploaded.csv"].ingestion_id == ""
    assert by_name["not_uploaded.csv"].status == ""


def test_collect_validation_accuracy_results_since_filters(db_session):
    t0 = datetime(2026, 3, 27, 12, 0, 0, tzinfo=timezone.utc)
    t1 = datetime(2026, 3, 27, 12, 5, 0, tzinfo=timezone.utc)

    db_session.add(
        Ingestion(
            instrument_id="i",
            run_id="r0",
            uploader_id="u",
            spec_version="v",
            uploader_received_at=t0,
            api_received_at=t0,
            submitted_sha256=None,
            server_sha256="s0",
            status="FAILED VALIDATION",
            error_code="csv_parse_error",
            error_detail={"message": "older"},
            source_filename="bad.csv",
        )
    )
    db_session.add(
        Ingestion(
            instrument_id="i",
            run_id="r1",
            uploader_id="u",
            spec_version="v",
            uploader_received_at=t1,
            api_received_at=t1,
            submitted_sha256=None,
            server_sha256="s1",
            status="COMPLETED",
            error_code=None,
            error_detail=None,
            source_filename="bad.csv",
        )
    )
    db_session.commit()

    rows = collect_validation_accuracy_results(
        session=db_session,
        file_names=["bad.csv"],
        since=datetime(2026, 3, 27, 12, 1, 0, tzinfo=timezone.utc),
    )

    assert len(rows) == 1
    assert rows[0].file_name == "bad.csv"
    assert rows[0].status == "COMPLETED"
