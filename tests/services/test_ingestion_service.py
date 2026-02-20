from __future__ import annotations

import csv
import hashlib
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import uuid

import pytest
from sqlalchemy import select

from app.core.ingestion_status_enums import IngestionStatus
from app.persistence.models.core import Ingestion, RawData
from app.persistence.models.parsing import Panel
from app.persistence.repositories.panel_repo import PanelRepository
from app.persistence.repositories.test_repo import (
    TestRepository as LabTestRepository,
)
from app.services.ingestion_service import IngestionService
from app.services.validator import RowValidationError
from app.services.utils import NormalizationError

CSV_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "csv"


def _read_fixture_bytes(filename: str) -> bytes:
    return (CSV_DIR / filename).read_bytes()


@pytest.fixture
def ingestion_service(db_session):
    return IngestionService(db_session)


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _seed_ingestion_and_raw_data(
    db_session,
    *,
    ingestion_id,
    csv_bytes: bytes,
    uploader_id: str,
    spec_version: str,
    instrument_id: str,
    run_id: str,
    source_filename: str = "fixture.csv",
) -> None:
    """
    Seeds minimal required rows for process_ingestion():
      - Ingestion in RECEIVED
      - RawData with content_bytes
    """
    now = datetime.now(timezone.utc)
    server_sha = _sha256_hex(csv_bytes)

    ingestion = Ingestion(
        ingestion_id=ingestion_id,
        instrument_id=instrument_id,
        run_id=run_id,
        uploader_id=uploader_id,
        spec_version=spec_version,
        uploader_received_at=now,
        api_received_at=now,
        submitted_sha256=None,
        server_sha256=server_sha,
        status=IngestionStatus.RECEIVED,
        error_code=None,
        error_detail=None,
        source_filename=source_filename,
        ingestion_idempotency_disposition=None,
    )
    db_session.add(ingestion)
    db_session.flush()

    raw = RawData(
        ingestion_id=ingestion_id,
        content_bytes=csv_bytes,
        content_mime="text/csv",
        content_size_bytes=len(csv_bytes),
    )
    db_session.add(raw)

    db_session.flush()


def _parse_csv_rows_for_expectations(csv_bytes: bytes) -> list[dict[str, str]]:
    """
    Parse rows in the same general shape as CanonicalAnalyzerCsvParser:
    dict[str, str] with whitespace stripped.
    Kept local to tests so we can compute expected grouping/test counts.
    """
    text = csv_bytes.decode("utf-8-sig")
    reader = csv.DictReader(text.splitlines())
    out: list[dict[str, str]] = []
    for raw in reader:
        normalized: dict[str, str] = {}
        for k, v in raw.items():
            if k is None:
                continue
            if isinstance(v, str):
                normalized[str(k)] = v.strip()
            elif v is None:
                normalized[str(k)] = ""
            else:
                normalized[str(k)] = str(v)
        out.append(normalized)
    return out


def _normalize_group_key(
    row: dict[str, str],
) -> tuple[str, str | None, datetime]:
    """
    Matches the grouping spec used by PanelValidation:
      (panel_code, sample_id or None, collection_timestamp normalized to tz-aware).
    """
    panel_code = row["panel_code"]
    sample_id_raw = row.get("sample_id", "")
    sample_id = sample_id_raw or None

    ts_raw = row["collection_timestamp"]
    ts = datetime.fromisoformat(ts_raw)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    return (panel_code, sample_id, ts)


class TestIngestionServiceUnit:
    def test_errors_to_json_converts_row_validation_errors(
        self, db_session, ingestion_service
    ):
        """Three data types used in errors: RowValidationError, NormalizationError, dict, error
        message. Data type converted into JSON dicts without errors.
        """
        errors: list[Any] = [
            RowValidationError(
                row_number=3,
                field="test_code",
                message="required field missing",
            ),
            NormalizationError(
                model="Test",
                field="result_raw",
                message="expected numeric",
            ),
            {"row_number": 4, "field": "result", "message": "already a dict"},
            ValueError("unexpected error"),
        ]

        out = ingestion_service._errors_to_json(errors)

        assert out == [
            {
                "row_number": 3,
                "field": "test_code",
                "message": "required field missing",
            },
            {
                "model": "Test",
                "field": "result_raw",
                "message": "expected numeric",
            },
            {"row_number": 4, "field": "result", "message": "already a dict"},
            {"message": "unexpected error"},
        ]


class TestIngestionServiceIntegration:

    def test_process_ingestion_missing_raw_data_marks_failed(
        self,
        db_session,
        ingestion_service,
        fetch_events,
        uploader_id,
        spec_version,
        instrument_id,
        run_id,
    ):
        ingestion_id = uuid.uuid4()
        now = datetime.now(timezone.utc)

        # Seed ingestion row only (no RawData)
        db_session.add(
            Ingestion(
                ingestion_id=ingestion_id,
                instrument_id=instrument_id,
                run_id=run_id,
                uploader_id=uploader_id,
                spec_version=spec_version,
                uploader_received_at=now,
                api_received_at=now,
                submitted_sha256=None,
                server_sha256="0" * 64,
                status=IngestionStatus.RECEIVED,
                error_code=None,
                error_detail=None,
                source_filename="missing_raw.csv",
                ingestion_idempotency_disposition=None,
            )
        )
        db_session.flush()

        ingestion_service.process_ingestion(ingestion_id)

        ingestion = db_session.scalars(
            select(Ingestion).where(Ingestion.ingestion_id == ingestion_id)
        ).one()
        assert ingestion.status == IngestionStatus.FAILED
        assert ingestion.error_code == "raw_data_not_found"
        assert ingestion.error_detail is not None

        events = fetch_events(ingestion_id)
        types = [e["event_type"] for e in events]
        assert "PARSE_STARTED" in types
        assert "PARSE_FAILED" in types
        parse_failed = [
            e for e in events if e["event_type"] == "PARSE_FAILED"
        ][-1]
        assert (parse_failed.get("details") or {}).get(
            "error_code"
        ) == "raw_data_not_found"

    def test_process_ingestion_empty_csv_marks_failed_validation(
        self,
        db_session,
        ingestion_service,
        fetch_events,
        uploader_id,
        spec_version,
        instrument_id,
        run_id,
    ):
        ingestion_id = uuid.uuid4()
        csv_bytes = b""

        _seed_ingestion_and_raw_data(
            db_session,
            ingestion_id=ingestion_id,
            csv_bytes=csv_bytes,
            uploader_id=uploader_id,
            spec_version=spec_version,
            instrument_id=instrument_id,
            run_id=run_id,
            source_filename="empty.csv",
        )

        ingestion_service.process_ingestion(ingestion_id)

        ingestion = db_session.scalars(
            select(Ingestion).where(Ingestion.ingestion_id == ingestion_id)
        ).one()
        assert ingestion.status == IngestionStatus.FAILED_VALIDATION
        assert ingestion.error_code == "empty_csv"
        assert ingestion.error_detail is not None

        events = fetch_events(ingestion_id)
        types = [e["event_type"] for e in events]
        assert "PARSE_STARTED" in types
        assert "PARSE_FAILED" in types
        parse_failed = [
            e for e in events if e["event_type"] == "PARSE_FAILED"
        ][-1]
        assert (parse_failed.get("details") or {}).get(
            "error_code"
        ) == "empty_csv"

    def test_process_ingestion_invalid_utf8_marks_failed_validation(
        self,
        db_session,
        ingestion_service,
        fetch_events,
        uploader_id,
        spec_version,
        instrument_id,
        run_id,
    ):
        ingestion_id = uuid.uuid4()
        csv_bytes = b"\xff\xfe\xfa"  # invalid utf-8

        _seed_ingestion_and_raw_data(
            db_session,
            ingestion_id=ingestion_id,
            csv_bytes=csv_bytes,
            uploader_id=uploader_id,
            spec_version=spec_version,
            instrument_id=instrument_id,
            run_id=run_id,
            source_filename="bad_encoding.csv",
        )

        ingestion_service.process_ingestion(ingestion_id)

        ingestion = db_session.scalars(
            select(Ingestion).where(Ingestion.ingestion_id == ingestion_id)
        ).one()
        assert ingestion.status == IngestionStatus.FAILED_VALIDATION
        assert ingestion.error_code == "csv_decode_error"
        assert ingestion.error_detail is not None

        events = fetch_events(ingestion_id)
        parse_failed = [
            e for e in events if e["event_type"] == "PARSE_FAILED"
        ][-1]
        assert (parse_failed.get("details") or {}).get(
            "error_code"
        ) == "csv_decode_error"

    def test_process_ingestion_happy_path_data_persists(
        self,
        db_session,
        ingestion_service,
        fetch_events,
        uploader_id,
        spec_version,
        instrument_id,
        run_id,
    ):
        ingestion_id = uuid.uuid4()
        csv_bytes = _read_fixture_bytes("valid_csv_20260128_004.csv")

        _seed_ingestion_and_raw_data(
            db_session,
            ingestion_id=ingestion_id,
            csv_bytes=csv_bytes,
            uploader_id=uploader_id,
            spec_version=spec_version,
            instrument_id=instrument_id,
            run_id=run_id,
            source_filename="valid_csv_20260128_004.csv",
        )
        # Expected counts derived from fixture content (without calling validator/grouping code)
        rows = _parse_csv_rows_for_expectations(csv_bytes)
        expected_group_counts: dict[tuple[str, str | None, datetime], int] = {}
        for r in rows:
            key = _normalize_group_key(r)
            expected_group_counts[key] = expected_group_counts.get(key, 0) + 1

        ingestion_service.process_ingestion(ingestion_id)

        events = fetch_events(ingestion_id)
        types = [e["event_type"] for e in events]
        assert "PARSE_STARTED" in types
        assert "PARSE_SUCCEEDED" in types
        assert "VALIDATION_STARTED" in types
        assert "VALIDATION_SUCCEEDED" in types

        # Panels persisted
        panels = PanelRepository(db_session).get_by_ingestion_id(ingestion_id)
        assert len(panels) == len(expected_group_counts)

        # Tests persisted per panel group
        test_repo = LabTestRepository(db_session)
        for p in panels:
            panel_key = (p.panel_code, p.sample_id, p.collection_timestamp)
            expected_tests = expected_group_counts[panel_key]
            actual_tests = list(test_repo.get_by_panel_id(p.panel_id))
            assert len(actual_tests) == expected_tests

        # Ingestion is marked COMPLETED after normalization succeeds
        ingestion = db_session.scalars(
            select(Ingestion).where(Ingestion.ingestion_id == ingestion_id)
        ).one()
        assert ingestion.status == IngestionStatus.COMPLETED
        assert ingestion.error_detail is None

    def test_process_ingestion_validation_failure_persists_nothing_and_marks_failed_validation(
        self,
        ingestion_service,
        db_session,
        fetch_events,
        uploader_id,
        spec_version,
        instrument_id,
        run_id,
    ):
        ingestion_id = uuid.uuid4()
        csv_bytes = _read_fixture_bytes(
            "invalid_csv_missing_fields_20260128_003.csv"
        )

        _seed_ingestion_and_raw_data(
            db_session,
            ingestion_id=ingestion_id,
            csv_bytes=csv_bytes,
            uploader_id=uploader_id,
            spec_version=spec_version,
            instrument_id=instrument_id,
            run_id=run_id,
            source_filename="invalid_csv_missing_fields_20260128_003.csv",
        )

        ingestion_service.process_ingestion(ingestion_id)

        # Persist nothing: no panels for this ingestion. No tests linked to this ingestion
        panels = PanelRepository(db_session).get_by_ingestion_id(ingestion_id)
        assert panels == []

        # Status and error detail persisted on ingestion
        ingestion = db_session.scalars(
            select(Ingestion).where(Ingestion.ingestion_id == ingestion_id)
        ).one()
        assert ingestion.status == IngestionStatus.FAILED_VALIDATION

        assert ingestion.error_detail is not None
        assert "validation_errors" in ingestion.error_detail
        assert isinstance(ingestion.error_detail["validation_errors"], list)
        assert len(ingestion.error_detail["validation_errors"]) >= 1

        first = ingestion.error_detail["validation_errors"][0]
        assert isinstance(first, dict)
        assert "field" in first
        assert "message" in first

        events = fetch_events(ingestion_id)
        types = [e["event_type"] for e in events]
        assert "PARSE_STARTED" in types
        assert "PARSE_SUCCEEDED" in types
        assert "VALIDATION_STARTED" in types
        assert "VALIDATION_FAILED" in types
