from __future__ import annotations

import uuid
from datetime import datetime, timezone
import threading

import pytest
from sqlalchemy import select, text
from sqlalchemy.orm import Session
from sqlalchemy.exc import OperationalError

from app.persistence.models.normalization import DiagnosticReport, Observation
from app.persistence.models.core import Ingestion
from app.persistence.models.parsing import Panel, Test as ParsingTest
from app.services.normalizer import NormalizationJob

### PHASE 1 RELATIONAL ###

"""
Happy path:
- Normalization Phase 1, Phase 2 and overall succeeds. 
- Data persisted. 

Test cases:
- 1 panel and 2 panels per ingestion. 3 tests per panel.
- variants within panel:
    - result_value_num within reference range, no flag discrepancy
    - result_value_num out of range and has flag discrepancy
    - no result_value_num, only result_raw that cannot be coerced to float
"""


@pytest.mark.parametrize("panel_count", [1, 2])
def test_phase1_happy_path_creates_rows_and_emits_events(
    db_session,
    fetch_events,
    seed_ingestion,
    seed_panel,
    seed_test,
    panel_count: int,
):
    ingestion = seed_ingestion()

    panels = []
    if panel_count == 1:
        panels.append(
            seed_panel(
                ingestion_id=ingestion.ingestion_id,
                panel_code="LIPID",
                sample_id=f"SAM-{uuid.uuid4()}",
            )
        )
    else:
        panels.append(
            seed_panel(
                ingestion_id=ingestion.ingestion_id,
                panel_code="LIPID",
                sample_id=f"SAM-{uuid.uuid4()}",
            )
        )
        panels.append(
            seed_panel(
                ingestion_id=ingestion.ingestion_id,
                panel_code="BMP",
                sample_id=f"SAM-{uuid.uuid4()}",
            )
        )

    expected_test_ids: list[uuid.UUID] = []
    expected_text_only_test_ids: set[uuid.UUID] = set()
    expected_discrepancy_test_ids: set[uuid.UUID] = set()

    for i, panel in enumerate(panels):
        # 1) numeric + in-range (system NORMAL; analyzer flag NORMAL)
        t_in = seed_test(
            panel_id=panel.panel_id,
            row_number=10 + i * 10 + 1,
            test_code="INRANGE",
            test_name="In-range Numeric",
            result_raw="100",
            result_value_num=100.0,
            ref_low_raw="0",
            ref_high_raw="200",
            flag_raw="normal",
            unit="mg/dL",
        )

        # 2) numeric + out-of-range (system HIGH; analyzer flag NORMAL -> discrepancy)
        t_hi = seed_test(
            panel_id=panel.panel_id,
            row_number=10 + i * 10 + 2,
            test_code="OUTRANGE",
            test_name="Out-of-range Numeric",
            result_raw="300",
            result_value_num=300.0,
            ref_low_raw="0",
            ref_high_raw="200",
            flag_raw="normal",
            unit="mg/dL",
        )
        expected_discrepancy_test_ids.add(t_hi.test_id)

        # 3) text-only (no result_value_num, has result_raw)
        t_txt = seed_test(
            panel_id=panel.panel_id,
            row_number=10 + i * 10 + 3,
            test_code="TEXTONLY",
            test_name="Text-only",
            result_raw="NEGATIVE",
            result_value_num=None,
            ref_low_raw="",
            ref_high_raw="",
            flag_raw="",
            unit=None,
        )
        expected_text_only_test_ids.add(t_txt.test_id)

        expected_test_ids.extend([t_in.test_id, t_hi.test_id, t_txt.test_id])

    job = NormalizationJob(db_session)
    ok, norm_errors, json_failures = job.run_for_ingestion_id(
        ingestion.ingestion_id
    )

    assert ok is True
    assert norm_errors == []
    assert json_failures == []

    # DiagnosticReport rows exist (Phase 1) and have resource_json written (Phase 2)
    drs = (
        db_session.execute(
            select(DiagnosticReport).where(
                DiagnosticReport.ingestion_id == ingestion.ingestion_id
            )
        )
        .scalars()
        .all()
    )
    assert len(drs) == panel_count
    assert all(dr.resource_json is not None for dr in drs)

    # Observation rows exist, reference DR, and have JSON written
    obs = (
        db_session.execute(
            select(Observation).where(
                Observation.ingestion_id == ingestion.ingestion_id
            )
        )
        .scalars()
        .all()
    )
    assert len(obs) == (panel_count * 3)
    assert all(o.diagnostic_report_id is not None for o in obs)
    assert all(o.resource_json is not None for o in obs)

    obs_by_test_id = {o.test_id: o for o in obs}
    for test_id in expected_test_ids:
        assert test_id in obs_by_test_id

    # Text-only test should map to value_text
    for test_id in expected_text_only_test_ids:
        o = obs_by_test_id[test_id]
        assert o.value_num is None
        assert o.value_text == "NEGATIVE"

    # Discrepancy test(s) should have mismatch markers
    for test_id in expected_discrepancy_test_ids:
        o = obs_by_test_id[test_id]
        assert o.flag_system_interpretation == "HIGH"
        assert (o.discrepancy or "").startswith("analyzer and system")

    # Phase 1 events emitted
    events = fetch_events(ingestion.ingestion_id)
    event_types = {e["event_type"] for e in events}
    assert "NORMALIZATION_STARTED" in event_types
    assert "NORMALIZATION_RELATIONAL_SUCCEEDED" in event_types

    # Sanity-check discrepancy count in Phase 1 success details
    phase1_events = [
        e
        for e in events
        if e["event_type"] == "NORMALIZATION_RELATIONAL_SUCCEEDED"
    ]
    assert len(phase1_events) == 1
    details = phase1_events[0].get("details") or {}
    assert details.get("discrepancy_count") == panel_count


# Phase 1 failure due do missing required fields. Nothing persists.
def test_phase1_validation_failure_is_all_or_nothing_and_emits_failed_events(
    db_session,
    fetch_events,
    seed_ingestion,
    seed_panel,
    seed_test,
):
    """
    If any panel/test fails normalization validation, nothing should be
    persisted to normalized tables.
    """

    ingestion = seed_ingestion()
    panel = seed_panel(
        ingestion_id=ingestion.ingestion_id,
        panel_code="LIPID",
        sample_id=f"SAM-{uuid.uuid4()}",
    )

    # Missing numeric + missing result_raw (empty string becomes None via optional())
    seed_test(
        panel_id=panel.panel_id,
        row_number=1,
        test_code="BAD",
        test_name="Bad Row",
        result_raw="",
        result_value_num=None,
        ref_low_raw="0",
        ref_high_raw="200",
        flag_raw="",
        unit="mg/dL",
    )

    ok, norm_errors, json_failures = NormalizationJob(
        db_session
    ).run_for_ingestion_id(ingestion.ingestion_id)

    assert ok is False
    assert json_failures == []
    assert any(
        (
            e.model == "Test"
            and e.field == "result_raw"
            and "required" in e.message
        )
        for e in norm_errors
    )

    drs = (
        db_session.execute(
            select(DiagnosticReport).where(
                DiagnosticReport.ingestion_id == ingestion.ingestion_id
            )
        )
        .scalars()
        .all()
    )
    obs = (
        db_session.execute(
            select(Observation).where(
                Observation.ingestion_id == ingestion.ingestion_id
            )
        )
        .scalars()
        .all()
    )
    assert drs == []
    assert obs == []

    events = fetch_events(ingestion.ingestion_id)
    event_types = {e["event_type"] for e in events}
    assert "NORMALIZATION_STARTED" in event_types
    assert "NORMALIZATION_RELATIONAL_FAILED" in event_types
    assert "NORMALIZATION_FAILED" in event_types


# Phase 1 fails as no panels with ingestion_id exist
def test_phase1_empty_ingestion_returns_error_and_emits_failed_events(
    db_session,
    fetch_events,
    seed_ingestion,
):

    ingestion = seed_ingestion()

    ok, norm_errors, json_failures = NormalizationJob(
        db_session
    ).run_for_ingestion_id(ingestion.ingestion_id)

    assert ok is False
    assert json_failures == []
    assert any(
        (e.model == "Panel" and "not found" in e.message) for e in norm_errors
    )

    drs = (
        db_session.execute(
            select(DiagnosticReport).where(
                DiagnosticReport.ingestion_id == ingestion.ingestion_id
            )
        )
        .scalars()
        .all()
    )
    obs = (
        db_session.execute(
            select(Observation).where(
                Observation.ingestion_id == ingestion.ingestion_id
            )
        )
        .scalars()
        .all()
    )
    assert drs == []
    assert obs == []

    events = fetch_events(ingestion.ingestion_id)
    event_types = {e["event_type"] for e in events}
    assert "NORMALIZATION_STARTED" in event_types
    assert "NORMALIZATION_RELATIONAL_FAILED" in event_types
    assert "NORMALIZATION_FAILED" in event_types


# Re-running normalization for the same ingestion is idempotent
def test_phase1_idempotency_rerun_same_ingestion_no_duplicates_and_created_counts_zero(
    db_session,
    fetch_events,
    seed_ingestion,
    seed_panel,
    seed_test,
):

    ingestion = seed_ingestion()
    panel = seed_panel(
        ingestion_id=ingestion.ingestion_id,
        panel_code="LIPID",
        sample_id=f"SAM-{uuid.uuid4()}",
    )

    tests = [
        seed_test(
            panel_id=panel.panel_id,
            row_number=1,
            test_code="INRANGE",
            test_name="In-range Numeric",
            result_raw="100",
            result_value_num=100.0,
            ref_low_raw="0",
            ref_high_raw="200",
            flag_raw="normal",
            unit="mg/dL",
        ),
        seed_test(
            panel_id=panel.panel_id,
            row_number=2,
            test_code="OUTRANGE",
            test_name="Out-of-range Numeric",
            result_raw="300",
            result_value_num=300.0,
            ref_low_raw="0",
            ref_high_raw="200",
            flag_raw="normal",
            unit="mg/dL",
        ),
        seed_test(
            panel_id=panel.panel_id,
            row_number=3,
            test_code="TEXTONLY",
            test_name="Text-only",
            result_raw="NEGATIVE",
            result_value_num=None,
            ref_low_raw="",
            ref_high_raw="",
            flag_raw="",
            unit=None,
        ),
    ]

    job = NormalizationJob(db_session)

    ok1, norm_errors1, json_failures1 = job.run_for_ingestion_id(
        ingestion.ingestion_id
    )
    assert ok1 is True
    assert norm_errors1 == []
    assert json_failures1 == []

    drs_1 = (
        db_session.execute(
            select(DiagnosticReport).where(
                DiagnosticReport.ingestion_id == ingestion.ingestion_id
            )
        )
        .scalars()
        .all()
    )
    obs_1 = (
        db_session.execute(
            select(Observation).where(
                Observation.ingestion_id == ingestion.ingestion_id
            )
        )
        .scalars()
        .all()
    )
    assert len(drs_1) == 1
    assert len(obs_1) == len(tests)

    dr_json_1 = {dr.diagnostic_report_id: dr.resource_json for dr in drs_1}
    ob_json_1 = {ob.test_id: ob.resource_json for ob in obs_1}

    ok2, norm_errors2, json_failures2 = job.run_for_ingestion_id(
        ingestion.ingestion_id
    )
    assert ok2 is True
    assert norm_errors2 == []
    assert json_failures2 == []

    drs_2 = (
        db_session.execute(
            select(DiagnosticReport).where(
                DiagnosticReport.ingestion_id == ingestion.ingestion_id
            )
        )
        .scalars()
        .all()
    )
    obs_2 = (
        db_session.execute(
            select(Observation).where(
                Observation.ingestion_id == ingestion.ingestion_id
            )
        )
        .scalars()
        .all()
    )
    assert len(drs_2) == 1
    assert len(obs_2) == len(tests)

    dr_json_2 = {dr.diagnostic_report_id: dr.resource_json for dr in drs_2}
    ob_json_2 = {ob.test_id: ob.resource_json for ob in obs_2}

    # Resource JSON should remain stable between runs
    assert dr_json_2 == dr_json_1
    assert ob_json_2 == ob_json_1

    # Verify Phase 1 created counts are zero on the second run
    events = fetch_events(ingestion.ingestion_id)
    phase1_events = [
        e
        for e in events
        if e["event_type"] == "NORMALIZATION_RELATIONAL_SUCCEEDED"
    ]
    assert len(phase1_events) == 2
    phase1_events_sorted = sorted(
        phase1_events, key=lambda e: e["occurred_at"]
    )
    details_second = phase1_events_sorted[-1].get("details") or {}
    assert details_second.get("diagnostic_reports_created") == 0
    assert details_second.get("observations_created") == 0


# Two concurrent runs should not create duplicates or crash
def test_phase1_concurrency_two_sessions_no_duplicates_and_total_created_counts_match(
    test_db,
    instrument_id,
    run_id,
    uploader_id,
    spec_version,
):

    ingestion_id = uuid.uuid4()
    # Ensure uniqueness even if the base fixture value is constant (e.g. date-based).
    run_id = f"{run_id}-{uuid.uuid4().hex[:10]}"

    panel_id = uuid.uuid4()
    test_ids = [uuid.uuid4(), uuid.uuid4(), uuid.uuid4()]

    def _fetch_events(session: Session) -> list[dict]:
        rows = (
            session.execute(
                text(
                    """
                SELECT event_type, occurred_at, details
                FROM processing_event
                WHERE ingestion_id = :ingestion_id
                ORDER BY occurred_at ASC
                """
                ),
                {"ingestion_id": ingestion_id},
            )
            .mappings()
            .all()
        )
        return [dict(r) for r in rows]

    # Seed data in committed state so both sessions can see it.
    seed_conn = test_db.connect()
    seed_session = Session(bind=seed_conn)
    try:
        now = datetime.now(timezone.utc)
        seed_session.add(
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
                status="PROCESSING",
                error_code=None,
                error_detail=None,
                source_filename="fixture.csv",
                ingestion_idempotency_disposition=None,
            )
        )
        seed_session.add(
            Panel(
                panel_id=panel_id,
                ingestion_id=ingestion_id,
                patient_id=f"PAT-{uuid.uuid4()}",
                panel_code="LIPID",
                sample_id=f"SAM-{uuid.uuid4()}",
                collection_timestamp=datetime(
                    2026, 1, 28, 16, 5, 33, tzinfo=timezone.utc
                ),
            )
        )

        seed_session.add(
            ParsingTest(
                test_id=test_ids[0],
                panel_id=panel_id,
                row_number=1,
                test_code="INRANGE",
                test_name="In-range",
                analyte_type=None,
                result_raw="100",
                units_raw="mg/dL",
                result_value_num=100.0,
                result_comparator=None,
                ref_low_raw="0",
                ref_high_raw="200",
                flag="normal",
            )
        )
        seed_session.add(
            ParsingTest(
                test_id=test_ids[1],
                panel_id=panel_id,
                row_number=2,
                test_code="OUTRANGE",
                test_name="Out",
                analyte_type=None,
                result_raw="300",
                units_raw="mg/dL",
                result_value_num=300.0,
                result_comparator=None,
                ref_low_raw="0",
                ref_high_raw="200",
                flag="normal",
            )
        )
        seed_session.add(
            ParsingTest(
                test_id=test_ids[2],
                panel_id=panel_id,
                row_number=3,
                test_code="TEXTONLY",
                test_name="Text",
                analyte_type=None,
                result_raw="NEGATIVE",
                units_raw=None,
                result_value_num=None,
                result_comparator=None,
                ref_low_raw="",
                ref_high_raw="",
                flag=None,
            )
        )

        seed_session.commit()

        barrier = threading.Barrier(2)
        results: list[tuple[bool, list, list]] = []
        errors: list[BaseException] = []
        lock = threading.Lock()

        def _runner() -> None:
            conn = test_db.connect()
            session = Session(bind=conn)
            try:
                barrier.wait(timeout=10)
                ok, norm_errors, json_failures = NormalizationJob(
                    session
                ).run_for_ingestion_id(ingestion_id)
                with lock:
                    results.append((ok, norm_errors, json_failures))
            except BaseException as e:
                with lock:
                    errors.append(e)
            finally:
                session.close()
                conn.close()

        t1 = threading.Thread(target=_runner)
        t2 = threading.Thread(target=_runner)
        t1.start()
        t2.start()
        t1.join(timeout=30)
        t2.join(timeout=30)

        assert errors == []
        assert len(results) == 2
        assert all(r[0] is True for r in results)
        assert all(r[1] == [] for r in results)
        assert all(r[2] == [] for r in results)

        # Validate final DB state
        verify_session = Session(bind=seed_conn)
        try:
            drs = (
                verify_session.execute(
                    select(DiagnosticReport).where(
                        DiagnosticReport.ingestion_id == ingestion_id
                    )
                )
                .scalars()
                .all()
            )
            obs = (
                verify_session.execute(
                    select(Observation).where(
                        Observation.ingestion_id == ingestion_id
                    )
                )
                .scalars()
                .all()
            )
            assert len(drs) == 1
            assert len(obs) == 3

            events = _fetch_events(verify_session)
            phase1_events = [
                e
                for e in events
                if e["event_type"] == "NORMALIZATION_RELATIONAL_SUCCEEDED"
            ]
            # one per job invocation
            assert len(phase1_events) == 2
            total_dr_created = sum(
                int(
                    (e.get("details") or {}).get("diagnostic_reports_created")
                    or 0
                )
                for e in phase1_events
            )
            total_ob_created = sum(
                int((e.get("details") or {}).get("observations_created") or 0)
                for e in phase1_events
            )
            assert total_dr_created == 1
            assert total_ob_created == 3
        finally:
            verify_session.close()

    finally:
        # Cleanup persisted data (this test bypasses db_session rollback).
        cleanup = Session(bind=seed_conn)
        try:
            cleanup.execute(
                text("DELETE FROM processing_event WHERE ingestion_id = :id"),
                {"id": ingestion_id},
            )
            cleanup.execute(
                text("DELETE FROM observation WHERE ingestion_id = :id"),
                {"id": ingestion_id},
            )
            cleanup.execute(
                text("DELETE FROM diagnostic_report WHERE ingestion_id = :id"),
                {"id": ingestion_id},
            )
            cleanup.execute(
                text("DELETE FROM test WHERE panel_id = :pid"),
                {"pid": panel_id},
            )
            cleanup.execute(
                text("DELETE FROM panel WHERE ingestion_id = :id"),
                {"id": ingestion_id},
            )
            cleanup.execute(
                text("DELETE FROM raw_data WHERE ingestion_id = :id"),
                {"id": ingestion_id},
            )
            cleanup.execute(
                text("DELETE FROM ingestion WHERE ingestion_id = :id"),
                {"id": ingestion_id},
            )
            cleanup.commit()
        finally:
            cleanup.close()
            seed_session.close()
            seed_conn.close()


### PHASE 2 FHIR JSON ###


# Happy path: Phase 2 should populate resource_json and emit success events
def test_phase2_happy_path_writes_json_to_both_tables_and_emits_events(
    db_session,
    fetch_events,
    seed_ingestion,
    seed_panel,
    seed_test,
):

    ingestion = seed_ingestion()
    panel = seed_panel(
        ingestion_id=ingestion.ingestion_id,
        panel_code="LIPID",
        sample_id=f"SAM-{uuid.uuid4()}",
    )

    seed_test(
        panel_id=panel.panel_id,
        row_number=1,
        test_code="INRANGE",
        test_name="In-range Numeric",
        result_raw="100",
        result_value_num=100.0,
        ref_low_raw="0",
        ref_high_raw="200",
        flag_raw="normal",
        unit="mg/dL",
    )
    seed_test(
        panel_id=panel.panel_id,
        row_number=2,
        test_code="OUTRANGE",
        test_name="Out-of-range Numeric",
        result_raw="300",
        result_value_num=300.0,
        ref_low_raw="0",
        ref_high_raw="200",
        flag_raw="normal",
        unit="mg/dL",
    )
    seed_test(
        panel_id=panel.panel_id,
        row_number=3,
        test_code="TEXTONLY",
        test_name="Text-only",
        result_raw="NEGATIVE",
        result_value_num=None,
        ref_low_raw="",
        ref_high_raw="",
        flag_raw="",
        unit=None,
    )

    ok, norm_errors, json_failures = NormalizationJob(
        db_session
    ).run_for_ingestion_id(ingestion.ingestion_id)
    assert ok is True
    assert norm_errors == []
    assert json_failures == []

    drs = (
        db_session.execute(
            select(DiagnosticReport).where(
                DiagnosticReport.ingestion_id == ingestion.ingestion_id
            )
        )
        .scalars()
        .all()
    )
    obs = (
        db_session.execute(
            select(Observation).where(
                Observation.ingestion_id == ingestion.ingestion_id
            )
        )
        .scalars()
        .all()
    )
    assert len(drs) == 1
    assert all(dr.resource_json is not None for dr in drs)
    assert len(obs) == 3
    assert all(o.resource_json is not None for o in obs)

    events = fetch_events(ingestion.ingestion_id)
    event_types = {e["event_type"] for e in events}
    assert "FHIR_JSON_GENERATION_SUCCEEDED" in event_types
    assert "NORMALIZATION_SUCCEEDED" in event_types
    assert "FHIR_JSON_GENERATION_FAILED" not in event_types
    assert "NORMALIZATION_SUCCEEDED_WITH_WARNINGS" not in event_types


# One Observation JSON failure yields ok=True with json_failures and WARN events
def test_phase2_partial_json_failure_returns_ok_with_warnings_and_leaves_one_resource_json_null(
    db_session,
    fetch_events,
    seed_ingestion,
    seed_panel,
    seed_test,
    monkeypatch,
):
    ingestion = seed_ingestion()
    panel = seed_panel(
        ingestion_id=ingestion.ingestion_id,
        panel_code="LIPID",
        sample_id=f"SAM-{uuid.uuid4()}",
    )

    t1 = seed_test(
        panel_id=panel.panel_id,
        row_number=1,
        test_code="INRANGE",
        test_name="In-range Numeric",
        result_raw="100",
        result_value_num=100.0,
        ref_low_raw="0",
        ref_high_raw="200",
        flag_raw="normal",
        unit="mg/dL",
    )
    t2 = seed_test(
        panel_id=panel.panel_id,
        row_number=2,
        test_code="OUTRANGE",
        test_name="Out-of-range Numeric",
        result_raw="300",
        result_value_num=300.0,
        ref_low_raw="0",
        ref_high_raw="200",
        flag_raw="normal",
        unit="mg/dL",
    )
    t3 = seed_test(
        panel_id=panel.panel_id,
        row_number=3,
        test_code="TEXTONLY",
        test_name="Text-only",
        result_raw="NEGATIVE",
        result_value_num=None,
        ref_low_raw="",
        ref_high_raw="",
        flag_raw="",
        unit=None,
    )

    fail_test_id = t2.test_id
    job = NormalizationJob(db_session)
    original_make_observation = job.serializer.make_observation

    def _make_observation_with_failure(ob: Observation):
        if getattr(ob, "test_id", None) == fail_test_id:
            raise RuntimeError("forced serializer error")
        return original_make_observation(ob)

    monkeypatch.setattr(
        job.serializer, "make_observation", _make_observation_with_failure
    )

    ok, norm_errors, json_failures = job.run_for_ingestion_id(
        ingestion.ingestion_id
    )
    assert ok is True
    assert norm_errors == []
    assert len(json_failures) == 1
    assert getattr(json_failures[0], "resource_type", None) == "Observation"

    # DiagnosticReport JSON should be written; one Observation JSON should
    # remain NULL
    dr = (
        db_session.execute(
            select(DiagnosticReport).where(
                DiagnosticReport.ingestion_id == ingestion.ingestion_id
            )
        )
        .scalars()
        .one()
    )
    assert dr.resource_json is not None

    obs = (
        db_session.execute(
            select(Observation).where(
                Observation.ingestion_id == ingestion.ingestion_id
            )
        )
        .scalars()
        .all()
    )
    assert len(obs) == 3
    obs_by_test_id = {o.test_id: o for o in obs}
    assert obs_by_test_id[t1.test_id].resource_json is not None
    assert obs_by_test_id[t2.test_id].resource_json is None
    assert obs_by_test_id[t3.test_id].resource_json is not None

    events = fetch_events(ingestion.ingestion_id)
    event_types = {e["event_type"] for e in events}

    assert "FHIR_JSON_GENERATION_FAILED" in event_types
    assert "NORMALIZATION_SUCCEEDED_WITH_WARNINGS" in event_types
    assert "FHIR_JSON_GENERATION_SUCCEEDED" not in event_types
    assert "NORMALIZATION_SUCCEEDED" not in event_types

    warn_events = [
        e
        for e in events
        if e["event_type"]
        in {
            "FHIR_JSON_GENERATION_FAILED",
            "NORMALIZATION_SUCCEEDED_WITH_WARNINGS",
        }
    ]
    assert all(e.get("severity") == "WARN" for e in warn_events)


# With frozen time, Phase 2 JSON should be semantically identical across reruns
def test_phase2_json_semantic_determinism_rerun_same_ingestion_with_frozen_time(
    db_session,
    freeze_time,
    seed_ingestion,
    seed_panel,
    seed_test,
):

    ingestion = seed_ingestion()
    panel = seed_panel(
        ingestion_id=ingestion.ingestion_id,
        panel_code="LIPID",
        sample_id=f"SAM-{uuid.uuid4()}",
    )

    seed_test(
        panel_id=panel.panel_id,
        row_number=1,
        test_code="INRANGE",
        test_name="In-range Numeric",
        result_raw="100",
        result_value_num=100.0,
        ref_low_raw="0",
        ref_high_raw="200",
        flag_raw="normal",
        unit="mg/dL",
    )
    seed_test(
        panel_id=panel.panel_id,
        row_number=2,
        test_code="OUTRANGE",
        test_name="Out-of-range Numeric",
        result_raw="300",
        result_value_num=300.0,
        ref_low_raw="0",
        ref_high_raw="200",
        flag_raw="normal",
        unit="mg/dL",
    )
    seed_test(
        panel_id=panel.panel_id,
        row_number=3,
        test_code="TEXTONLY",
        test_name="Text-only",
        result_raw="NEGATIVE",
        result_value_num=None,
        ref_low_raw="",
        ref_high_raw="",
        flag_raw="",
        unit=None,
    )

    job = NormalizationJob(db_session)

    ok1, norm_errors1, json_failures1 = job.run_for_ingestion_id(
        ingestion.ingestion_id
    )
    assert ok1 is True
    assert norm_errors1 == []
    assert json_failures1 == []

    drs_1 = (
        db_session.execute(
            select(DiagnosticReport).where(
                DiagnosticReport.ingestion_id == ingestion.ingestion_id
            )
        )
        .scalars()
        .all()
    )
    obs_1 = (
        db_session.execute(
            select(Observation).where(
                Observation.ingestion_id == ingestion.ingestion_id
            )
        )
        .scalars()
        .all()
    )
    dr_json_1 = {dr.diagnostic_report_id: dr.resource_json for dr in drs_1}
    ob_json_1 = {ob.test_id: ob.resource_json for ob in obs_1}

    ok2, norm_errors2, json_failures2 = job.run_for_ingestion_id(
        ingestion.ingestion_id
    )
    assert ok2 is True
    assert norm_errors2 == []
    assert json_failures2 == []

    drs_2 = (
        db_session.execute(
            select(DiagnosticReport).where(
                DiagnosticReport.ingestion_id == ingestion.ingestion_id
            )
        )
        .scalars()
        .all()
    )
    obs_2 = (
        db_session.execute(
            select(Observation).where(
                Observation.ingestion_id == ingestion.ingestion_id
            )
        )
        .scalars()
        .all()
    )
    dr_json_2 = {dr.diagnostic_report_id: dr.resource_json for dr in drs_2}
    ob_json_2 = {ob.test_id: ob.resource_json for ob in obs_2}

    assert dr_json_2 == dr_json_1
    assert ob_json_2 == ob_json_1


def _transient_operational_error() -> OperationalError:
    # Phase retry logic treats OperationalError as retryable.
    return OperationalError("SELECT 1", {}, Exception("transient db error"))


# Phase 1 should retry on retryable database errors. Use OperationalError for
# the test
def test_phase1_retries_on_retryable_db_error_and_emits_retry_attempts_gt_1(
    db_session,
    fetch_events,
    seed_ingestion,
    seed_panel,
    seed_test,
    monkeypatch,
):
    ingestion = seed_ingestion()
    panel = seed_panel(
        ingestion_id=ingestion.ingestion_id,
        panel_code="LIPID",
        sample_id=f"SAM-{uuid.uuid4()}",
    )
    seed_test(
        panel_id=panel.panel_id,
        row_number=1,
        test_code="INRANGE",
        test_name="In-range Numeric",
        result_raw="100",
        result_value_num=100.0,
        ref_low_raw="0",
        ref_high_raw="200",
        flag_raw="normal",
        unit="mg/dL",
    )

    job = NormalizationJob(db_session)
    original_get_panels = job.panel_repo.get_by_ingestion_id
    calls = {"count": 0}

    def _flaky_get_by_ingestion_id(ingestion_id: uuid.UUID):
        calls["count"] += 1
        if calls["count"] == 1:
            raise _transient_operational_error()
        return original_get_panels(ingestion_id)

    monkeypatch.setattr(
        job.panel_repo, "get_by_ingestion_id", _flaky_get_by_ingestion_id
    )

    ok, norm_errors, json_failures = job.run_for_ingestion_id(
        ingestion.ingestion_id
    )
    assert ok is True
    assert norm_errors == []
    assert json_failures == []
    assert calls["count"] >= 2

    events = fetch_events(ingestion.ingestion_id)
    phase1_success = [
        e
        for e in events
        if e["event_type"] == "NORMALIZATION_RELATIONAL_SUCCEEDED"
    ]
    assert len(phase1_success) == 1
    details = phase1_success[0].get("details") or {}
    retry_attempts = details.get("retry_attempts")
    assert retry_attempts is not None and retry_attempts > 1


# Phase 2 should retry on retryable DB errors and still succeed
def test_phase2_retries_on_retryable_db_error_and_emits_attempts_gt_1(
    db_session,
    fetch_events,
    seed_ingestion,
    seed_panel,
    seed_test,
    monkeypatch,
):

    ingestion = seed_ingestion()
    panel = seed_panel(
        ingestion_id=ingestion.ingestion_id,
        panel_code="LIPID",
        sample_id=f"SAM-{uuid.uuid4()}",
    )
    seed_test(
        panel_id=panel.panel_id,
        row_number=1,
        test_code="INRANGE",
        test_name="In-range Numeric",
        result_raw="100",
        result_value_num=100.0,
        ref_low_raw="0",
        ref_high_raw="200",
        flag_raw="normal",
        unit="mg/dL",
    )

    job = NormalizationJob(db_session)
    original_phase2 = job._phase2_persist_fhir_json
    calls = {"count": 0}

    def _flaky_phase2(ingestion_id: uuid.UUID):
        calls["count"] += 1
        if calls["count"] == 1:
            raise _transient_operational_error()
        return original_phase2(ingestion_id)

    monkeypatch.setattr(job, "_phase2_persist_fhir_json", _flaky_phase2)

    ok, norm_errors, json_failures = job.run_for_ingestion_id(
        ingestion.ingestion_id
    )
    assert ok is True
    assert norm_errors == []
    assert json_failures == []
    assert calls["count"] >= 2

    events = fetch_events(ingestion.ingestion_id)
    event_types = {e["event_type"] for e in events}
    assert "FHIR_JSON_GENERATION_SUCCEEDED" in event_types
    assert "NORMALIZATION_SUCCEEDED" in event_types
    assert "FHIR_JSON_GENERATION_FAILED" not in event_types
    assert "NORMALIZATION_SUCCEEDED_WITH_WARNINGS" not in event_types

    phase2_success = [
        e
        for e in events
        if e["event_type"] == "FHIR_JSON_GENERATION_SUCCEEDED"
    ]
    assert len(phase2_success) == 1
    details = phase2_success[0].get("details") or {}
    attempts = details.get("attempts")
    assert attempts is not None and attempts > 1
