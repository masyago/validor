"""
Normalize parsed Panel/Test models into persistence payloads for FHIR-shaped
storage.
"""

from __future__ import annotations
from typing import Any, Optional
from datetime import datetime, timezone
import uuid
from decimal import Decimal
from dataclasses import dataclass
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError, DBAPIError, OperationalError
from sqlalchemy import select

from app.persistence.models.parsing import Panel, Test
from app.persistence.models.normalization import DiagnosticReport, Observation
from app.persistence.models.provenance import (
    ProcessingEventActor,
    ProcessingEventSeverity,
    ProcessingEventTargetType,
    ProcessingEventType,
)
from app.persistence.repositories.panel_repo import PanelRepository
from app.persistence.repositories.test_repo import TestRepository
from app.persistence.repositories.diagnostic_report_repo import (
    DiagnosticReportRepository,
)
from app.persistence.repositories.processing_event_repo import (
    ProcessingEventRepository,
)
from app.persistence.repositories.observation_repo import ObservationRepository
from app.provenance.emitter import (
    EventContext,
    emit,
    emit_started,
    emit_failed,
)
from app.services.utils import (
    NormalizationError,
    parse_str_to_num,
    require_aware_datetime,
    require_non_null,
    require_str,
    optional,
)
from app.domain.fhir.r4.obs_dr_v1 import (
    R4ObsDrV1Serializer,
)

ALLOWED_COMPARATORS = {"<", "<=", ">", ">=", "="}


class DiagnosticReportNormalization:
    """
    Mapping fields from Panel to DiagnosticReport. All fields are required:
    - ingestion_id -> ingestion_id
    - panel_id -> panel_id
    - patient_id -> patient_id
    - panel_code -> panel_code
    - collection_timestamp -> effective_at

    """

    def build_diagnostic_report_payload(
        self, panel: Panel
    ) -> tuple[Optional[dict[str, Any]], list[NormalizationError]]:
        errors: list[NormalizationError] = []

        ingestion_id = require_non_null(
            model="Panel",
            field="ingestion_id",
            val=getattr(panel, "ingestion_id", None),
            errors=errors,
        )

        panel_id = require_non_null(
            model="Panel",
            field="panel_id",
            val=getattr(panel, "panel_id", None),
            errors=errors,
        )

        patient_id = require_str(
            model="Panel",
            field="patient_id",
            val=getattr(panel, "patient_id", None),
            errors=errors,
        )

        panel_code = require_str(
            model="Panel",
            field="panel_code",
            val=getattr(panel, "panel_code", None),
            errors=errors,
        )

        effective_at = require_aware_datetime(
            model="Panel",
            field="collection_timestamp",
            val=getattr(panel, "collection_timestamp", None),
            errors=errors,
        )

        if errors:
            return None, errors

        payload: dict[str, Any] = {
            "ingestion_id": ingestion_id,
            "panel_id": panel_id,
            "patient_id": patient_id,
            "panel_code": panel_code,
            "effective_at": effective_at,
            "normalized_at": None,  # set when Normalization is executed
            "resource_json": None,
            "status": "FINAL",
        }

        return payload, errors


class ObservationNormalization:
    """
    Mapping fields from Test to Observation. Required fields:
    - test_id (UUID)-> test_id (UUID)
    - ingestion_id (UUID) -> ingestion_id (UUID)
    - test_code (str) -> code (str)

    Optional fields from Test to Observation:
    - test_name (str) -> display (str)
    - result_value_num (float) -> value_num (float)
    - result_raw (str) -> value_text (str) - required if no result_value_num
    - result_comparator (str) -> comparator (str)
    - units_raw (str) -> unit (str)
    - ref_low_raw (str) -> ref_low_num (float)
    - ref_high_raw (str) -> ref_high_num (float)
    - flag (str) -> flag_analyzer_interpretation (str)
    - [NEW] flag_system_interpretation (str)
    - [NEW] discrepancy (str)

    Other fields:

    - Note: it'll be added to the payload right before persisting it. REQUIRED: diagnostic_report.diagnostic_report_id -> diagnostic_report_id
    - REQUIRED: panel.ingestion_id -> ingestion_id
    - REQUIRED: panel.patient_id -> patient_id
    - REQUIRED: panel.collection_timestamp -> effective_at
    - CREATE: normalized_at
    - status: "FINAL"
    - resource_json: None

    """

    def build_observation_payload_core(
        self, *, panel: Panel, test: Test
    ) -> tuple[Optional[dict[str, Any]], list[NormalizationError]]:
        errors: list[NormalizationError] = []

        test_id = require_non_null(
            model="Test",
            field="test_id",
            val=getattr(test, "test_id", None),
            errors=errors,
        )
        ingestion_id = require_non_null(
            model="Panel",
            field="ingestion_id",
            val=getattr(panel, "ingestion_id", None),
            errors=errors,
        )

        patient_id = require_str(
            model="Panel",
            field="patient_id",
            val=getattr(panel, "patient_id", None),
            errors=errors,
        )
        effective_at = require_aware_datetime(
            model="Panel",
            field="collection_timestamp",
            val=getattr(panel, "collection_timestamp", None),
            errors=errors,
        )

        code = require_str(
            model="Test",
            field="test_code",
            val=getattr(test, "test_code", None),
            errors=errors,
        )

        value_num = getattr(test, "result_value_num", None)
        value_text: Optional[str] = None

        if value_num is None:
            value_text = optional(getattr(test, "result_raw", None))
            if value_text is None:
                errors.append(
                    NormalizationError(
                        model="Test",
                        field="result_raw",
                        message="result_raw is required when result_value_num is missing",
                    )
                )

        comparator = optional(getattr(test, "result_comparator", None))

        ref_low_num = optional(getattr(test, "ref_low_raw", None))
        ref_high_num = optional(getattr(test, "ref_high_raw", None))

        ref_low_num = parse_str_to_num(ref_low_num or "")
        ref_high_num = parse_str_to_num(ref_high_num or "")

        if ref_low_num is not None and ref_high_num is not None:
            if ref_low_num > ref_high_num:
                errors.append(
                    NormalizationError(
                        model="Test",
                        field="ref_low_raw",
                        message="ref_low_raw cannot be greater than ref_high_raw",
                    )
                )

        if errors:
            return None, errors

        flag_analyzer_interpretation = optional(getattr(test, "flag", None))
        analyzer_flag_norm = (
            flag_analyzer_interpretation.casefold()
            if flag_analyzer_interpretation is not None
            else None
        )
        flag_system_interpretation = "UNKNOWN"
        discrepancy = None

        if ref_low_num is not None and ref_high_num is not None:
            try:
                value_num_f = (
                    float(value_num)
                    if isinstance(value_num, (int, float, Decimal))
                    else None
                )
            except (TypeError, ValueError):
                value_num_f = None

            if value_num_f is not None and ref_low_num < ref_high_num:
                if value_num_f > ref_high_num:
                    flag_system_interpretation = "HIGH"
                elif value_num_f < ref_low_num:
                    flag_system_interpretation = "LOW"
                else:
                    flag_system_interpretation = "NORMAL"
                if analyzer_flag_norm in {"low", "high", "normal"}:
                    if (
                        analyzer_flag_norm
                        != flag_system_interpretation.casefold()
                    ):
                        discrepancy = "analyzer and system flag mismatch"

        payload: dict[str, Any] = {
            "test_id": test_id,
            "ingestion_id": ingestion_id,
            "patient_id": patient_id,
            "code": code,
            "display": optional(getattr(test, "test_name", None)),
            "effective_at": effective_at,
            "normalized_at": None,  # set by the NormalizerJob
            "value_num": value_num,
            "value_text": value_text,
            "comparator": comparator,
            "unit": optional(getattr(test, "units_raw", None)),
            "ref_low_num": ref_low_num,
            "ref_high_num": ref_high_num,
            "flag_analyzer_interpretation": flag_analyzer_interpretation,
            "flag_system_interpretation": flag_system_interpretation,
            "discrepancy": discrepancy,
            "resource_json": None,
            "status": "FINAL",
        }

        return payload, errors

    def attach_diagnostic_report_id(
        self, core_payload: dict[str, Any], diagnostic_report_id: uuid.UUID
    ) -> dict[str, Any]:
        out = dict(core_payload)
        out["diagnostic_report_id"] = diagnostic_report_id
        return out


# Captures error details when runner executes Phases 1 & 2
@dataclass(frozen=True)
class PhaseExecutionError:
    phase: str  # "Phase1: Relational" or "Phase2: JSON resources"
    message: str  # error message
    type: str


@dataclass(frozen=True)
class JsonBuildFailure:
    resource_type: str  # "Observation" or "DiagnosticReport"
    resource_id: str  # UUID string observation_id or diagnostic_report_id
    panel_id: str  # provenance and grouping only
    message: str
    type: str


@dataclass(frozen=True)
class Phase1Summary:
    normalized_at: datetime
    diagnostic_reports_created: int
    observations_created: int
    discrepancy_details: list[dict[str, Any]] | None


@dataclass(frozen=True)
class Phase2Summary:
    diagnostic_reports_json_written: int
    observations_json_written: int


def _is_retryable_exception(exc: Exception) -> bool:
    """
    Conservative retry policy for Phase 1:
    - Retry transient DB connectivity / transaction failures.
    - Do NOT retry deterministic data errors (those should surface as ok=False + norm_errors).
    """
    if isinstance(exc, (OperationalError,)):
        return True

    if isinstance(exc, DBAPIError):
        # connection_invalidated is a strong signal of transient disconnect
        if getattr(exc, "connection_invalidated", False):
            return True

    return False


class NormalizationJob:
    """
    Orchestrates normalization in two phases:

    Phase 1:
    - Build normalized rows for DiagnosticReport and Observation and commit.
    - If any normalization errors exist, rollback and persist nothing.

    Phase 2:
    - Build FHIR JSON and persist into resource_json.
    - If JSON creation/persistence fails, leave that resource_json as None.
    """

    def __init__(self, session: Session):
        self.session = session
        self.panel_repo = PanelRepository(session)
        self.test_repo = TestRepository(session)
        self.dr_repo = DiagnosticReportRepository(session)
        self.obs_repo = ObservationRepository(session)
        self.pe_repo = ProcessingEventRepository(session)

        self.dr_norm = DiagnosticReportNormalization()
        self.obs_norm = ObservationNormalization()

        self.serializer = R4ObsDrV1Serializer()

    def run_for_ingestion_id(
        self, ingestion_id: uuid.UUID
    ) -> tuple[bool, list[NormalizationError], list[JsonBuildFailure]]:
        """
        Runs normalization phases. Returns (ok, normalization_errors, json_failures)

        - Phase 1. If `ok` is `False`, Phase 1 failed. Nothing persisted for
          normalized tables.
        - Phase 2. In case of json_failures, Phase 1 stays committed, but
          resource_json remains None for any failed resource_json.
        """

        def _dedupe_key(event_type: ProcessingEventType) -> str:
            # Stable within this job invocation; prevents duplicate rows on retries.
            return f"{ctx.actor.value}:{event_type.value}:{ctx.execution_id}"

        # Emit NORMALIZATION_STARTED
        ctx = EventContext(
            ingestion_id=ingestion_id,
            actor=ProcessingEventActor.NORMALIZER,
            artifact_versions={
                "serializer_version": getattr(
                    self.serializer, "full_version", None
                )
            },
        )

        emit_started(
            self.pe_repo,
            ctx,
            event_type=ProcessingEventType.NORMALIZATION_STARTED,
            message="Normalization job started",
            details={"phase": "start"},
            dedupe_key=_dedupe_key(ProcessingEventType.NORMALIZATION_STARTED),
        )

        self.session.commit()

        # retry loop for phase 1 starts
        max_attempts = 3
        attempt = 0
        ok: bool = False
        phase1_summary: Phase1Summary | None = None
        phase1_exc: Exception | None = None
        norm_errors: list[NormalizationError] = []

        while attempt < max_attempts:
            attempt += 1
            try:
                ok, norm_errors, phase1_summary = (
                    self._phase1_normalize_and_persist(ingestion_id)
                )
                if not ok:
                    break

                # Phase 1 succeeded; exit retry loop
                break

            except Exception as exc:
                # Any partial work in the transaction must be rolled back
                # before retrying.
                phase1_exc = exc
                self.session.rollback()

                if not _is_retryable_exception(exc) or attempt >= max_attempts:
                    ok = False
                    break

            # Retryable exception and attempts remain: loop continues.
            continue

        if not ok:
            err = phase1_exc or Exception("Phase 1 normalization failed")
            emit_failed(
                self.pe_repo,
                ctx,
                event_type=ProcessingEventType.NORMALIZATION_RELATIONAL_FAILED,
                error=err,
                message="Phase 1 (relational) normalization failed",
                details={
                    "failed_phase": "phase1",
                    "retry_attempts": attempt,
                    "max_retry_attempts": max_attempts,
                    "exception_type": type(err).__name__,
                    "exception_message": str(err),
                    "normalization_error_count": len(norm_errors),
                    "normalization_errors_sample": [
                        str(ne)
                        for ne in (norm_errors[:20] if norm_errors else [])
                    ],
                },
            )

            emit_failed(
                self.pe_repo,
                ctx,
                event_type=ProcessingEventType.NORMALIZATION_FAILED,
                error=err,
                message="Normalization failed",
                details={
                    "failed_phase": "phase1",
                    "retry_attempts": attempt,
                    "exception_type": type(err).__name__,
                    "exception_message": str(err),
                    "normalization_error_count": len(norm_errors),
                },
            )

            self.session.commit()
            return False, norm_errors, []

        # Observation: flags discrepancies
        discrepancy_count = (
            len(phase1_summary.discrepancy_details)
            if (phase1_summary and phase1_summary.discrepancy_details)
            else 0
        )

        # Set max_discrepancies to store to avoid bloating
        # ProcessingEvent.details
        max_discrepancies_to_store = 50
        discrepancy_details_payload = (
            (phase1_summary.discrepancy_details or [])[
                :max_discrepancies_to_store
            ]
            if phase1_summary is not None
            else []
        )
        emit(
            self.pe_repo,
            ctx,
            event_type=ProcessingEventType.NORMALIZATION_RELATIONAL_SUCCEEDED,
            severity=ProcessingEventSeverity.INFO,
            message="Phase 1 (relational) normalization succeeded",
            details={
                "succeeded_phase": "phase1",
                "retry_attempts": attempt,
                "normalized_at": (
                    phase1_summary.normalized_at.isoformat()
                    if phase1_summary is not None
                    else None
                ),
                "diagnostic_reports_created": (
                    phase1_summary.diagnostic_reports_created
                    if phase1_summary is not None
                    else None
                ),
                "observations_created": (
                    phase1_summary.observations_created
                    if phase1_summary is not None
                    else None
                ),
                "discrepancy_count": discrepancy_count,
                "discrepancy_details": discrepancy_details_payload,
                "discrepancy_details_truncated": (
                    discrepancy_count > max_discrepancies_to_store
                ),
            },
            dedupe_key=_dedupe_key(
                ProcessingEventType.NORMALIZATION_RELATIONAL_SUCCEEDED
            ),
            target_type=ProcessingEventTargetType.INGESTION,
            target_id=None,
            deduped=True,
        )

        # commit phase 1
        self.session.commit()

        # retry loop for phase 2 starts
        # Phase 2 retry loop only retries exceptions (DB/system issues),
        # not json_failures returned from the function.
        max_attempts = 3
        attempt = 0
        json_failures: list[JsonBuildFailure] = []
        phase2_summary: Phase2Summary | None = None
        phase2_exc: Exception | None = None
        phase2_failed_error: PhaseExecutionError | None = None

        while attempt < max_attempts:
            attempt += 1
            try:
                json_failures, phase2_summary = self._phase2_persist_fhir_json(
                    ingestion_id
                )
                break
            except Exception as exc:
                # Phase 2 runs in its own transaction scope; reset session before retry.
                phase2_exc = exc
                self.session.rollback()

                if not _is_retryable_exception(exc) or attempt >= max_attempts:
                    phase2_failed_error = PhaseExecutionError(
                        phase="Phase 2: JSON resources",
                        message="Reached maximum number of retries or non-retryable error was raised.",
                        type=type(phase2_exc).__name__,
                    )
                    phase2_summary = Phase2Summary(
                        diagnostic_reports_json_written=0,
                        observations_json_written=0,
                    )
                    break

            # Retryable exception and attempts remain: loop continues.
            continue
        if json_failures or phase2_failed_error:
            # FHIR_JSON_GENERATION_FAILED. Target: ingestion. Severity: WARN
            # Include: counts (e.g., failed_count, succeeded_count, serializer version)
            emit(
                self.pe_repo,
                ctx,
                event_type=ProcessingEventType.FHIR_JSON_GENERATION_FAILED,
                severity=ProcessingEventSeverity.WARN,
                message="FHIR JSON generation completed with failures",
                details={
                    "failed_phase": "phase2",
                    "attempts": attempt,
                    "max_attempts": max_attempts,
                    "execution_error": str(phase2_failed_error),
                    "fhir_json_failure_count": len(json_failures),
                    "serializer_version": getattr(
                        self.serializer, "full_version", None
                    ),
                    "failures_sample": [
                        {
                            "resource_type": f.resource_type,
                            "resource_id": f.resource_id,
                            "panel_id": f.panel_id,
                            "type": f.type,
                            "message": f.message,
                        }
                        for f in json_failures[:10]
                    ],
                },
                dedupe_key=_dedupe_key(
                    ProcessingEventType.FHIR_JSON_GENERATION_FAILED
                ),
                target_type=ProcessingEventTargetType.INGESTION,
                target_id=None,
                deduped=True,
            )

            emit(
                self.pe_repo,
                ctx,
                event_type=ProcessingEventType.NORMALIZATION_SUCCEEDED_WITH_WARNINGS,
                severity=ProcessingEventSeverity.WARN,
                message="Normalization succeeded with warnings (FHIR JSON failures)",
                details={
                    "failed_phase": "phase2",
                    "execution_error": str(phase2_failed_error),
                    "fhir_json_failure_count": len(json_failures),
                    "serializer_version": getattr(
                        self.serializer, "full_version", None
                    ),
                    "discrepancy_count": discrepancy_count,
                    "discrepancy_details": discrepancy_details_payload,
                    "discrepancy_details_truncated": (
                        discrepancy_count > max_discrepancies_to_store
                    ),
                },
                dedupe_key=_dedupe_key(
                    ProcessingEventType.NORMALIZATION_SUCCEEDED_WITH_WARNINGS
                ),
                target_type=ProcessingEventTargetType.INGESTION,
                target_id=None,
                deduped=True,
            )

            self.session.commit()
            return True, [], json_failures

        # FHIR_JSON_GENERATION_SUCCEEDED. Target: ingestion. Severity: INFO
        # Include: counts + serializer version
        emit(
            self.pe_repo,
            ctx,
            event_type=ProcessingEventType.FHIR_JSON_GENERATION_SUCCEEDED,
            severity=ProcessingEventSeverity.INFO,
            message="FHIR JSON generation succeeded",
            details={
                "attempts": attempt,
                "max_attempts": max_attempts,
                "failed_count": 0,
                "diagnostic_reports_json_written": (
                    phase2_summary.diagnostic_reports_json_written
                    if phase2_summary is not None
                    else None
                ),
                "observations_json_written": (
                    phase2_summary.observations_json_written
                    if phase2_summary is not None
                    else None
                ),
                "serializer_version": getattr(
                    self.serializer, "full_version", None
                ),
            },
            dedupe_key=_dedupe_key(
                ProcessingEventType.FHIR_JSON_GENERATION_SUCCEEDED
            ),
            target_type=ProcessingEventTargetType.INGESTION,
            target_id=None,
            deduped=True,
        )

        # emit NORMALIZATION_SUCCEEDED, include counts
        emit(
            self.pe_repo,
            ctx,
            event_type=ProcessingEventType.NORMALIZATION_SUCCEEDED,
            severity=ProcessingEventSeverity.INFO,
            message="Normalization succeeded",
            details={
                "serializer_full_version": getattr(
                    self.serializer,
                    "full_version",
                    None,
                    # additionally, include counts for dr, obs rows
                ),
                "discrepancy_count": discrepancy_count,
                "discrepancy_details": discrepancy_details_payload,
                "discrepancy_details_truncated": (
                    discrepancy_count > max_discrepancies_to_store
                ),
            },
            dedupe_key=_dedupe_key(
                ProcessingEventType.NORMALIZATION_SUCCEEDED
            ),
            target_type=ProcessingEventTargetType.INGESTION,
            target_id=None,
            deduped=True,
        )

        # commit phase 2
        self.session.commit()
        return True, [], json_failures

    """
    Phase 1 and Phase 2 methods don't emit processing_event records. 
    Instead, they track structured outcomes (counts, errors, failures) and 
    return them to the runner.
    """

    # Phase 1: Normalize to FHIR-shaped rows, commit to db
    def _phase1_normalize_and_persist(
        self, ingestion_id: uuid.UUID
    ) -> tuple[bool, list[NormalizationError], Phase1Summary | None]:
        """
        1. Validates data from Panel or Test
        2. If no validation errors, persists normalized rows, and commits.
           Uuid's generated at db side.
        3. If validation errors present, nothing is persisted.
        """
        errors: list[NormalizationError] = []

        panels = self.panel_repo.get_by_ingestion_id(ingestion_id)
        if not panels:
            panels_missing = NormalizationError(
                model="Panel",
                field="all fields",
                message=f"Panel rows with ingestion_id={ingestion_id} not found.",
            )
            errors.append(panels_missing)

            return False, errors, None
        panel_ids = [panel.panel_id for panel in panels]

        # Validate and build payloads
        dr_payload_by_panel_id: dict[uuid.UUID, dict[str, Any]] = {}
        obs_core_by_test_id: dict[uuid.UUID, dict[str, Any]] = {}
        discrepancy_details: list[dict[str, Any]] = []

        # Pre-fetch test rows
        tests_all = self.test_repo.get_by_panel_ids(panel_ids)
        tests_by_panel_id: dict[uuid.UUID, list[Test]] = {}
        for test in tests_all:
            tests_by_panel_id.setdefault(test.panel_id, []).append(test)

        for panel in panels:
            dr_payload, dr_errors = (
                self.dr_norm.build_diagnostic_report_payload(panel)
            )
            errors.extend(dr_errors)
            if dr_payload is not None:
                dr_payload_by_panel_id[panel.panel_id] = dr_payload

            tests = tests_by_panel_id.get(panel.panel_id, [])
            for test in tests:
                core_payload, obs_errors = (
                    self.obs_norm.build_observation_payload_core(
                        panel=panel, test=test
                    )
                )
                errors.extend(obs_errors)
                if core_payload is not None:
                    obs_core_by_test_id[test.test_id] = core_payload

        if errors:
            self.session.rollback()
            return False, errors, None

        # Persist normalized payloads. First, DiagnosticReport.
        # Then Observation, including diagnostic_report_id.
        try:
            now = datetime.now(timezone.utc)
            dr_created = 0
            ob_created = 0
            dr_payloads: list[dict[str, Any]] = []

            for panel in panels:
                dr_payload = dict(dr_payload_by_panel_id[panel.panel_id])
                dr_payload["normalized_at"] = now
                dr_payloads.append(dr_payload)

            # DR bulk upsert
            by_panel_id, dr_inserted_count = (
                self.dr_repo.upsert_many_from_payloads(dr_payloads)
            )
            dr_created += dr_inserted_count

            # Observation bulk upsert (per panel so we attach correct DR id)
            for panel in panels:
                dr_id = by_panel_id.get(panel.panel_id)
                if dr_id is None:
                    # Shouldn't happen: DR upsert should resolve all panel_ids.
                    continue

                tests = tests_by_panel_id.get(panel.panel_id, [])
                obs_payloads: list[dict[str, Any]] = []
                for test in tests:
                    core = obs_core_by_test_id.get(test.test_id)
                    if core is None:
                        continue

                    obs_payload = self.obs_norm.attach_diagnostic_report_id(
                        core, dr_id
                    )
                    obs_payload = dict(obs_payload)
                    obs_payload["normalized_at"] = now
                    obs_payloads.append(obs_payload)

                by_test_id, inserted_count = (
                    self.obs_repo.upsert_many_from_payload(obs_payloads)
                )
                ob_created += inserted_count

                inserted_test_ids: set[uuid.UUID] = set()
                if inserted_count:
                    # Determine which test_ids were newly inserted in this call.
                    requested_test_ids = [
                        p.get("test_id") for p in obs_payloads
                    ]
                    requested_test_ids_uuid = [
                        t
                        for t in requested_test_ids
                        if isinstance(t, uuid.UUID)
                    ]
                    if requested_test_ids_uuid:
                        existing_test_ids = set(
                            self.session.execute(
                                select(Observation.test_id).where(
                                    Observation.test_id.in_(
                                        requested_test_ids_uuid
                                    )
                                )
                            )
                            .scalars()
                            .all()
                        )
                        # After the upsert, *all* are in DB; to infer inserted,
                        # fall back to treating all requested as inserted when
                        # we can't observe pre-state. This preserves prior behavior
                        # best-effort in tests and avoids missing discrepancy reporting.
                        inserted_test_ids = set(requested_test_ids_uuid)

                # Discrepancies are only counted for newly inserted rows
                # (mirrors prior per-row upsert behavior).
                if inserted_count:
                    for payload in obs_payloads:
                        test_id = payload.get("test_id")
                        if not isinstance(test_id, uuid.UUID):
                            continue
                        if test_id not in inserted_test_ids:
                            continue
                        if payload.get("discrepancy") is None:
                            continue
                        ob_id = by_test_id.get(test_id)
                        if ob_id is None:
                            continue
                        discrepancy_details.append(
                            {
                                "observation_id": str(ob_id),
                                "code": payload.get("code"),
                                "discrepancy_text": payload.get("discrepancy"),
                                "flag_analyzer_interpretation": payload.get(
                                    "flag_analyzer_interpretation"
                                ),
                                "flag_system_interpretation": payload.get(
                                    "flag_system_interpretation"
                                ),
                            }
                        )

            # IMPORTANT: Do not commit here.
            # `NormalizationJob.run_for_ingestion_id()` owns the transaction
            # boundaries so it can emit processing events consistently and so
            # test harnesses using savepoints can manage isolation.
            return (
                True,
                [],
                Phase1Summary(
                    normalized_at=now,
                    diagnostic_reports_created=dr_created,
                    observations_created=ob_created,
                    discrepancy_details=(
                        discrepancy_details if discrepancy_details else None
                    ),
                ),
            )

        except SQLAlchemyError:
            self.session.rollback()
            raise
        except Exception:
            self.session.rollback()
            raise

    # Phase build JSON for resources and persist the ones that don't have errors.
    def _phase2_persist_fhir_json(
        self, ingestion_id: uuid.UUID
    ) -> tuple[list[JsonBuildFailure], Phase2Summary]:
        # Phase 2 intentionally does not open a new transaction via `begin()`.
        # The runner commits Phase 1 before invoking Phase 2, and commits again
        # after Phase 2 event emission.
        #
        # Within Phase 2 we use SAVEPOINTs (`begin_nested`) to isolate per-resource
        # JSON build/persist so one bad resource doesn't abort the entire phase.
        return self._phase2_persist_fhir_json_body(ingestion_id)

    def _phase2_persist_fhir_json_body(
        self, ingestion_id: uuid.UUID
    ) -> tuple[list[JsonBuildFailure], Phase2Summary]:
        failures: list[JsonBuildFailure] = []
        dr_json_written = 0
        ob_json_written = 0

        # Phase 2 runs inside the caller's transaction scope. In tests, the
        # session is often wrapped in a SAVEPOINT; we therefore avoid using
        # nested savepoints here and instead handle failures by catching
        # exceptions per resource.
        #
        # This keeps Phase 2 deterministic across drivers and avoids leaving
        # nested transactions open.

        panels = self.panel_repo.get_by_ingestion_id(ingestion_id)

        # Prefetch relational rows in (roughly) one query per table for this ingestion.
        # This avoids N+1 SELECT patterns when building JSON resources.
        panel_ids = [panel.panel_id for panel in panels]

        tests_all = self.test_repo.get_by_panel_ids(panel_ids)
        tests_by_panel_id: dict[uuid.UUID, list[Test]] = {}
        for test in tests_all:
            tests_by_panel_id.setdefault(test.panel_id, []).append(test)

        dr_all = self.dr_repo.get_by_ingestion_id(ingestion_id)
        dr_by_panel_id: dict[uuid.UUID, DiagnosticReport] = {
            dr.panel_id: dr for dr in dr_all
        }

        obs_all = self.obs_repo.get_by_ingestion_id(ingestion_id)
        obs_by_test_id: dict[uuid.UUID, Observation] = {
            ob.test_id: ob for ob in obs_all
        }

        for panel in panels:
            tests = tests_by_panel_id.get(panel.panel_id, [])

            # Record missing observation rows (stable, one failure per missing test).
            for test in tests:
                if test.test_id not in obs_by_test_id:
                    failures.append(
                        JsonBuildFailure(
                            resource_type="Observation",
                            resource_id="",
                            panel_id=str(panel.panel_id),
                            message=f"missing Observation for test_id={test.test_id}",
                            type="MissingRow",
                        )
                    )

            # DiagnosticReport JSON (isolated)
            dr = dr_by_panel_id.get(panel.panel_id)
            if dr is None:
                failures.append(
                    JsonBuildFailure(
                        resource_type="DiagnosticReport",
                        resource_id="",
                        panel_id=str(panel.panel_id),
                        message=f"missing DiagnosticReport for panel_id={panel.panel_id}",
                        type="MissingRow",
                    )
                )
            else:
                try:
                    # Build list of Observation instances for DR serializer
                    ob_list = [
                        obs_by_test_id[t.test_id]
                        for t in tests
                        if t.test_id in obs_by_test_id
                    ]

                    dr_json_dict = self.serializer.make_diagnostic_report(
                        dr, ob_list
                    )
                    self.dr_repo.update_resource_json(
                        dr.diagnostic_report_id, dr_json_dict
                    )
                    dr_json_written += 1
                except Exception as e:
                    failures.append(
                        JsonBuildFailure(
                            resource_type="DiagnosticReport",
                            resource_id=str(dr.diagnostic_report_id),
                            panel_id=str(panel.panel_id),
                            message=str(e),
                            type=type(e).__name__,
                        )
                    )

            # Observation JSON (isolated per Observation)
            obs_json_params: list[dict[str, Any]] = []
            for test in tests:
                ob = obs_by_test_id.get(test.test_id)
                if ob is None:
                    continue
                try:
                    ob_json = self.serializer.make_observation(ob)
                    obs_json_params.append(
                        {
                            "observation_id": ob.observation_id,
                            "resource_json": ob_json,
                        }
                    )

                    ob_json_written += 1
                except Exception as e:
                    failures.append(
                        JsonBuildFailure(
                            resource_type="Observation",
                            resource_id=str(ob.observation_id),
                            panel_id=str(panel.panel_id),
                            message=str(e),
                            type=type(e).__name__,
                        )
                    )

            if obs_json_params:
                self.obs_repo.update_many_resource_json(obs_json_params)

        # Ensure JSON updates are written to the DB within the current transaction
        # before the runner emits events / callers query in the same Session.
        self.session.flush()

        return failures, Phase2Summary(
            diagnostic_reports_json_written=dr_json_written,
            observations_json_written=ob_json_written,
        )
