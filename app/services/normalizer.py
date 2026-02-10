"""
Normalize parsed Panel/Test models into persistence payloads for FHIR-shaped
storage.
"""

from __future__ import annotations
from typing import Any, Optional
from datetime import datetime, timezone
import uuid

from app.persistence.models.parsing import Panel, Test
from app.services.utils import (
    NormalizationError,
    parse_str_to_num,
    require_aware_datetime,
    require_non_null,
    require_str,
    optional,
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

        now = datetime.now(timezone.utc)

        payload: dict[str, Any] = {
            "ingestion_id": ingestion_id,
            "panel_id": panel_id,
            "patient_id": patient_id,
            "panel_code": panel_code,
            "effective_at": effective_at,
            "normalized_at": now,
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
    )
    - [NEW] flag_system_interpretation (str)
    - [NEW] discrepancy (str)

    Other fields:
    - REQUIRED: diagnostic_report.diagnostic_report_id -> diagnostic_report_id
    - REQUIRED: panel.ingestion_id -> ingestion_id
    - REQUIRED: panel.patient_id -> patient_id
    - REQUIRED: panel.collection_timestamp -> effective_at
    - CREATE: normalized_at
    - status: "FINAL"
    - resource_json: None

    """

    def build_observation_payload(
        self, *, panel: Panel, test: Test, diagnostic_report_id: uuid.UUID
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
        if comparator is not None and comparator not in ALLOWED_COMPARATORS:
            errors.append(
                NormalizationError(
                    model="Test",
                    field="result_comparator",
                    message="invalid comparator",
                )
            )
            comparator = None

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

        if (
            isinstance(value_num, (int, float))
            and isinstance(ref_low_num, (int, float))
            and isinstance(ref_high_num, (int, float))
            and ref_low_num < ref_high_num
        ):
            if value_num > ref_high_num:
                flag_system_interpretation = "HIGH"
            elif value_num < ref_low_num:
                flag_system_interpretation = "LOW"
            else:
                flag_system_interpretation = "NORMAL"
            if analyzer_flag_norm in {"low", "high", "normal"}:
                if analyzer_flag_norm != flag_system_interpretation.casefold():
                    discrepancy = "analyzer and system flag mismatch"

        # TODO: If provided and computed flags differ, add a note to processing_event

        now = datetime.now(timezone.utc)

        payload: dict[str, Any] = {
            "test_id": test_id,
            "diagnostic_report_id": diagnostic_report_id,
            "ingestion_id": ingestion_id,
            "patient_id": patient_id,
            "code": code,
            "display": optional(getattr(test, "test_name", None)),
            "effective_at": effective_at,
            "normalized_at": now,
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


class DiagnosticReportCreateJSON:
    # if fails, still persist DiagnosticReport, keep `resource_json` as None
    # TODO: send FHIR_JSON_GENERATION_FAILED to processing_event (when ready)
    pass


class ObservationReportCreateJSON:
    pass


"""
From chatbot. Use for structure guidance:

# Coordinator entry point
class NormalizationJob:
    def __init__(self, session_factory, dr_norm, obs_norm, fhir_serializer, event_writer, clock):
        self.session_factory = session_factory
        self.dr_norm = dr_norm
        self.obs_norm = obs_norm
        self.fhir_serializer = fhir_serializer
        self.event_writer = event_writer
        self.clock = clock

    def run(self, ingestion_id: UUID) -> None:
        # ---- Phase 0: job start event
        with self.session_factory() as session:
            self.event_writer.log(
                session,
                event_type="NORMALIZATION_STARTED",
                ingestion_id=ingestion_id,
                target_type="INGESTION",
                details={"started_at": self.clock.now_iso()}
            )
            session.commit()

        # ---- Phase 1: relational upserts
        try:
            phase1_stats = self._phase1_relational(ingestion_id)
        except Exception as e:
            with self.session_factory() as session:
                self.event_writer.log(
                    session,
                    event_type="NORMALIZATION_PHASE1_FAILED",
                    ingestion_id=ingestion_id,
                    target_type="INGESTION",
                    details={"error": repr(e)}
                )
                session.commit()
            raise

        with self.session_factory() as session:
            self.event_writer.log(
                session,
                event_type="NORMALIZATION_PHASE1_SUCCEEDED",
                ingestion_id=ingestion_id,
                target_type="INGESTION",
                details=phase1_stats
            )
            session.commit()

        # ---- Phase 2: FHIR JSON projection
        try:
            phase2_stats = self._phase2_generate_json(ingestion_id)
        except Exception as e:
            with self.session_factory() as session:
                self.event_writer.log(
                    session,
                    event_type="FHIR_JSON_PHASE2_FAILED",
                    ingestion_id=ingestion_id,
                    target_type="INGESTION",
                    details={"error": repr(e)}
                )
                session.commit()
            # NOTE: do NOT raise if you want pipeline to continue; or raise if JSON is mandatory
            return

        with self.session_factory() as session:
            self.event_writer.log(
                session,
                event_type="FHIR_JSON_PHASE2_SUCCEEDED",
                ingestion_id=ingestion_id,
                target_type="INGESTION",
                details=phase2_stats
            )
            session.commit()

# PHASE 1 Call both normalization classes inside one transaction
    def _phase1_relational(self, ingestion_id: UUID) -> dict:
        normalized_at = self.clock.now_utc()

        with self.session_factory() as session:
            # Optional: lock ingestion row or ensure status is VALIDATED
            # Optional: write per-phase START event here instead of Phase 0

            dr_count = self.dr_norm.upsert_for_ingestion(
                session=session,
                ingestion_id=ingestion_id,
                normalized_at=normalized_at,
            )

            obs_count = self.obs_norm.upsert_for_ingestion(
                session=session,
                ingestion_id=ingestion_id,
                normalized_at=normalized_at,
            )

            # Integrity check example (cheap + valuable):
            # - Ensure all observations belong to DRs of same ingestion_id, etc.

            session.commit()

        return {
            "normalized_at": normalized_at.isoformat(),
            "diagnostic_reports_upserted": dr_count,
            "observations_upserted": obs_count,
        }

# PHASE 2:generate and persist resource_json via SELECT → serialize → UPDATE

    def _phase2_generate_json(self, ingestion_id: UUID) -> dict:
        generated_at = self.clock.now_utc()
        serializer_version = self.fhir_serializer.version

        with self.session_factory() as session:
            # 1) Load normalized DR + Obs rows for ingestion_id
            diagnostic_reports = self.dr_norm.fetch_for_ingestion(session, ingestion_id)
            observations_by_dr = self.obs_norm.fetch_grouped_by_report(session, ingestion_id)

            # 2) Serialize DR resources
            dr_success = 0
            dr_fail = 0
            for dr in diagnostic_reports:
                try:
                    obs = observations_by_dr.get(dr.diagnostic_report_id, [])
                    resource = self.fhir_serializer.make_diagnostic_report(dr, obs)
                    resource_json = self.fhir_serializer.to_json(resource)

                    self.dr_norm.update_resource_json(
                        session=session,
                        diagnostic_report_id=dr.diagnostic_report_id,
                        resource_json=resource_json,
                        serializer_version=serializer_version,
                        generated_at=generated_at,
                    )
                    dr_success += 1
                except Exception as e:
                    dr_fail += 1
                    self.event_writer.log(
                        session,
                        event_type="FHIR_JSON_RESOURCE_FAILED",
                        ingestion_id=ingestion_id,
                        target_type="DIAGNOSTIC_REPORT",
                        target_id=str(dr.diagnostic_report_id),
                        details={"error": repr(e), "serializer_version": serializer_version},
                    )

            # 3) Serialize each Observation (optional; depends if you store JSON at obs-level too)
            obs_success = 0
            obs_fail = 0
            observations = self.obs_norm.fetch_for_ingestion(session, ingestion_id)
            for ob in observations:
                try:
                    resource = self.fhir_serializer.make_observation(ob)
                    resource_json = self.fhir_serializer.to_json(resource)

                    self.obs_norm.update_resource_json(
                        session=session,
                        observation_id=ob.observation_id,
                        resource_json=resource_json,
                        serializer_version=serializer_version,
                        generated_at=generated_at,
                    )
                    obs_success += 1
                except Exception as e:
                    obs_fail += 1
                    self.event_writer.log(
                        session,
                        event_type="FHIR_JSON_RESOURCE_FAILED",
                        ingestion_id=ingestion_id,
                        target_type="OBSERVATION",
                        target_id=str(ob.observation_id),
                        details={"error": repr(e), "serializer_version": serializer_version},
                    )

            session.commit()

        return {
            "generated_at": generated_at.isoformat(),
            "serializer_version": serializer_version,
            "diagnostic_reports_json_ok": dr_success,
            "diagnostic_reports_json_failed": dr_fail,
            "observations_json_ok": obs_success,
            "observations_json_failed": obs_fail,
        }


"""
