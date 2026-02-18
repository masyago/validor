# ...existing code...


class NormalizationJob:
    # ...existing code...

    def run_for_ingestion_id(
        self, ingestion_id: uuid.UUID
    ) -> tuple[bool, list[NormalizationError], list[JsonBuildFailure]]:
        # ...existing code...

        # Include: counts (e.g., diagnostic_reports_upserted, observations_upserted, normalized_at)
        discrepancy_count = (
            len(phase1_summary.discrepancy_details)
            if (phase1_summary and phase1_summary.discrepancy_details)
            else 0
        )

        # Avoid bloating ProcessingEvent.details; store up to N items (tune as needed).
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
                "attempts": attempt,
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
                # NEW: ingestion-scoped discrepancy rollup
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

        # ...existing code...

        if json_failures:
            emit(
                self.pe_repo,
                ctx,
                event_type=ProcessingEventType.NORMALIZATION_SUCCEEDED_WITH_WARNINGS,
                severity=ProcessingEventSeverity.WARN,
                message="Normalization succeeded with warnings (FHIR JSON failures)",
                details={
                    "failed_phase": "phase2",
                    "fhir_json_failure_count": len(json_failures),
                    "serializer_version": getattr(
                        self.serializer, "full_version", None
                    ),
                    # NEW: carry discrepancy rollup forward (still run/ingestion-scoped)
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
                ),
                # NEW: carry discrepancy rollup forward
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

        # ...existing code...

    def _phase1_normalize_and_persist(
        self, ingestion_id: uuid.UUID, *, ctx: EventContext
    ) -> tuple[bool, list[NormalizationError], Phase1Summary | None]:
        # ...existing code...

        try:
            now = datetime.now(timezone.utc)
            dr_created = 0
            ob_created = 0

            # NEW: define once (prevents UnboundLocalError and prevents reset-per-observation)
            discrepancy_details: list[dict[str, Any]] = []

            for panel in panels:
                dr = self.dr_repo.get_by_panel_id(panel.panel_id)
                if dr is None:
                    dr_payload = dict(dr_payload_by_panel_id[panel.panel_id])
                    dr_payload["normalized_at"] = now
                    dr = DiagnosticReport(**dr_payload)
                    self.dr_repo.create(dr)  # flushes in repo
                    self.session.flush()
                    dr_created += 1

                tests = self.test_repo.get_by_panel_id(panel.panel_id)
                for test in tests:
                    existing_ob = self.obs_repo.get_by_test_id(test.test_id)
                    if existing_ob is not None:
                        continue

                    core = obs_core_by_test_id[test.test_id]
                    obs_payload = self.obs_norm.attach_diagnostic_report_id(
                        core, dr.diagnostic_report_id
                    )
                    obs_payload = dict(obs_payload)
                    obs_payload["normalized_at"] = now

                    ob = Observation(**obs_payload)
                    self.obs_repo.create(ob)
                    self.session.flush()
                    ob_created += 1

                    if getattr(ob, "discrepancy", None) is not None:
                        # Ensure JSON-serializable values in ProcessingEvent.details
                        discrepancy_details.append(
                            {
                                "observation_id": str(ob.observation_id),
                                "test_id": str(ob.test_id),
                                "panel_id": str(panel.panel_id),
                                "code": ob.code,
                                "discrepancy_text": getattr(ob, "discrepancy"),
                                "flag_analyzer_interpretation": getattr(
                                    ob, "flag_analyzer_interpretation", None
                                ),
                                "flag_system_interpretation": getattr(
                                    ob, "flag_system_interpretation", None
                                ),
                            }
                        )

            self.session.commit()
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


# ...existing code...


# ...existing code...
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional
import uuid

# ...existing code...


@dataclass(frozen=True)
class Phase1Summary:
    normalized_at: datetime
    diagnostic_reports_created: int
    observations_created: int


@dataclass(frozen=True)
class Phase2Summary:
    diagnostic_reports_json_written: int
    observations_json_written: int


# ...existing code...


class NormalizationJob:
    # ...existing code...

    def run_for_ingestion_id(
        self, ingestion_id: uuid.UUID
    ) -> tuple[bool, list[NormalizationError], list[JsonBuildFailure]]:
        # ...existing code creating ctx + emit_started...

        self.session.commit()

        max_attempts = 3
        attempt = 0
        ok: bool = False
        norm_errors: list[NormalizationError] = []
        phase1_summary: Phase1Summary | None = None
        phase1_exc: Exception | None = (
            None  # FIX: was referenced but not defined
        )

        while attempt < max_attempts:
            attempt += 1
            try:
                ok, norm_errors, phase1_summary = (
                    self._phase1_normalize_and_persist(
                        ingestion_id,
                        ctx=ctx,
                    )
                )

                if not ok:
                    break
                break

            except Exception as exc:
                phase1_exc = exc
                self.session.rollback()

                if not _is_retryable_exception(exc) or attempt >= max_attempts:
                    norm_errors = [
                        NormalizationError(
                            model="NormalizationJob",
                            field="phase1",
                            message=str(exc),
                        )
                    ]
                    ok = False
                    break

            continue

        if not ok:
            err = phase1_exc or Exception(
                "Phase 1 (relational) normalization failed"
            )
            emit_failed(
                self.pe_repo,
                ctx,
                event_type=ProcessingEventType.NORMALIZATION_RELATIONAL_FAILED,
                error=err,
                message="Phase 1 (relational) normalization failed",
                details={
                    "failed_phase": "phase1",
                    "attempts": attempt,
                    "max_attempts": max_attempts,
                    "normalization_error_count": len(norm_errors),
                    "normalization_errors_sample": [
                        str(ne)
                        for ne in (norm_errors[:10] if norm_errors else [])
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
                    "attempts": attempt,
                    "normalization_error_count": len(norm_errors),
                },
            )
            self.session.commit()
            return False, norm_errors, []

        emit(
            self.pe_repo,
            ctx,
            event_type=ProcessingEventType.NORMALIZATION_RELATIONAL_SUCCEEDED,
            severity=ProcessingEventSeverity.INFO,
            message="Phase 1 (relational) normalization succeeded",
            details={
                "succeeded_phase": "phase1",
                "attempts": attempt,
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
            },
            dedupe_key=_dedupe_key(
                ProcessingEventType.NORMALIZATION_RELATIONAL_SUCCEEDED
            ),
            target_type=ProcessingEventTargetType.INGESTION,
            target_id=None,
            deduped=True,
        )

        self.session.commit()

        # Phase 2 retry loop
        max_attempts = 3
        attempt = 0
        json_failures: list[JsonBuildFailure] = []
        phase2_exc: Exception | None = None
        phase2_summary: Phase2Summary | None = None

        while attempt < max_attempts:
            attempt += 1
            try:
                json_failures, phase2_summary = self._phase2_persist_fhir_json(
                    ingestion_id
                )
                break
            except Exception as exc:
                phase2_exc = exc
                self.session.rollback()

                if not _is_retryable_exception(exc) or attempt >= max_attempts:
                    json_failures = [
                        JsonBuildFailure(
                            resource_type="Phase2",
                            resource_id="",
                            panel_id="",
                            message=str(exc),
                            type=type(exc).__name__,
                        )
                    ]
                    phase2_summary = Phase2Summary(
                        diagnostic_reports_json_written=0,
                        observations_json_written=0,
                    )
                    break

            continue

        if json_failures:
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
                    "failed_count": len(json_failures),
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
                    "error_type": (
                        type(phase2_exc).__name__ if phase2_exc else None
                    ),
                    "error_message": str(phase2_exc) if phase2_exc else None,
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
                    "fhir_json_failure_count": len(json_failures),
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
                    ProcessingEventType.NORMALIZATION_SUCCEEDED_WITH_WARNINGS
                ),
                target_type=ProcessingEventTargetType.INGESTION,
                target_id=None,
                deduped=True,
            )

            self.session.commit()
            return True, [], json_failures

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

        # ...existing code emitting NORMALIZATION_SUCCEEDED...
        self.session.commit()
        return True, [], json_failures

    def _phase1_normalize_and_persist(
        self, ingestion_id: uuid.UUID, *, ctx: EventContext
    ) -> tuple[bool, list[NormalizationError], Phase1Summary | None]:
        # ...existing code that builds panels/payload dicts...

        if errors:
            self.session.rollback()
            return False, errors, None

        try:
            now = datetime.now(timezone.utc)
            dr_created = 0
            ob_created = 0

            for panel in panels:
                dr = self.dr_repo.get_by_panel_id(panel.panel_id)
                if dr is None:
                    dr_payload = dict(dr_payload_by_panel_id[panel.panel_id])
                    dr_payload["normalized_at"] = now
                    dr = DiagnosticReport(**dr_payload)
                    self.dr_repo.create(dr)
                    self.session.flush()
                    dr_created += 1

                tests = self.test_repo.get_by_panel_id(panel.panel_id)
                for test in tests:
                    existing_ob = self.obs_repo.get_by_test_id(test.test_id)
                    if existing_ob is not None:
                        continue

                    core = obs_core_by_test_id[test.test_id]
                    obs_payload = self.obs_norm.attach_diagnostic_report_id(
                        core, dr.diagnostic_report_id
                    )
                    obs_payload = dict(obs_payload)
                    obs_payload["normalized_at"] = now

                    ob = Observation(**obs_payload)
                    self.obs_repo.create(ob)
                    self.session.flush()  # ensure observation_id exists
                    ob_created += 1

                    if getattr(ob, "discrepancy", None) is not None:
                        emit(
                            self.pe_repo,
                            ctx,
                            event_type=ProcessingEventType.NORMALIZATION_SUCCEEDED_WITH_WARNINGS,
                            severity=ProcessingEventSeverity.WARN,
                            message="Observation discrepancy detected",
                            details={
                                "discrepancy": getattr(ob, "discrepancy"),
                                "flag_analyzer_interpretation": getattr(
                                    ob, "flag_analyzer_interpretation", None
                                ),
                                "flag_system_interpretation": getattr(
                                    ob, "flag_system_interpretation", None
                                ),
                            },
                            dedupe_key=(
                                f"{ctx.actor.value}:obs-discrepancy:"
                                f"{ctx.execution_id}:{ob.observation_id}"
                            ),
                            target_type=ProcessingEventTargetType.OBSERVATION,
                            target_id=ob.observation_id,
                            deduped=True,
                        )

            return (
                True,
                [],
                Phase1Summary(
                    normalized_at=now,
                    diagnostic_reports_created=dr_created,
                    observations_created=ob_created,
                ),
            )

        except SQLAlchemyError:
            self.session.rollback()
            raise
        except Exception:
            self.session.rollback()
            raise

    def _phase2_persist_fhir_json(
        self, ingestion_id: uuid.UUID
    ) -> tuple[list[JsonBuildFailure], Phase2Summary]:
        failures: list[JsonBuildFailure] = []
        panels = self.panel_repo.get_by_ingestion_id(ingestion_id)

        dr_json_written = 0
        ob_json_written = 0

        for panel in panels:
            dr = self.dr_repo.get_by_panel_id(panel.panel_id)
            tests = self.test_repo.get_by_panel_id(panel.panel_id)

            observations: list[Observation] = []
            for test in tests:
                ob = self.obs_repo.get_by_test_id(test.test_id)
                if ob is not None:
                    observations.append(ob)

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
                    with self.session.begin_nested():
                        dr_json = self.serializer.make_diagnostic_report(
                            dr, observations
                        )
                        self.dr_repo.update_resource_json(
                            dr.diagnostic_report_id, dr_json
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

            for ob in observations:
                try:
                    with self.session.begin_nested():
                        ob_json = self.serializer.make_observation(ob)
                        self.obs_repo.update_resource_json(
                            ob.observation_id, ob_json
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

        return failures, Phase2Summary(
            diagnostic_reports_json_written=dr_json_written,
            observations_json_written=ob_json_written,
        )


# ...existing code...


# ...existing code...
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional
import uuid

from sqlalchemy.exc import SQLAlchemyError, DBAPIError, OperationalError
from sqlalchemy.orm import Session

from app.persistence.models.normalization import DiagnosticReport, Observation
from app.persistence.models.provenance import (
    ProcessingEventActor,
    ProcessingEventSeverity,
    ProcessingEventTargetType,
    ProcessingEventType,
)
from app.provenance.emitter import (
    EventContext,
    emit,
    emit_started,
    emit_failed,
)

# ...existing code...


@dataclass(frozen=True)
class Phase1Summary:
    normalized_at: datetime
    diagnostic_reports_created: int
    observations_created: int


@dataclass(frozen=True)
class Phase2Summary:
    json_reports_written: int


# ...existing code...


class NormalizationJob:
    # ...existing code...

    def run_for_ingestion_id(
        self, ingestion_id: uuid.UUID
    ) -> tuple[bool, list[NormalizationError], list[JsonBuildFailure]]:
        # ...existing code...

        # retry loop for phase 1 starts
        max_attempts = 3
        attempt = 0
        ok: bool = False
        norm_errors: list[NormalizationError] = []
        phase1_summary: Phase1Summary | None = None
        phase1_exc: Exception | None = (
            None  # FIX: was referenced but not defined
        )

        while attempt < max_attempts:
            attempt += 1
            try:
                ok, norm_errors, phase1_summary = (
                    self._phase1_normalize_and_persist(ingestion_id, ctx=ctx)
                )

                if not ok:
                    break
                break

            except Exception as exc:
                phase1_exc = exc
                self.session.rollback()

                if not _is_retryable_exception(exc) or attempt >= max_attempts:
                    norm_errors = [
                        NormalizationError(
                            model="NormalizationJob",
                            field="phase1",
                            message=str(exc),
                        )
                    ]
                    ok = False
                    break

            continue

        if not ok:
            err = phase1_exc or Exception(
                "Phase 1 (relational) normalization failed"
            )
            emit_failed(
                self.pe_repo,
                ctx,
                event_type=ProcessingEventType.NORMALIZATION_RELATIONAL_FAILED,
                error=err,
                message="Phase 1 (relational) normalization failed",
                details={
                    "failed_phase": "phase1",
                    "attempts": attempt,
                    "max_attempts": max_attempts,
                    "normalization_error_count": len(norm_errors),
                    "normalization_errors_sample": [
                        str(ne)
                        for ne in (norm_errors[:10] if norm_errors else [])
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
                    "attempts": attempt,
                    "normalization_error_count": len(norm_errors),
                },
            )
            self.session.commit()
            return False, norm_errors, []

        emit(
            self.pe_repo,
            ctx,
            event_type=ProcessingEventType.NORMALIZATION_RELATIONAL_SUCCEEDED,
            severity=ProcessingEventSeverity.INFO,
            message="Phase 1 (relational) normalization succeeded",
            details={
                "succeeded_phase": "phase1",
                "attempts": attempt,
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
            },
            dedupe_key=f"{ctx.actor.value}:{ProcessingEventType.NORMALIZATION_RELATIONAL_SUCCEEDED.value}:{ctx.execution_id}",
            target_type=ProcessingEventTargetType.INGESTION,
            target_id=None,
            deduped=True,
        )

        self.session.commit()

        # retry loop for phase 2 starts
        max_attempts = 3
        attempt = 0
        json_failures: list[JsonBuildFailure] = []
        phase2_summary: Phase2Summary | None = None
        phase2_exc: Exception | None = None

        while attempt < max_attempts:
            attempt += 1
            try:
                json_failures, phase2_summary = self._phase2_persist_fhir_json(
                    ingestion_id
                )
                break
            except Exception as exc:
                phase2_exc = exc
                self.session.rollback()

                if not _is_retryable_exception(exc) or attempt >= max_attempts:
                    json_failures = [
                        JsonBuildFailure(
                            resource_type="Phase2",
                            resource_id="",
                            panel_id="",
                            message=str(exc),
                            type=type(exc).__name__,
                        )
                    ]
                    phase2_summary = Phase2Summary(json_reports_written=0)
                    break

            continue

        if json_failures:
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
                    "failed_count": len(json_failures),
                    "json_reports_written": (
                        phase2_summary.json_reports_written
                        if phase2_summary is not None
                        else None
                    ),
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
                    "error_type": (
                        type(phase2_exc).__name__ if phase2_exc else None
                    ),
                    "error_message": str(phase2_exc) if phase2_exc else None,
                },
                dedupe_key=f"{ctx.actor.value}:{ProcessingEventType.FHIR_JSON_GENERATION_FAILED.value}:{ctx.execution_id}",
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
                    "fhir_json_failure_count": len(json_failures),
                    "json_reports_written": (
                        phase2_summary.json_reports_written
                        if phase2_summary is not None
                        else None
                    ),
                    "serializer_version": getattr(
                        self.serializer, "full_version", None
                    ),
                },
                dedupe_key=f"{ctx.actor.value}:{ProcessingEventType.NORMALIZATION_SUCCEEDED_WITH_WARNINGS.value}:{ctx.execution_id}",
                target_type=ProcessingEventTargetType.INGESTION,
                target_id=None,
                deduped=True,
            )

            self.session.commit()
            return True, [], json_failures

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
                "json_reports_written": (
                    phase2_summary.json_reports_written
                    if phase2_summary is not None
                    else None
                ),
                "serializer_version": getattr(
                    self.serializer, "full_version", None
                ),
            },
            dedupe_key=f"{ctx.actor.value}:{ProcessingEventType.FHIR_JSON_GENERATION_SUCCEEDED.value}:{ctx.execution_id}",
            target_type=ProcessingEventTargetType.INGESTION,
            target_id=None,
            deduped=True,
        )

        emit(
            self.pe_repo,
            ctx,
            event_type=ProcessingEventType.NORMALIZATION_SUCCEEDED,
            severity=ProcessingEventSeverity.INFO,
            message="Normalization succeeded",
            details={
                "serializer_full_version": getattr(
                    self.serializer, "full_version", None
                )
            },
            dedupe_key=f"{ctx.actor.value}:{ProcessingEventType.NORMALIZATION_SUCCEEDED.value}:{ctx.execution_id}",
            target_type=ProcessingEventTargetType.INGESTION,
            target_id=None,
            deduped=True,
        )

        self.session.commit()
        return True, [], json_failures

    # Phase 1: Normalize to FHIR-shaped rows, commit to db
    def _phase1_normalize_and_persist(
        self, ingestion_id: uuid.UUID, *, ctx: EventContext
    ) -> tuple[bool, list[NormalizationError], Phase1Summary | None]:
        # ...existing code up to "if errors:" unchanged...

        try:
            now = datetime.now(timezone.utc)
            diagnostic_reports_created = 0
            observations_created = 0

            for panel in panels:
                dr = self.dr_repo.get_by_panel_id(panel.panel_id)
                if dr is None:
                    dr_payload = dict(dr_payload_by_panel_id[panel.panel_id])
                    dr_payload["normalized_at"] = now

                    dr = DiagnosticReport(**dr_payload)
                    self.dr_repo.create(dr)
                    self.session.flush()
                    diagnostic_reports_created += 1

                tests = self.test_repo.get_by_panel_id(panel.panel_id)
                for test in tests:
                    existing_ob = self.obs_repo.get_by_test_id(test.test_id)
                    if existing_ob is not None:
                        continue

                    core = obs_core_by_test_id[test.test_id]
                    obs_payload = self.obs_norm.attach_diagnostic_report_id(
                        core, dr.diagnostic_report_id
                    )
                    obs_payload = dict(obs_payload)
                    obs_payload["normalized_at"] = now

                    ob = Observation(**obs_payload)
                    self.obs_repo.create(ob)
                    self.session.flush()  # ensure ob.observation_id exists
                    observations_created += 1

                    # NEW: discrepancy => emit observation-scoped warning event
                    if getattr(ob, "discrepancy", None) is not None:
                        emit(
                            self.pe_repo,
                            ctx,
                            event_type=ProcessingEventType.NORMALIZATION_SUCCEEDED_WITH_WARNINGS,
                            severity=ProcessingEventSeverity.WARN,
                            message="Observation discrepancy detected",
                            details={
                                "discrepancy": getattr(ob, "discrepancy"),
                                "flag_analyzer_interpretation": getattr(
                                    ob, "flag_analyzer_interpretation", None
                                ),
                                "flag_system_interpretation": getattr(
                                    ob, "flag_system_interpretation", None
                                ),
                            },
                            dedupe_key=(
                                f"{ctx.actor.value}:OBS_DISCREPANCY:{ctx.execution_id}:{ob.observation_id}"
                            ),
                            target_type=ProcessingEventTargetType.OBSERVATION,
                            target_id=ob.observation_id,
                            deduped=True,
                        )

            self.session.commit()
            return (
                True,
                [],
                Phase1Summary(
                    normalized_at=now,
                    diagnostic_reports_created=diagnostic_reports_created,
                    observations_created=observations_created,
                ),
            )

        except SQLAlchemyError:
            self.session.rollback()
            raise
        except Exception:
            self.session.rollback()
            raise

    def _phase2_persist_fhir_json(
        self, ingestion_id: uuid.UUID
    ) -> tuple[list[JsonBuildFailure], Phase2Summary]:
        failures: list[JsonBuildFailure] = []
        panels = self.panel_repo.get_by_ingestion_id(ingestion_id)

        json_reports_written = 0

        for panel in panels:
            dr = self.dr_repo.get_by_panel_id(panel.panel_id)
            tests = self.test_repo.get_by_panel_id(panel.panel_id)

            # Build Observation rows for DR serializer (NOT UUIDs; NOT ingestion_id)
            observations: list[Observation] = []
            for test in tests:
                ob = self.obs_repo.get_by_test_id(test.test_id)
                if ob is not None:
                    observations.append(ob)

            # DiagnosticReport JSON (isolated)
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
                    with self.session.begin_nested():  # SAVEPOINT
                        dr_json_dict = self.serializer.make_diagnostic_report(
                            dr, observations
                        )
                        self.dr_repo.update_resource_json(
                            dr.diagnostic_report_id, dr_json_dict
                        )
                    json_reports_written += 1
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
            for ob in observations:
                try:
                    with self.session.begin_nested():  # SAVEPOINT
                        ob_json_dict = self.serializer.make_observation(ob)
                        self.obs_repo.update_resource_json(
                            ob.observation_id, ob_json_dict
                        )
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

        try:
            self.session.commit()
        except Exception:
            self.session.rollback()
            raise

        return failures, Phase2Summary(
            json_reports_written=json_reports_written
        )


# ...existing code...


# ...existing code...
from dataclasses import dataclass
from datetime import datetime, timezone
import uuid
from typing import Any, Optional

from sqlalchemy.exc import SQLAlchemyError, DBAPIError, OperationalError
from sqlalchemy.orm import Session

from app.persistence.models.normalization import DiagnosticReport, Observation
from app.persistence.models.parsing import Panel, Test
from app.persistence.models.provenance import (
    ProcessingEventActor,
    ProcessingEventSeverity,
    ProcessingEventTargetType,
    ProcessingEventType,
)
from app.provenance.emitter import EventContext, emit, emit_failed

# ...existing code...


@dataclass(frozen=True)
class Phase1Summary:
    normalized_at: datetime
    diagnostic_reports_created: int
    observations_created: int


@dataclass(frozen=True)
class Phase2Summary:
    diagnostic_reports_json_written: int
    observations_json_written: int


# ...existing code...


class NormalizationJob:
    # ...existing code...

    def _phase1_normalize_and_persist(
        self, ingestion_id: uuid.UUID, *, ctx: EventContext
    ) -> tuple[bool, list["NormalizationError"], Phase1Summary | None]:
        # ...existing code that builds payloads and validates...

        if errors:
            self.session.rollback()
            return False, errors, None

        try:
            now = datetime.now(timezone.utc)
            dr_created = 0
            ob_created = 0

            for panel in panels:
                dr = self.dr_repo.get_by_panel_id(panel.panel_id)
                if dr is None:
                    dr_payload = dict(dr_payload_by_panel_id[panel.panel_id])
                    dr_payload["normalized_at"] = now
                    dr = DiagnosticReport(**dr_payload)
                    self.dr_repo.create(dr)
                    self.session.flush()
                    dr_created += 1

                tests = self.test_repo.get_by_panel_id(panel.panel_id)
                for test in tests:
                    existing_ob = self.obs_repo.get_by_test_id(test.test_id)
                    if existing_ob is not None:
                        continue

                    core = obs_core_by_test_id[test.test_id]
                    obs_payload = self.obs_norm.attach_diagnostic_report_id(
                        core, dr.diagnostic_report_id
                    )
                    obs_payload = dict(obs_payload)
                    obs_payload["normalized_at"] = now

                    ob = Observation(**obs_payload)
                    self.obs_repo.create(ob)
                    self.session.flush()  # ensure observation_id is available
                    ob_created += 1

                    # NEW: discrepancy => ProcessingEvent.details must include
                    # discrepancy + both flags
                    if getattr(ob, "discrepancy", None):
                        emit(
                            self.pe_repo,
                            ctx,
                            event_type=ProcessingEventType.NORMALIZATION_SUCCEEDED_WITH_WARNINGS,
                            severity=ProcessingEventSeverity.WARN,
                            message="Observation discrepancy detected",
                            details={
                                "panel_id": str(panel.panel_id),
                                "test_id": str(test.test_id),
                                "discrepancy": getattr(ob, "discrepancy"),
                                "flag_analyzer_interpretation": getattr(
                                    ob, "flag_analyzer_interpretation", None
                                ),
                                "flag_system_interpretation": getattr(
                                    ob, "flag_system_interpretation", None
                                ),
                            },
                            dedupe_key=(
                                f"{ctx.actor.value}:obs-discrepancy:"
                                f"{ctx.execution_id}:{ob.observation_id}"
                            ),
                            target_type=ProcessingEventTargetType.OBSERVATION,
                            target_id=ob.observation_id,
                            deduped=True,
                        )

            return (
                True,
                [],
                Phase1Summary(
                    normalized_at=now,
                    diagnostic_reports_created=dr_created,
                    observations_created=ob_created,
                ),
            )

        except SQLAlchemyError:
            self.session.rollback()
            raise

    def _phase2_persist_fhir_json(
        self, ingestion_id: uuid.UUID
    ) -> tuple[list["JsonBuildFailure"], Phase2Summary]:
        failures: list["JsonBuildFailure"] = []
        panels = self.panel_repo.get_by_ingestion_id(ingestion_id)

        dr_json_written = 0
        ob_json_written = 0

        for panel in panels:
            dr = self.dr_repo.get_by_panel_id(panel.panel_id)
            tests = self.test_repo.get_by_panel_id(panel.panel_id)

            # Build observation rows for DR.result references
            observations: list[Observation] = []
            for test in tests:
                ob = self.obs_repo.get_by_test_id(test.test_id)
                if ob is not None:
                    observations.append(ob)

            if dr is not None:
                try:
                    with self.session.begin_nested():
                        dr_json = self.serializer.make_diagnostic_report(
                            dr, observations
                        )
                        self.dr_repo.update_resource_json(
                            dr.diagnostic_report_id, dr_json
                        )
                    dr_json_written += 1
                except Exception as e:
                    failures.append(
                        JsonBuildFailure(
                            resource_type="DiagnosticReport",
                            resource_id=str(dr.diagnostic_report_id),
                            panel_id=str(panel.panel_id),
                            type=type(e).__name__,
                            message=str(e),
                        )
                    )

            # Observation JSON
            for ob in observations:
                try:
                    with self.session.begin_nested():
                        ob_json = self.serializer.make_observation(ob)
                        self.obs_repo.update_resource_json(
                            ob.observation_id, ob_json
                        )
                    ob_json_written += 1
                except Exception as e:
                    failures.append(
                        JsonBuildFailure(
                            resource_type="Observation",
                            resource_id=str(ob.observation_id),
                            panel_id=str(panel.panel_id),
                            type=type(e).__name__,
                            message=str(e),
                        )
                    )

        return failures, Phase2Summary(
            diagnostic_reports_json_written=dr_json_written,
            observations_json_written=ob_json_written,
        )


# ...existing code...
