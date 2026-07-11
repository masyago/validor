from langchain_core.documents import Document

from app.services.parser import CanonicalAnalyzerCsvParser
from app.services.validator import (
    PanelValidation,
    TestValidation,
    RowValidationError,
)
from app.persistence.repositories.raw_data_repo import RawDataRepository
from app.persistence.repositories.ingestion_repo import IngestionRepository
from app.persistence.repositories.diagnostic_report_repo import (
    DiagnosticReportRepository,
)
from app.persistence.repositories.observation_repo import ObservationRepository
from app.persistence.repositories.panel_repo import PanelRepository
from app.persistence.repositories.test_repo import TestRepository
from app.persistence.models.parsing import Panel, Test
from app.services.normalizer import NormalizationJob
from app.ai.ai_orchestration import (
    AIEnrichmentRequest,
    ObservationContext,
    orchestrate_ai_enrichment,
)
from app.ai.content_versions.ai_annotation_content_v1_0_0 import (
    build_rejected_ai_annotation_audit,
)
from app.ai.patient_message_orchestration import (
    PatientMessageDraftRequest,
    orchestrate_patient_message_draft,
)
from app.ai.content_versions.patient_message_content_v1_2_0 import (
    build_rejected_patient_message_audit,
)
from app.persistence.repositories.patient_message_repo import (
    PatientMessageRepository,
)
from app.persistence.models.patient_message import (
    PatientMessage,
    PatientMessageReviewStatus,
    PatientMessageValidationStatus,
)

import uuid
from dataclasses import asdict, dataclass, is_dataclass
from typing import Any, Optional

from sqlalchemy.exc import MultipleResultsFound, NoResultFound, SQLAlchemyError

from app.persistence.models.provenance import (
    ProcessingEventActor,
    ProcessingEventSeverity,
    ProcessingEventType,
)
from app.persistence.repositories.processing_event_repo import (
    ProcessingEventRepository,
)
from app.persistence.repositories.ai_annotation_repo import (
    AiAnnotationRepository,
)
from app.persistence.repositories.patient_repo import PatientRepository
from app.persistence.repositories.ai_generation_job_repo import (
    AiGenerationJobRepository,
)
from app.persistence.models.ai import (
    AIAnnotationType,
    AIAnnotationValidationStatus,
    AiAnnotation,
)
from app.persistence.models.patient import AiGenerationJobType
from app.services.synthetic_patient import synthetic_patient_fields
from app.provenance.emitter import (
    EventContext,
    emit,
    emit_failed,
    emit_started,
)

PanelPayload = dict[str, Any]
TestPayload = dict[str, Any]


@dataclass(frozen=True)
class PanelPackage:
    """
    Validated persistence unit: one Panel row and N associated Test rows
    """

    panel_payload: PanelPayload
    test_payloads: list[TestPayload]
    group_key: Optional[tuple[str, str | None, Any]] = (
        None  # (panel_code, sample_id, collection_timestamp)
    )


class IngestionService:
    def __init__(self, session):  # sessions received from API
        self.session = session  # not sure if it's ok
        self.raw_repo = RawDataRepository(session)
        self.ingestion_repo = IngestionRepository(session)
        self.diagnostic_report_repo = DiagnosticReportRepository(session)
        self.observation_repo = ObservationRepository(session)
        self.panel_repo = PanelRepository(session)
        self.test_repo = TestRepository(session)
        self.pe_repo = ProcessingEventRepository(session)
        self.ai_annotation_repo = AiAnnotationRepository(session)
        self.patient_repo = PatientRepository(session)
        self.ai_generation_job_repo = AiGenerationJobRepository(session)
        self.patient_message_repo = PatientMessageRepository(session)

    def _dedupe_key(
        self,
        ctx: EventContext,
        event_type: ProcessingEventType,
        *,
        error_code: str | None = None,
    ) -> str:
        base = f"{ctx.actor.value}:{event_type.value}:{ctx.execution_id}"
        return f"{base}:{error_code}" if error_code else base

    def _emit_stage_failed(
        self,
        ctx: EventContext,
        *,
        event_type: ProcessingEventType,
        error_code: str,
        error: Exception,
        message: str,
        details: dict[str, Any] | None = None,
        do_commit: bool = True,
    ) -> None:
        payload = dict(details or {})
        payload["error_code"] = error_code

        emit_failed(
            self.pe_repo,
            ctx,
            event_type=event_type,
            error=error,
            message=message,
            details=payload,
            dedupe_key=self._dedupe_key(
                ctx, event_type, error_code=error_code
            ),
            deduped=True,
        )

        if do_commit:
            self.session.commit()

    def _errors_to_json(self, errors: list[Any]) -> list[dict[str, Any]]:
        """
        Convert RowValidationError (and similar) objects into JSON-safe dicts.
        """
        out: list[dict[str, Any]] = []
        for e in errors:
            if isinstance(e, RowValidationError):
                out.append(
                    {
                        "row_number": e.row_number,
                        "field": e.field,
                        "message": e.message,
                    }
                )
            elif is_dataclass(e) and not isinstance(e, type):
                # e.g., NormalizationError from app.services.utils
                out.append(asdict(e))
            elif isinstance(e, dict):
                out.append(e)
            else:
                out.append({"message": str(e)})
        return out

    def get_csv_file(self, ingestion_id):
        csv_content_bytes = self.raw_repo.get_content_bytes(ingestion_id)
        return csv_content_bytes

    def parse_csv_file(self, csv_content_bytes):
        rows = CanonicalAnalyzerCsvParser().parse(csv_content_bytes)
        return rows

    def validate_panel_tests(
        self, rows: list[dict[str, str]]
    ) -> tuple[list[PanelPackage], list[RowValidationError]]:
        """
        Returns:
        - panel_packages: validated payload for persistence (Panel and Test) excluding ingestion_id (Panel) and panel_id (Test)
        OR
        - if validation error present, returns validation_errors: list[RowValidationError]
        """
        validation_errors = []
        panel_validation = PanelValidation()
        test_validation = TestValidation()

        groups, group_errors = panel_validation.determine_panels(rows)

        # change ingestion status to FAILED VALIDATION.
        # send the errors to Ingestion model/db table
        validation_errors.extend(group_errors)

        panel_packages: list[PanelPackage] = []

        for key, group in groups.items():
            panel_payload = group["panel_payload"]
            panel_rows = group["panel_rows"]

            test_payloads: list[TestPayload] = []
            for row_number, row in panel_rows:
                test_payload, test_errors = test_validation.build_test_payload(
                    row, row_number
                )
                if test_errors:
                    validation_errors.extend(test_errors)
                    continue

                # if no errors, payload must exist
                assert test_payload is not None
                test_payloads.append(test_payload)

            panel_packages.append(
                PanelPackage(
                    panel_payload=panel_payload,
                    test_payloads=test_payloads,
                    group_key=key,
                )
            )

        return panel_packages, validation_errors

    def generate_payload_for_db(self, rows: list[dict[str, str]]):
        """
        Convenience wrapper: validates and returns only the payload needed for persistence.
        """
        panel_packages, validation_errors = self.validate_panel_tests(rows)

        if validation_errors:
            return None, validation_errors

        return panel_packages, []

    def _get_ai_enrichment_request(
        self, ingestion_id: uuid.UUID
    ) -> AIEnrichmentRequest:
        current_observations = self.observation_repo.get_by_ingestion_id(
            ingestion_id
        )
        if not current_observations:
            raise RuntimeError(
                "Normalization succeeded but no Observation rows were found"
            )

        patient_ids = {obs.patient_id for obs in current_observations}
        if len(patient_ids) != 1:
            raise RuntimeError(
                "Expected exactly one patient_id for AI enrichment"
            )

        patient_id = next(iter(patient_ids))
        diagnostic_reports = self.diagnostic_report_repo.get_by_ingestion_id(
            ingestion_id
        )
        analyte_codes = sorted({obs.code for obs in current_observations})
        # patient_id is used here, server-side only, to fetch history.
        historical_observations = (
            self.observation_repo.get_latest_by_patient_id(
                patient_id,
                exclude_ingestion_id=ingestion_id,
                codes=analyte_codes,
                per_code_limit=10,
            )
        )

        # Mint a random, job-scoped token and store token->patient on the
        # trusted side. The AI layer only ever sees this correlation_id.
        correlation_id = uuid.uuid4()
        self.ai_generation_job_repo.create(
            correlation_id=correlation_id,
            job_type=AiGenerationJobType.ENRICHMENT,
            patient_id=patient_id,
            ingestion_id=ingestion_id,
        )

        return AIEnrichmentRequest(
            ingestion_id=ingestion_id,
            correlation_id=correlation_id,
            panel_codes=[report.panel_code for report in diagnostic_reports],
            collected_at=max(obs.effective_at for obs in current_observations),
            current_observations=[
                ObservationContext(
                    code=obs.code,
                    display=obs.display,
                    value_num=obs.value_num,
                    value_text=obs.value_text,
                    unit=obs.unit,
                    ref_low_num=obs.ref_low_num,
                    ref_high_num=obs.ref_high_num,
                    interpretation=obs.flag_system_interpretation,
                    effective_at=obs.effective_at,
                )
                for obs in current_observations
            ],
            historical_observations=[
                ObservationContext(
                    code=obs.code,
                    display=obs.display,
                    value_num=obs.value_num,
                    value_text=obs.value_text,
                    unit=obs.unit,
                    ref_low_num=obs.ref_low_num,
                    ref_high_num=obs.ref_high_num,
                    interpretation=obs.flag_system_interpretation,
                    effective_at=obs.effective_at,
                )
                for obs in historical_observations
            ],
        )

    def _emit_ai_stage_event(
        self,
        ctx: EventContext,
        *,
        event_type: ProcessingEventType,
        severity: ProcessingEventSeverity,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        emit(
            self.pe_repo,
            ctx,
            event_type=event_type,
            severity=severity,
            message=message,
            details=details,
            dedupe_key=self._dedupe_key(ctx, event_type),
            deduped=True,
        )

    def _emit_ai_stage_result(self, ctx: EventContext, ai_result) -> None:
        base_details = {
            "provider": ai_result.provider,
            "model_id": ai_result.model_id,
            "prompt_version": ai_result.prompt_version,
            "temperature": ai_result.temperature,
            "content_schema_version": ai_result.content_schema_version,
            "input_hash": ai_result.input_hash,
        }

        if ai_result.failure_reason is not None:
            self._emit_ai_stage_event(
                ctx,
                event_type=ProcessingEventType.AI_ENRICHMENT_FAILED,
                severity=ProcessingEventSeverity.ERROR,
                message="AI enrichment configuration is missing or invalid",
                details={
                    **base_details,
                    "failure_reason": ai_result.failure_reason,
                },
            )
            return

        if ai_result.llm_response_content is not None:
            self._emit_ai_stage_event(
                ctx,
                event_type=ProcessingEventType.AI_ENRICHMENT_SUCCEEDED,
                severity=ProcessingEventSeverity.INFO,
                message="AI enrichment succeeded",
                details=base_details,
            )
            return

        if ai_result.llm_response_text is not None:
            self._emit_ai_stage_event(
                ctx,
                event_type=ProcessingEventType.AI_ENRICHMENT_FAILED,
                severity=ProcessingEventSeverity.ERROR,
                message="AI enrichment response failed schema validation",
                details={
                    **base_details,
                    "rejection_reason": ai_result.rejection_reason,
                },
            )
            return

        self._emit_ai_stage_event(
            ctx,
            event_type=ProcessingEventType.AI_ENRICHMENT_SKIPPED,
            severity=ProcessingEventSeverity.WARN,
            message="AI enrichment produced no annotation",
            details=base_details,
        )

    def _persist_ai_annotation(
        self,
        *,
        ai_request: AIEnrichmentRequest,
        ai_result,
    ) -> None:
        if ai_result is None:
            return

        if ai_result.llm_response_content is not None:
            annotation_type = AIAnnotationType(
                ai_result.llm_response_content.annotation_type
            )
            content_json = ai_result.llm_response_content.model_dump(
                mode="json"
            )
            validation_status = AIAnnotationValidationStatus.ACCEPTED
            rejection_reason = None
            validated_at = ai_result.created_at
        elif ai_result.llm_response_text is not None:
            annotation_type = None
            audit_payload = build_rejected_ai_annotation_audit(
                raw_llm_response=ai_result.llm_response_text,
                rejection_reason=ai_result.rejection_reason
                or "AI annotation validation failed",
            )
            content_json = audit_payload.model_dump(mode="json")
            validation_status = AIAnnotationValidationStatus.REJECTED
            rejection_reason = audit_payload.rejection_reason
            validated_at = ai_result.created_at
        else:
            return

        ai_annotation = AiAnnotation(
            ingestion_id=ai_request.ingestion_id,
            annotation_type=annotation_type,
            content_json=content_json,
            provider=ai_result.provider,
            model_id=ai_result.model_id,
            prompt_version=ai_result.prompt_version,
            temperature=ai_result.temperature,
            content_schema_version=ai_result.content_schema_version,
            input_hash=ai_result.input_hash,
            correlation_id=ai_request.correlation_id,
            created_at=ai_result.created_at,
            validation_status=validation_status,
            validated_at=validated_at,
            rejection_reason=rejection_reason,
        )
        self.ai_annotation_repo.create(ai_annotation)

    def _upsert_patient(self, patient_id: str) -> None:
        """Idempotently ensure a synthetic patient row exists for patient_id."""
        fields = synthetic_patient_fields(patient_id)
        self.patient_repo.upsert(
            patient_id=patient_id,
            given_name=fields["given_name"],
            family_name=fields["family_name"],
            email=fields["email"],
            is_synthetic=True,
        )

    # ------------------------------------------------------------------
    # Patient message drafting stage
    # ------------------------------------------------------------------

    def should_draft_patient_message(self, ingestion_id) -> bool:
        """
        Gate keyed off the processing_event log. Message drafting starts only
        when normalization completed (cleanly or with warnings) AND the AI
        enrichment phase succeeded. Parenthesized exactly so it does NOT fire on
        a clean ingestion with no enrichment.
        """
        event_types = {
            event.event_type
            for event in self.pe_repo.list_by_ingestion_id(ingestion_id)
        }
        normalization_ok = (
            ProcessingEventType.NORMALIZATION_SUCCEEDED in event_types
            or ProcessingEventType.NORMALIZATION_SUCCEEDED_WITH_WARNINGS
            in event_types
        )
        enrichment_ok = (
            ProcessingEventType.AI_ENRICHMENT_SUCCEEDED in event_types
        )
        return normalization_ok and enrichment_ok

    def _get_patient_message_request(
        self,
        ingestion_id: uuid.UUID,
        patient_id: str,
        correlation_id: uuid.UUID,
    ) -> PatientMessageDraftRequest:
        """
        Build a DE-IDENTIFIED draft request from the validated, structured
        findings. patient_id is used here (server-side) only to fetch history;
        it is deliberately NOT placed on the request.
        """
        current_observations = self.observation_repo.get_by_ingestion_id(
            ingestion_id
        )
        diagnostic_reports = self.diagnostic_report_repo.get_by_ingestion_id(
            ingestion_id
        )
        analyte_codes = sorted({obs.code for obs in current_observations})
        historical_observations = (
            self.observation_repo.get_latest_by_patient_id(
                patient_id,
                exclude_ingestion_id=ingestion_id,
                codes=analyte_codes,
                per_code_limit=10,
            )
        )

        def _to_context(obs) -> ObservationContext:
            return ObservationContext(
                code=obs.code,
                display=obs.display,
                value_num=obs.value_num,
                value_text=obs.value_text,
                unit=obs.unit,
                ref_low_num=obs.ref_low_num,
                ref_high_num=obs.ref_high_num,
                interpretation=obs.flag_system_interpretation,
                effective_at=obs.effective_at,
            )

        return PatientMessageDraftRequest(
            ingestion_id=ingestion_id,
            correlation_id=correlation_id,
            panel_codes=[report.panel_code for report in diagnostic_reports],
            collected_at=max(
                obs.effective_at for obs in current_observations
            ),
            current_observations=[
                _to_context(obs) for obs in current_observations
            ],
            historical_observations=[
                _to_context(obs) for obs in historical_observations
            ],
        )

    def _emit_message_stage_event(
        self,
        ctx: EventContext,
        *,
        event_type: ProcessingEventType,
        severity: ProcessingEventSeverity,
        message: str,
        details: dict[str, Any] | None = None,
        deduped: bool = True,
    ):
        return emit(
            self.pe_repo,
            ctx,
            event_type=event_type,
            severity=severity,
            message=message,
            details=details,
            dedupe_key=self._dedupe_key(ctx, event_type),
            deduped=deduped,
        )

    def draft_patient_message(
        self, ingestion_id, guideline_context: list[Document] | None = None
    ) -> None:
        """
        Separate AI stage: draft a plain-language patient message. Gated on the
        processing_event log. Any failure here is logged but never fails the
        ingestion.
        """
        if not self.should_draft_patient_message(ingestion_id):
            return

        # Idempotency: don't draft a second active message for the ingestion.
        if (
            self.patient_message_repo.get_active_by_ingestion_id(ingestion_id)
            is not None
        ):
            return

        msg_ctx = EventContext(
            ingestion_id=ingestion_id,
            actor=ProcessingEventActor.MESSAGE_DRAFTER,
        )

        # Resolve patient_id on the trusted side.
        current_observations = self.observation_repo.get_by_ingestion_id(
            ingestion_id
        )
        patient_ids = {obs.patient_id for obs in current_observations}
        if len(patient_ids) != 1:
            self._emit_message_stage_event(
                msg_ctx,
                event_type=ProcessingEventType.MESSAGE_DRAFT_SKIPPED,
                severity=ProcessingEventSeverity.WARN,
                message="Patient message skipped: no single patient_id",
            )
            self.session.commit()
            return
        patient_id = next(iter(patient_ids))

        self._emit_message_stage_event(
            msg_ctx,
            event_type=ProcessingEventType.MESSAGE_DRAFT_STARTED,
            severity=ProcessingEventSeverity.INFO,
            message="Patient message draft started",
        )
        self.session.commit()

        # Mint the job-scoped token on the trusted side.
        correlation_id = uuid.uuid4()
        self.ai_generation_job_repo.create(
            correlation_id=correlation_id,
            job_type=AiGenerationJobType.PATIENT_MESSAGE,
            patient_id=patient_id,
            ingestion_id=ingestion_id,
        )

        request = self._get_patient_message_request(
            ingestion_id, patient_id, correlation_id
        )
        try:
            result = orchestrate_patient_message_draft(
                request, guideline_context=guideline_context
            )
        except Exception as e:
            self._emit_stage_failed(
                msg_ctx,
                event_type=ProcessingEventType.MESSAGE_DRAFT_FAILED,
                error_code="patient_message_exception",
                error=e,
                message="Patient message draft failed",
            )
            return

        # Recover patient_id by LOOKING UP the token (consume, one-time use) —
        # never by decoding it.
        job = self.ai_generation_job_repo.consume(correlation_id)
        resolved_patient_id = job.patient_id if job is not None else patient_id

        self._persist_patient_message(
            msg_ctx,
            ingestion_id=ingestion_id,
            patient_id=resolved_patient_id,
            request=request,
            result=result,
        )
        self.session.commit()

    def _persist_patient_message(
        self,
        ctx: EventContext,
        *,
        ingestion_id,
        patient_id: str,
        request: PatientMessageDraftRequest,
        result,
    ) -> None:
        base_details = {
            "provider": result.provider,
            "model_id": result.model_id,
            "prompt_version": result.prompt_version,
            "temperature": result.temperature,
            "content_schema_version": result.content_schema_version,
            "input_hash": result.input_hash,
            "correlation_id": str(request.correlation_id),
        }

        if result.llm_response_content is not None:
            draft_content = result.llm_response_content.model_dump(mode="json")
            validation_status = PatientMessageValidationStatus.ACCEPTED
            validation_error = None
            # Machine gate passed → advance out of DRAFT for human review.
            review_status = PatientMessageReviewStatus.PENDING_REVIEW
            event = self._emit_message_stage_event(
                ctx,
                event_type=ProcessingEventType.MESSAGE_DRAFT_SUCCEEDED,
                severity=ProcessingEventSeverity.INFO,
                message="Patient message draft succeeded",
                details=base_details,
                deduped=False,
            )
        elif result.llm_response_text is not None:
            audit = build_rejected_patient_message_audit(
                raw_llm_response=result.llm_response_text,
                rejection_reason=result.rejection_reason
                or "Patient message validation failed",
            )
            draft_content = audit.model_dump(mode="json")
            validation_status = PatientMessageValidationStatus.REJECTED
            validation_error = audit.rejection_reason
            # Machine gate failed → stays in DRAFT, never offered for review.
            review_status = PatientMessageReviewStatus.DRAFT
            event = self._emit_message_stage_event(
                ctx,
                event_type=ProcessingEventType.MESSAGE_DRAFT_FAILED,
                severity=ProcessingEventSeverity.ERROR,
                message="Patient message draft failed schema validation",
                details={
                    **base_details,
                    "rejection_reason": result.rejection_reason,
                },
                deduped=False,
            )
        else:
            # No LLM response (e.g. missing config) — nothing to persist.
            self._emit_message_stage_event(
                ctx,
                event_type=ProcessingEventType.MESSAGE_DRAFT_SKIPPED,
                severity=ProcessingEventSeverity.WARN,
                message="Patient message produced no draft",
                details={
                    **base_details,
                    "failure_reason": result.failure_reason,
                },
            )
            return

        generation_event_id = getattr(event, "event_id", None)

        patient_message = PatientMessage(
            patient_id=patient_id,
            ingestion_id=ingestion_id,
            draft_content_json=draft_content,
            content_schema_version=result.content_schema_version,
            correlation_id=request.correlation_id,
            generation_event_id=generation_event_id,
            provider=result.provider,
            model_id=result.model_id,
            prompt_version=result.prompt_version,
            temperature=result.temperature,
            input_hash=result.input_hash,
            retrieved_refs_json=result.retrieved_refs,
            validation_status=validation_status,
            validated_at=result.created_at,
            validation_error=validation_error,
            review_status=review_status,
        )
        self.patient_message_repo.create(patient_message)

    def insert_panel_test_data(
        self,
        ingestion_id,
        panel_packages: list[PanelPackage] | None,
        validation_errors: list[Any],
    ) -> bool:
        """
        If validation errors exist, persist no Panel and Tests.
        """
        if validation_errors:
            error_code = "validation_error"
            error_detail = {
                "validation_errors": self._errors_to_json(validation_errors)
            }

            self.ingestion_repo.mark_failed_validation(
                ingestion_id=ingestion_id,
                error_code=error_code,
                error_detail=error_detail,
            )
            return False

        if panel_packages is not None:
            # Ensure a patient row exists for every patient_id before inserting
            # panels/observations that now FK back to patient(patient_id).
            for patient_id in {
                pp.panel_payload["patient_id"]
                for pp in panel_packages
                if pp.panel_payload.get("patient_id")
            }:
                self._upsert_patient(patient_id)

            panels: list[Panel] = [
                Panel(ingestion_id=ingestion_id, **pp.panel_payload)
                for pp in panel_packages
            ]
            self.panel_repo.create_many(panels)

            tests: list[Test] = []
            for panel, panel_package in zip(
                panels, panel_packages, strict=True
            ):
                for test_payload in panel_package.test_payloads:
                    tests.append(Test(panel_id=panel.panel_id, **test_payload))
            self.test_repo.create_many(tests)

        return True

    def process_ingestion(self, ingestion_id, skip_ai_stages: bool = False):
        if not self.ingestion_repo.claim_for_processing(ingestion_id):
            return  # already claimed or not in a processable state

        # One execution_id for the full orchestration attempt so PARSE/
        # VALIDATION events correlate. Normalizer uses its own execution_id.
        root_ctx = EventContext(
            ingestion_id=ingestion_id,
            actor=ProcessingEventActor.INGESTION_API,
        )
        parser_ctx = root_ctx.child(actor=ProcessingEventActor.PARSER)
        validator_ctx = root_ctx.child(actor=ProcessingEventActor.VALIDATOR)
        ai_ctx = root_ctx.child(actor=ProcessingEventActor.AI_WORKER)

        try:
            emit_started(
                self.pe_repo,
                parser_ctx,
                event_type=ProcessingEventType.PARSE_STARTED,
                message="CSV parse started",
                details=None,
                dedupe_key=self._dedupe_key(
                    parser_ctx, ProcessingEventType.PARSE_STARTED
                ),
                deduped=True,
            )
            self.session.commit()

            try:
                csv_content = self.get_csv_file(ingestion_id)
            except NoResultFound as e:
                # Ingestion exists (we just claimed it) but its raw bytes are missing.
                self.ingestion_repo.mark_failed(
                    ingestion_id=ingestion_id,
                    error_code="raw_data_not_found",
                    error_detail={
                        "message": "No CSV content found for ingestion_id.",
                    },
                )

                self._emit_stage_failed(
                    parser_ctx,
                    event_type=ProcessingEventType.PARSE_FAILED,
                    error_code="raw_data_not_found",
                    error=e,
                    message="No raw CSV content found for ingestion",
                )
                return
            except MultipleResultsFound as e:
                # Data integrity issue: multiple RawData rows for one ingestion.
                self.ingestion_repo.mark_failed(
                    ingestion_id=ingestion_id,
                    error_code="raw_data_multiple",
                    error_detail={
                        "message": "Multiple raw CSV files found for ingestion_id.",
                    },
                )

                self._emit_stage_failed(
                    parser_ctx,
                    event_type=ProcessingEventType.PARSE_FAILED,
                    error_code="raw_data_multiple",
                    error=e,
                    message="Multiple raw CSV rows found for ingestion",
                )
                return

            if not csv_content:
                # Empty file is a data issue rather than an internal error.
                self.ingestion_repo.mark_failed_validation(
                    ingestion_id=ingestion_id,
                    error_code="empty_csv",
                    error_detail={
                        "message": "Raw CSV content is empty.",
                    },
                )

                self._emit_stage_failed(
                    parser_ctx,
                    event_type=ProcessingEventType.PARSE_FAILED,
                    error_code="empty_csv",
                    error=ValueError("Raw CSV content is empty"),
                    message="CSV content was empty",
                )
                return

            try:
                csv_rows = self.parse_csv_file(csv_content)
            except UnicodeDecodeError as e:
                self.ingestion_repo.mark_failed_validation(
                    ingestion_id=ingestion_id,
                    error_code="csv_decode_error",
                    error_detail={
                        "message": str(e),
                        "type": type(e).__name__,
                    },
                )

                self._emit_stage_failed(
                    parser_ctx,
                    event_type=ProcessingEventType.PARSE_FAILED,
                    error_code="csv_decode_error",
                    error=e,
                    message="CSV decode failed",
                )
                return
            except Exception as e:
                self.ingestion_repo.mark_failed_validation(
                    ingestion_id=ingestion_id,
                    error_code="csv_parse_error",
                    error_detail={
                        "message": str(e),
                        "type": type(e).__name__,
                    },
                )

                self._emit_stage_failed(
                    parser_ctx,
                    event_type=ProcessingEventType.PARSE_FAILED,
                    error_code="csv_parse_error",
                    error=e,
                    message="CSV parse failed",
                )
                return

            if not csv_rows:
                self.ingestion_repo.mark_failed_validation(
                    ingestion_id=ingestion_id,
                    error_code="csv_no_rows",
                    error_detail={
                        "message": "CSV parsed successfully but contained no data rows.",
                    },
                )

                self._emit_stage_failed(
                    parser_ctx,
                    event_type=ProcessingEventType.PARSE_FAILED,
                    error_code="csv_no_rows",
                    error=ValueError(
                        "CSV parsed successfully but contained no data rows"
                    ),
                    message="CSV contained no data rows",
                )
                return

            emit(
                self.pe_repo,
                parser_ctx,
                event_type=ProcessingEventType.PARSE_SUCCEEDED,
                severity=ProcessingEventSeverity.INFO,
                message="CSV parse succeeded",
                details={"row_count": len(csv_rows)},
                dedupe_key=self._dedupe_key(
                    parser_ctx, ProcessingEventType.PARSE_SUCCEEDED
                ),
                deduped=True,
            )
            self.session.commit()

            emit_started(
                self.pe_repo,
                validator_ctx,
                event_type=ProcessingEventType.VALIDATION_STARTED,
                message="Validation started",
                details=None,
                dedupe_key=self._dedupe_key(
                    validator_ctx, ProcessingEventType.VALIDATION_STARTED
                ),
                deduped=True,
            )
            self.session.commit()

            try:
                panel_packages, validation_errors = (
                    self.generate_payload_for_db(csv_rows)
                )
            except Exception as e:
                self.ingestion_repo.mark_failed(
                    ingestion_id=ingestion_id,
                    error_code="validation_exception",
                    error_detail={
                        "message": str(e),
                        "type": type(e).__name__,
                    },
                )

                self._emit_stage_failed(
                    validator_ctx,
                    event_type=ProcessingEventType.VALIDATION_FAILED,
                    error_code="validation_exception",
                    error=e,
                    message="Validation crashed",
                )
                return

            try:
                ok = self.insert_panel_test_data(
                    ingestion_id, panel_packages, validation_errors
                )
            except SQLAlchemyError as e:
                # If a flush failed, SQLAlchemy requires rollback before any
                # further DB interaction on this session.
                self.session.rollback()
                self.ingestion_repo.mark_failed(
                    ingestion_id=ingestion_id,
                    error_code="persistence_error",
                    error_detail={
                        "message": str(e),
                        "type": type(e).__name__,
                    },
                )

                self._emit_stage_failed(
                    validator_ctx,
                    event_type=ProcessingEventType.VALIDATION_FAILED,
                    error_code="persistence_error",
                    error=e,
                    message="Persistence failed while saving validated data",
                )
                return
            if not ok:
                # insert_panel_test_data() already persists validation errors,
                # updates ingestion status to "VALIDATION_FAILED", and writes
                # no Panel/Test.

                error_sample = (
                    self._errors_to_json(validation_errors)[:20]
                    if validation_errors
                    else []
                )
                self._emit_stage_failed(
                    validator_ctx,
                    event_type=ProcessingEventType.VALIDATION_FAILED,
                    error_code="validation_error",
                    error=ValueError("Validation failed"),
                    message="Validation failed",
                    details={
                        "validation_error_count": (
                            len(validation_errors)
                            if validation_errors is not None
                            else 0
                        ),
                        "validation_errors_sample": error_sample,
                        "validation_errors_sample_truncated": (
                            len(validation_errors) > 20
                            if validation_errors
                            else False
                        ),
                    },
                )
                return

            # Otherwise, emit processing event "VALIDATION_SUCCEEDED"

            panel_count = len(panel_packages) if panel_packages else 0
            test_count = (
                sum(len(p.test_payloads) for p in panel_packages)
                if panel_packages
                else 0
            )
            emit(
                self.pe_repo,
                validator_ctx,
                event_type=ProcessingEventType.VALIDATION_SUCCEEDED,
                severity=ProcessingEventSeverity.INFO,
                message="Validation succeeded",
                details={
                    "panel_count": panel_count,
                    "test_count": test_count,
                },
                dedupe_key=self._dedupe_key(
                    validator_ctx, ProcessingEventType.VALIDATION_SUCCEEDED
                ),
                deduped=True,
            )
            self.session.commit()

            # run normalizer. It emits the processing events.
            # do we need to check for status of the ingestion before picking up
            # ingestion_id rows to normalize?
            normalization_job = NormalizationJob(self.session)
            try:
                ok, norm_errors, json_failures = (
                    normalization_job.run_for_ingestion_id(ingestion_id)
                )
            except SQLAlchemyError as e:
                self.session.rollback()
                self.ingestion_repo.mark_failed(
                    ingestion_id=ingestion_id,
                    error_code="normalization_db_error",
                    error_detail={
                        "message": str(e),
                        "type": type(e).__name__,
                    },
                )
                return
            except Exception as e:
                self.session.rollback()
                self.ingestion_repo.mark_failed(
                    ingestion_id=ingestion_id,
                    error_code="normalization_exception",
                    error_detail={
                        "message": str(e),
                        "type": type(e).__name__,
                    },
                )
                return

            if not ok:
                # Normalize error payload to JSON-safe structures.
                self.ingestion_repo.mark_failed_validation(
                    ingestion_id,
                    error_code="normalization_failed",
                    error_detail={
                        "normalization_errors": self._errors_to_json(
                            norm_errors
                        ),
                    },
                )
                return

            # Normalization succeeded (warnings are still a success path).
            if skip_ai_stages:
                self.ingestion_repo.mark_completed(ingestion_id)
                self.session.commit()
                return

            emit_started(
                self.pe_repo,
                ai_ctx,
                event_type=ProcessingEventType.AI_ENRICHMENT_STARTED,
                message="AI enrichment started",
                details=None,
                dedupe_key=self._dedupe_key(
                    ai_ctx, ProcessingEventType.AI_ENRICHMENT_STARTED
                ),
                deduped=True,
            )
            self.session.commit()

            ai_request = self._get_ai_enrichment_request(ingestion_id)
            try:
                ai_result = orchestrate_ai_enrichment(ai_request)
            except Exception as e:
                self._emit_stage_failed(
                    ai_ctx,
                    event_type=ProcessingEventType.AI_ENRICHMENT_FAILED,
                    error_code="ai_enrichment_exception",
                    error=e,
                    message="AI enrichment failed",
                )
                self.ingestion_repo.mark_completed(ingestion_id)
                self.session.commit()
                return

            self._persist_ai_annotation(
                ai_request=ai_request,
                ai_result=ai_result,
            )
            # Complete the round-trip: consume the trusted-side token
            # (one-time use). Enrichment persistence keys on ingestion_id, so we
            # don't need the recovered patient_id here — consuming keeps the
            # boundary symmetric with the patient-message flow.
            self.ai_generation_job_repo.consume(ai_request.correlation_id)
            self._emit_ai_stage_result(ai_ctx, ai_result)
            self.session.commit()

            # Patient-message drafting runs as its own stage, gated on the
            # processing_event log (normalization succeeded AND enrichment
            # succeeded). Failures here never fail the ingestion.
            self.draft_patient_message(
                ingestion_id, guideline_context=ai_result.guideline_context
            )

            self.ingestion_repo.mark_completed(ingestion_id)
            self.session.commit()
            return

        except Exception as e:
            # Let the task layer handle rollback + failure persistence in a
            # separate transaction.
            raise
