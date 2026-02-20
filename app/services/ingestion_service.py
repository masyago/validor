from app.services.parser import CanonicalAnalyzerCsvParser
from app.services.validator import (
    PanelValidation,
    TestValidation,
    RowValidationError,
)
from app.persistence.repositories.raw_data_repo import RawDataRepository
from app.persistence.repositories.ingestion_repo import IngestionRepository
from app.persistence.repositories.panel_repo import PanelRepository
from app.persistence.repositories.test_repo import TestRepository
from app.persistence.models.parsing import Panel, Test
from app.services.normalizer import NormalizationJob

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
        self.panel_repo = PanelRepository(session)
        self.test_repo = TestRepository(session)
        self.pe_repo = ProcessingEventRepository(session)

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
            for panel_package in panel_packages:
                panel = Panel(
                    ingestion_id=ingestion_id, **panel_package.panel_payload
                )
                panel = self.panel_repo.create(panel)

                for test_payload in panel_package.test_payloads:
                    test = Test(panel_id=panel.panel_id, **test_payload)
                    self.test_repo.create(test)

        return True

    def process_ingestion(self, ingestion_id):
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
            self.ingestion_repo.mark_completed(ingestion_id)
            self.session.commit()
            return

        except Exception as e:
            # Let the task layer handle rollback + failure persistence in a
            # separate transaction.
            raise
