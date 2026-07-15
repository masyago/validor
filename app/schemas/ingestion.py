from typing import Annotated, Literal, Any
from fastapi import Form
from pydantic import BaseModel, Field, AfterValidator, model_validator
from datetime import datetime
from app.core.ingestion_status_enums import IngestionStatus
from uuid import UUID

from app.schemas.identifiers import PatientId


class IngestionMetadata(BaseModel):
    """
    Defines the metadata structure of ingestion request from uploader to API layer.
    The fields are expected as part of a 'multipart/form-data' request.
    """

    uploader_id: str
    spec_version: str
    instrument_id: str
    run_id: str
    content_sha256: str | None = None
    uploader_received_at: datetime

    @classmethod
    def as_form(
        cls,
        uploader_id: str = Form(...),
        spec_version: str = Form(...),
        instrument_id: str = Form(...),
        run_id: str = Form(...),
        content_sha256: str | None = Form(None),
        uploader_received_at: datetime = Form(...),
    ):
        return cls(
            uploader_id=uploader_id,
            spec_version=spec_version,
            instrument_id=instrument_id,
            run_id=run_id,
            content_sha256=content_sha256,
            uploader_received_at=uploader_received_at,
        )


class IngestionDuplicateOkResponse(BaseModel):  # 200 OK - Duplicated matches
    existing_ingestion_id: str
    message: str


class IngestionAcceptedResponse(BaseModel):  # 202 Accepted
    ingestion_id: str
    status: IngestionStatus
    api_received_at: datetime
    message: str


class IngestionDuplicateConflictResponse(BaseModel):  # 409 Conflict
    code: str
    retryable: bool
    existing_ingestion_id: str
    conflict_key: dict[str, str]
    hashes: dict[str, str]
    message: str


class IngestionPayloadTooLargeResponse(BaseModel):  # 413 Payload Too Large
    code: str
    retryable: bool
    max_bytes: int
    message: str


class ValidationErrorDetail(BaseModel):
    field: str
    message: str


class IngestionMissingFieldResponse(BaseModel):  # 422 Missing Field
    code: str
    retryable: bool
    errors: list[ValidationErrorDetail]
    message: str


class IngestionContentHashMismatchResponse(
    BaseModel
):  # 422 Hash Mismatch Error
    code: str
    retryable: bool
    message: str


class PathResourceNotFoundResponse(BaseModel):  # 404 Not Found Error
    ingestion_id: UUID | None = None
    patient_id: str | None = None
    detail: str

    @model_validator(mode="after")
    def _validate_has_identifier(self) -> "PathResourceNotFoundResponse":
        if self.ingestion_id is None and self.patient_id is None:
            raise ValueError(
                "PathResourceNotFoundResponse must include ingestion_id or patient_id"
            )
        return self


class ReadIngestionIdFoundOkResponse(BaseModel):
    ingestion_id: UUID
    status: IngestionStatus
    api_received_at: datetime
    error_code: str | None = None
    error_detail: dict[str, Any] | None = None


class ReadProcessingEventOkResponse(BaseModel):
    event_id: UUID
    ingestion_id: UUID
    occurred_at: datetime
    event_type: str
    actor: str
    severity: str
    message: str | None = None
    details: dict[str, Any] | None = None


class ReadAiAnnotationOkResponse(BaseModel):
    ai_annotation_id: UUID
    ingestion_id: UUID
    annotation_type: str | None = None
    content_json: dict[str, Any]
    provider: str | None = None
    model_id: str | None = None
    prompt_version: str | None = None
    temperature: str | None = None
    content_schema_version: str
    input_hash: str | None = None
    created_at: datetime | None = None
    validation_status: str | None = None
    validated_at: datetime | None = None
    rejection_reason: str | None = None


class ReadPatientMessageOkResponse(BaseModel):
    patient_message_id: UUID
    ingestion_id: UUID
    patient_id: PatientId
    # Rendered "To:" line — synthetic PHI applied only at read/render time,
    # never baked into draft_content_json.
    patient_given_name: str | None = None
    patient_family_name: str | None = None
    patient_email: str | None = None
    draft_content_json: dict[str, Any]
    final_content_json: dict[str, Any] | None = None
    content_schema_version: str | None = None
    # Provenance
    correlation_id: UUID | None = None
    generation_event_id: UUID | None = None
    provider: str | None = None
    model_id: str | None = None
    prompt_version: str | None = None
    temperature: str | None = None
    input_hash: str | None = None
    retrieved_refs_json: list[dict[str, Any]] | None = None
    created_at: datetime | None = None
    # Machine gate
    validation_status: str
    validated_at: datetime | None = None
    validation_error: str | None = None
    # Human gate
    review_status: str
    reviewed_by: str | None = None
    approved_by: str | None = None
    reviewed_at: datetime | None = None
    approved_at: datetime | None = None
    sent_at: datetime | None = None
    review_note: str | None = None
    superseded_by: UUID | None = None


class ApprovePatientMessageRequest(BaseModel):
    approved_by: str
    # Optional clinician-edited content; if omitted the draft is approved as-is.
    final_content_json: dict[str, Any] | None = None


class ReviewPatientMessageRequest(BaseModel):
    reviewed_by: str
    note: str | None = None


class ReadDiagnosticReportsOkResponse(BaseModel):
    diagnostic_report_id: UUID
    patient_id: PatientId
    panel_code: str
    effective_at: datetime
    normalized_at: datetime
    resource_json: dict[str, Any] | None = None
    status: Literal["FINAL"]


# Backwards-compatible alias (older name used earlier in the project)
ReadDiagnosticReportsByIngestionIdOkResponse = ReadDiagnosticReportsOkResponse


class ReadObservationsOkResponse(BaseModel):
    observation_id: UUID
    diagnostic_report_id: UUID
    patient_id: PatientId
    code: str
    display: str | None = None
    effective_at: datetime
    normalized_at: datetime
    value_num: float | None = None
    value_text: str | None = None
    comparator: str | None = None
    unit: str | None = None
    ref_low_num: float | None = None
    ref_high_num: float | None = None
    flag_analyzer_interpretation: str | None = None
    flag_system_interpretation: str | None = None
    discrepancy: str | None = None
    resource_json: dict[str, Any] | None = None
    status: Literal["FINAL"]


# Backwards-compatible alias (older name used earlier in the project)
ReadObservationsByIngestionIdOkResponse = ReadObservationsOkResponse
