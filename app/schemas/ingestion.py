from typing import Annotated, Literal
from fastapi import Form
from pydantic import BaseModel, Field, AfterValidator
from datetime import datetime
from app.core.enums import IngestionStatus


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
