from fastapi import (
    APIRouter,
    BackgroundTasks,
    File,
    UploadFile,
    Form,
    Depends,
    status,
    Header,
    HTTPException,
    Response,
    Query,
)
from typing import Annotated
from app.schemas.ingestion import (
    IngestionMetadata,
    IngestionDuplicateOkResponse,
    IngestionAcceptedResponse,
    IngestionDuplicateConflictResponse,
    IngestionPayloadTooLargeResponse,
    ValidationErrorDetail,
    IngestionMissingFieldResponse,
    IngestionContentHashMismatchResponse,
    ReadIngestionIdFoundOkResponse,
    PathResourceNotFoundResponse,
    ReadDiagnosticReportsOkResponse,
    ReadObservationsOkResponse,
)

from app.schemas.identifiers import PatientId

from datetime import datetime
from uuid import UUID, uuid4
from app.core.ingestion_status_enums import IngestionStatus
import hashlib
import io
from typing import Union, Any, Literal, cast

from sqlalchemy.orm import Session
from sqlalchemy import select
from app.persistence.models.core import RawData, Ingestion
from app.api.routers.dependencies import get_session

from app.services.tasks.ingestion_tasks import process_ingestion_task
from app.services.tasks.ingestion_tasks import reap_stuck_ingestions_task
from app.persistence.repositories.ingestion_repo import IngestionRepository
from app.persistence.repositories.raw_data_repo import RawDataRepository
from app.persistence.repositories.panel_repo import PanelRepository
from app.persistence.repositories.diagnostic_report_repo import (
    DiagnosticReportRepository,
)
from app.persistence.repositories.observation_repo import ObservationRepository

from app.services.ingestion_service import IngestionService
from app.persistence.repositories.processing_event_repo import (
    ProcessingEventRepository,
)

from app.provenance.emitter import EventContext, emit
from app.persistence.models.provenance import (
    ProcessingEventActor,
    ProcessingEventType,
    ProcessingEventSeverity,
)
from app.persistence.repositories.ingestion_repo import IngestionRepository

router = APIRouter()

MAX_FILE_SIZE_BYTES = 1000000  # 1 MB


async def check_content_length(content_length: int | None = Header(None)):
    """
    Checks Content-Length header against the maximum file size.
    """
    if content_length and content_length > MAX_FILE_SIZE_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_CONTENT_TOO_LARGE,
            detail=IngestionPayloadTooLargeResponse(
                code="PAYLOAD_TOO_LARGE",
                retryable=False,
                max_bytes=MAX_FILE_SIZE_BYTES,
                message="File exceeds size limit.",
            ).model_dump(),
        )


def calculate_sha256(file_content: bytes):
    """
    Calculates sha256 hash of the file content.
    """
    hasher = hashlib.sha256()
    hasher.update(file_content)
    return hasher.hexdigest()


@router.post(
    "/ingestions",
    response_model=None,  # Disable automatic response validation
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_202_ACCEPTED: {
            "model": IngestionAcceptedResponse,
            "description": "Ingestion accepted and is being processed.",
        },
        status.HTTP_200_OK: {
            "model": IngestionDuplicateOkResponse,
            "description": "Duplicate exists and has identical content.",
        },
        status.HTTP_409_CONFLICT: {
            "model": IngestionDuplicateConflictResponse,
            "description": "Duplicate exist, but content doesn't match.",
        },
        status.HTTP_413_CONTENT_TOO_LARGE: {
            "model": IngestionPayloadTooLargeResponse,
            "description": "File exceeds size limit.",
        },
        status.HTTP_400_BAD_REQUEST: {
            "model": IngestionContentHashMismatchResponse,
            "description": "Content hash mismatch.",
        },
        status.HTTP_422_UNPROCESSABLE_CONTENT: {
            "model": IngestionMissingFieldResponse,
            "description": "Validation error: incorrect or missing metadata.",
        },
    },
    dependencies=[
        Depends(check_content_length),
    ],
)
async def create_ingestion(
    background_tasks: BackgroundTasks,
    response: Response,
    file: Annotated[UploadFile, File()],
    metadata: IngestionMetadata = Depends(IngestionMetadata.as_form),
    db: Session = Depends(get_session),
    # Uncomment when background tasks and database implemented:
    # background_tasks: BackgroundTasks,
):
    """
    Logic:
    - [No code - handled by FastAPI automatically].
       Check media type. If not multipart/form-data:
        - return code 415
    - [Can be handled by FastAPI automatically btu it won't use custom model.
       Need to write code to overwrite default behavior.]. Check that all required fields present. If not,   return 422 - Missing field
        - [DONE] if file size exceeds limit, return 413
        - [DONE] generate content hash server_sha256
            - if content_sha256 provided
                - if content_sha256 != server_sha256:
                                        - return 400 Content hash mismatch
        - check if combination of instrument_id and run_id present in database
          already. if yes, retrieve server_sha256.
            - compare server_sha256_new to server_sha256.
               - if differ, return 409
               - if teh same, return 200

    - return 202


    """
    # Calculate file hash
    # `read()` here returns the content as bytes
    file_content = await file.read()
    server_sha256_new = calculate_sha256(file_content)

    # Check if client-provided hash matches server-calculated hash
    if metadata.content_sha256:
        if metadata.content_sha256 != server_sha256_new:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=IngestionContentHashMismatchResponse(
                    code="CONTENT_HASH_MISMATCH",
                    retryable=False,
                    message="Content integrity check failed.",
                ).model_dump(),
            )

    # Check for existing ingestion
    existing_ingestion = IngestionRepository(db).get_by_instrument_id_run_id(
        metadata.instrument_id, metadata.run_id
    )

    if existing_ingestion:  # Simulate finding record
        if existing_ingestion.server_sha256 == server_sha256_new:
            # Set the Location header for the 200 OK response
            response.headers["Location"] = (
                f"/v1/ingestions/{existing_ingestion.ingestion_id}"
            )
            response.status_code = status.HTTP_200_OK
            return IngestionDuplicateOkResponse(
                existing_ingestion_id=str(existing_ingestion.ingestion_id),
                message="The run was already submitted.",
            )

        else:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=IngestionDuplicateConflictResponse(
                    code="RUN_ID_CONTENT_MISMATCH",
                    retryable=False,
                    existing_ingestion_id=str(existing_ingestion.ingestion_id),
                    conflict_key={
                        "instrument_id": metadata.instrument_id,
                        "run_id": metadata.run_id,
                    },
                    hashes={
                        "existing": existing_ingestion.server_sha256,
                        "submitted": server_sha256_new,
                    },
                    message="""An ingestion already exists for the run (instrument_id, run_id) but server-produced hash differs.""",
                ).model_dump(),
            )

    # Create new records
    new_ingestion_id = uuid4()
    new_ingestion_api_received_at = datetime.now()

    # Create and add Ingestion and RawData objects
    new_ingestion_record = Ingestion(
        ingestion_id=new_ingestion_id,
        instrument_id=metadata.instrument_id,
        run_id=metadata.run_id,
        uploader_id=metadata.uploader_id,
        spec_version=metadata.spec_version,
        uploader_received_at=metadata.uploader_received_at,
        api_received_at=new_ingestion_api_received_at,
        submitted_sha256=metadata.content_sha256,
        server_sha256=server_sha256_new,
        status=IngestionStatus.RECEIVED,
        source_filename=file.filename,
    )
    new_raw_data_record = RawData(
        ingestion_id=new_ingestion_id,
        content_bytes=file_content,
        content_mime=file.content_type,
        content_size_bytes=len(file_content),
    )

    IngestionRepository(db).create(new_ingestion_record)
    RawDataRepository(db).create(new_raw_data_record)
    db.commit()

    # Record acceptance in processing_event for traceability.
    pe_repo = ProcessingEventRepository(db)
    ctx = EventContext(
        ingestion_id=new_ingestion_id,
        actor=ProcessingEventActor.INGESTION_API,
    )
    emit(
        pe_repo,
        ctx,
        event_type=ProcessingEventType.INGESTION_ACCEPTED,
        severity=ProcessingEventSeverity.INFO,
        message="Ingestion accepted and queued for processing",
        details={"source_filename": file.filename},
        dedupe_key=f"ingestion-accepted:{new_ingestion_id}",
        deduped=True,
    )
    db.commit()

    # Enqueue CSV file processing
    background_tasks.add_task(process_ingestion_task, new_ingestion_id)

    response.headers["Location"] = f"/v1/ingestions/{new_ingestion_id}"
    return IngestionAcceptedResponse(
        ingestion_id=str(new_ingestion_id),
        status=IngestionStatus.RECEIVED,
        api_received_at=new_ingestion_api_received_at,
        message="Ingestion request received and queued for processing.",
    )


@router.post(
    "/ingestions/{ingestion_id}/process",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=None,
)
def process_ingestion(
    ingestion_id: UUID,
    background_tasks: BackgroundTasks,
    session: Session = Depends(get_session),
):
    # Command-style endpoint: kick off processing and return immediately.
    ingestion = IngestionRepository(session).get_by_ingestion_id(ingestion_id)
    if ingestion is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "code": "INGESTION_NOT_FOUND",
                "message": "No ingestion found for ingestion_id.",
            },
        )

    background_tasks.add_task(process_ingestion_task, ingestion_id)

    return Response(
        status_code=status.HTTP_202_ACCEPTED,
        headers={"Location": f"/v1/ingestions/{ingestion_id}"},
    )


@router.post(
    "/admin/reap-stuck-ingestions",
    status_code=status.HTTP_200_OK,
)
def reap_stuck_ingestions(
    max_age_seconds: int = 15 * 60,
    limit: int = 50,
    dry_run: bool = False,
):
    """Manual ops hook: find and retry ingestions stuck in PROCESSING."""
    return reap_stuck_ingestions_task(
        max_age_seconds=max_age_seconds,
        limit=limit,
        dry_run=dry_run,
    )


@router.get(
    "/ingestions/{ingestion_id}",
    response_model=ReadIngestionIdFoundOkResponse,
    responses={
        status.HTTP_404_NOT_FOUND: {
            "model": PathResourceNotFoundResponse,
            "description": "Item not found",
        },
    },
)
def read_ingestion_id(
    ingestion_id: UUID,
    db: Session = Depends(get_session),
):
    ingestion_repo = IngestionRepository(db)
    ingestion_row = ingestion_repo.get_by_ingestion_id(ingestion_id)
    if ingestion_row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=PathResourceNotFoundResponse(
                ingestion_id=ingestion_id, detail="Item not found"
            ).model_dump(mode="json", exclude_none=True),
        )

    return ReadIngestionIdFoundOkResponse(
        ingestion_id=ingestion_id,
        status=IngestionStatus(ingestion_row.status),
        api_received_at=ingestion_row.api_received_at,
        error_code=ingestion_row.error_code,
        error_detail=ingestion_row.error_detail,
    )


@router.get(
    "/ingestions/{ingestion_id}/diagnostic-reports",
    response_model=list[ReadDiagnosticReportsOkResponse],
    responses={
        status.HTTP_404_NOT_FOUND: {
            "model": PathResourceNotFoundResponse,
            "description": "Item not found",
        },
    },
)
async def read_diagnostic_reports_for_ingestion_id(
    ingestion_id: UUID,
    include_json: Annotated[
        int,
        Query(
            description=(
                "Whether to include `resource_json` (0 = don't include JSON (default), 1 = include JSON)."
            ),
            ge=0,
            le=1,
        ),
    ] = 0,
    db: Session = Depends(get_session),
) -> list[ReadDiagnosticReportsOkResponse]:
    dr_repo = DiagnosticReportRepository(db)
    dr_rows = dr_repo.get_by_ingestion_id(ingestion_id)

    if not dr_rows:
        ingestion_repo = IngestionRepository(db)
        ingestion_row = ingestion_repo.get_by_ingestion_id(ingestion_id)
        if ingestion_row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=PathResourceNotFoundResponse(
                    ingestion_id=ingestion_id,
                    detail="Item not found",
                ).model_dump(mode="json", exclude_none=True),
            )

    want_json = include_json == 1

    list_row_responses: list[ReadDiagnosticReportsOkResponse] = []
    for dr_row in dr_rows:
        row_response = ReadDiagnosticReportsOkResponse(
            diagnostic_report_id=dr_row.diagnostic_report_id,
            patient_id=dr_row.patient_id,
            panel_code=dr_row.panel_code,
            effective_at=dr_row.effective_at,
            normalized_at=dr_row.normalized_at,
            resource_json=dr_row.resource_json if want_json else None,
            status="FINAL",
        )
        list_row_responses.append(row_response)

    return list_row_responses


@router.get(
    "/ingestions/{ingestion_id}/observations",
    response_model=list[ReadObservationsOkResponse],
    responses={
        status.HTTP_404_NOT_FOUND: {
            "model": PathResourceNotFoundResponse,
            "description": "Item not found",
        },
    },
)
async def read_observations_for_ingestion_id(
    ingestion_id: UUID,
    include_json: Annotated[
        int,
        Query(
            description=(
                "Whether to include `resource_json` (0 = don't include JSON (default), 1 = include JSON)."
            ),
            ge=0,
            le=1,
        ),
    ] = 0,
    limit: Annotated[
        int,
        Query(
            description="Maximum number of observations to return.",
            ge=1,
        ),
    ] = 10,
    offset: Annotated[
        int,
        Query(
            description="Number of observations to skip from the beginning of the result set.",
            ge=0,
        ),
    ] = 0,
    db: Session = Depends(get_session),
) -> list[ReadObservationsOkResponse]:
    obs_repo = ObservationRepository(db)
    obs_rows = obs_repo.get_by_ingestion_id(ingestion_id)

    if not obs_rows:
        ingestion_repo = IngestionRepository(db)
        ingestion_row = ingestion_repo.get_by_ingestion_id(ingestion_id)
        if ingestion_row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=PathResourceNotFoundResponse(
                    ingestion_id=ingestion_id,
                    detail="Item not found",
                ).model_dump(mode="json", exclude_none=True),
            )

    want_json = include_json == 1
    page_rows = obs_rows[offset : offset + limit]

    list_row_responses: list[ReadObservationsOkResponse] = []
    for ob_row in page_rows:
        row_response = ReadObservationsOkResponse(
            observation_id=ob_row.observation_id,
            diagnostic_report_id=ob_row.diagnostic_report_id,
            patient_id=ob_row.patient_id,
            code=ob_row.code,
            display=ob_row.display,
            effective_at=ob_row.effective_at,
            normalized_at=ob_row.normalized_at,
            value_num=ob_row.value_num,
            value_text=ob_row.value_text,
            comparator=ob_row.comparator,
            unit=ob_row.unit,
            ref_low_num=ob_row.ref_low_num,
            ref_high_num=ob_row.ref_high_num,
            flag_analyzer_interpretation=ob_row.flag_analyzer_interpretation,
            flag_system_interpretation=ob_row.flag_system_interpretation,
            discrepancy=ob_row.discrepancy,
            resource_json=ob_row.resource_json if want_json else None,
            status="FINAL",
        )
        list_row_responses.append(row_response)

    return list_row_responses


# `GET /v1/patients/{patient_id}/diagnostic-reports?include_json=1&limit=...&offset=...`
@router.get(
    "/patients/{patient_id}/diagnostic-reports",
    response_model=list[ReadDiagnosticReportsOkResponse],
    responses={
        status.HTTP_404_NOT_FOUND: {
            "model": PathResourceNotFoundResponse,
            "description": "Item not found",
        },
    },
)
async def read_diagnostic_reports_for_patient_id(
    patient_id: PatientId,
    include_json: Annotated[
        int,
        Query(
            description=(
                "Whether to include `resource_json` (0 = don't include JSON (default), 1 = include JSON)."
            ),
            ge=0,
            le=1,
        ),
    ] = 0,
    limit: Annotated[
        int,
        Query(
            description="Maximum number of observations to return.",
            ge=1,
        ),
    ] = 10,
    offset: Annotated[
        int,
        Query(
            description="Number of observations to skip from the beginning of the result set.",
            ge=0,
        ),
    ] = 0,
    db: Session = Depends(get_session),
) -> list[ReadDiagnosticReportsOkResponse]:
    dr_repo = DiagnosticReportRepository(db)
    dr_rows = dr_repo.get_by_patient_id(patient_id)

    """
    Check if patient_id exists in panel_repo as Panel is the first table
    where patient_id's are extracted from CSV
    """
    if not dr_rows:
        panel_repo = PanelRepository(db)
        panel_rows = panel_repo.get_by_patient_id(patient_id)
        if not panel_rows:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=PathResourceNotFoundResponse(
                    patient_id=patient_id,
                    detail="Item not found",
                ).model_dump(mode="json", exclude_none=True),
            )

    want_json = include_json == 1
    page_rows = dr_rows[offset : offset + limit]

    list_row_responses: list[ReadDiagnosticReportsOkResponse] = []
    for dr_row in page_rows:
        row_response = ReadDiagnosticReportsOkResponse(
            diagnostic_report_id=dr_row.diagnostic_report_id,
            patient_id=dr_row.patient_id,
            panel_code=dr_row.panel_code,
            effective_at=dr_row.effective_at,
            normalized_at=dr_row.normalized_at,
            resource_json=dr_row.resource_json if want_json else None,
            status="FINAL",
        )
        list_row_responses.append(row_response)

    return list_row_responses


# `GET /v1/patients/{patient_id}/observations?include_json=1&limit=...&offset=...`
@router.get(
    "/patients/{patient_id}/observations",
    response_model=list[ReadObservationsOkResponse],
    responses={
        status.HTTP_404_NOT_FOUND: {
            "model": PathResourceNotFoundResponse,
            "description": "Item not found",
        },
    },
)
async def read_observations_for_patient_id(
    patient_id: PatientId,
    include_json: Annotated[
        int,
        Query(
            description=(
                "Whether to include `resource_json` (0 = don't include JSON (default), 1 = include JSON)."
            ),
            ge=0,
            le=1,
        ),
    ] = 0,
    limit: Annotated[
        int,
        Query(
            description="Maximum number of observations to return.",
            ge=1,
        ),
    ] = 10,
    offset: Annotated[
        int,
        Query(
            description="Number of observations to skip from the beginning of the result set.",
            ge=0,
        ),
    ] = 0,
    db: Session = Depends(get_session),
) -> list[ReadObservationsOkResponse]:
    obs_repo = ObservationRepository(db)
    obs_rows = obs_repo.get_by_patient_id(patient_id)

    """
    Check if patient_id exists in panel_repo as Panel is the first table
    where patient_id's are extracted from CSV
    """
    if not obs_rows:
        panel_repo = PanelRepository(db)
        panel_rows = panel_repo.get_by_patient_id(patient_id)
        if not panel_rows:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=PathResourceNotFoundResponse(
                    patient_id=patient_id,
                    detail="Item not found",
                ).model_dump(mode="json", exclude_none=True),
            )

    want_json = include_json == 1
    page_rows = obs_rows[offset : offset + limit]

    list_row_responses: list[ReadObservationsOkResponse] = []
    for ob_row in page_rows:
        row_response = ReadObservationsOkResponse(
            observation_id=ob_row.observation_id,
            diagnostic_report_id=ob_row.diagnostic_report_id,
            patient_id=ob_row.patient_id,
            code=ob_row.code,
            display=ob_row.display,
            effective_at=ob_row.effective_at,
            normalized_at=ob_row.normalized_at,
            value_num=ob_row.value_num,
            value_text=ob_row.value_text,
            comparator=ob_row.comparator,
            unit=ob_row.unit,
            ref_low_num=ob_row.ref_low_num,
            ref_high_num=ob_row.ref_high_num,
            flag_analyzer_interpretation=ob_row.flag_analyzer_interpretation,
            flag_system_interpretation=ob_row.flag_system_interpretation,
            discrepancy=ob_row.discrepancy,
            resource_json=ob_row.resource_json if want_json else None,
            status="FINAL",
        )
        list_row_responses.append(row_response)

    return list_row_responses
