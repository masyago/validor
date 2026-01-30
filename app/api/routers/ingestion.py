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
)
from datetime import datetime
import uuid
from app.core.ingestion_status_enums import IngestionStatus
import hashlib
import io
from typing import Union, Any

from sqlalchemy.orm import Session
from sqlalchemy import select
from app.persistence.models.core import RawData, Ingestion
from app.api.routers.dependencies import get_session

# from app.services.tasks.ingestion_tasks import process_ingestion_task
from app.persistence.repositories.ingestion_repo import (
    IngestionRepository,
    RawDataRepository,
)

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


# def get_existing_ingestion(db: Session, instrument_id: str, run_id: str):
#     """
#     Search database for an existing record with specified instrument_id and run_id.
#     If the record exists, retrieve and return its ingestion_id and server_sha256.
#     Otherwise, return None for both.
#     """
#     query = select(Ingestion).where(
#         Ingestion.instrument_id == instrument_id, Ingestion.run_id == run_id
#     )
#     existing_record = db.scalars(query).first()
#     if existing_record:
#         return existing_record.ingestion_id, existing_record.server_sha256
#     return None, None


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
        status.HTTP_422_UNPROCESSABLE_CONTENT: {
            "description": "Validation error, see response for details.",
            "content": {
                "application/json": {
                    # "schema": _merge_schemas(
                    #     IngestionMissingFieldResponse,
                    #     IngestionContentHashMismatchResponse,)
                    "schema": {
                        "oneOf": [
                            IngestionMissingFieldResponse.model_json_schema(),
                            IngestionContentHashMismatchResponse.model_json_schema(),
                        ]
                    },
                },
            },
        },
    },
    dependencies=[
        Depends(check_content_length),
        # Depends(_include_ingestion_models),
    ],
)
async def create_ingestion(
    background_tasks: BackgroundTasks,
    response: Response,
    file: Annotated[UploadFile, File()],
    metadata: IngestionMetadata = Depends(IngestionMetadata.as_form),
    db: Session = Depends(get_session),
    # _include1: Any = Depends(lambda: IngestionMissingFieldResponse),
    # _include2: Any = Depends(lambda: IngestionContentHashMismatchResponse),
    # Uncomment when background tasks and database implemented:
    # background_tasks: BackgroundTasks,
):
    """
    Logic:
    - [No code - handled by FastAPI automatically].
       Check media type. If not multipart/form-data:
        - return code 415
    - [Can be handled by FastAPI automatically btu it won't use custom model.
       Need to write code to overwrite default behavour.]. Check that all required fields present. If not,   return 422 - Missing field
        - [DONE] if file size exceeds limit, return 413
        - [DONE] generate content hash server_sha256
            - if content_sha256 provided
                - if content_sha256 != server_sha256:
                    - return 422 Content hahs mismatch
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
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
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
                    message="An ingestion already exists for the run (instrument_id, run_id) but server-produced hash differs.",
                ).model_dump(),
            )

    # Create new records
    new_ingestion_id = uuid.uuid4()
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

    # Enqueue CSV file processing
    # background_tasks.add_task(process_ingestion_task, new_ingestion_id)

    response.headers["Location"] = f"/v1/ingestions/{new_ingestion_id}"
    return IngestionAcceptedResponse(
        ingestion_id=str(new_ingestion_id),
        status=IngestionStatus.RECEIVED,
        api_received_at=new_ingestion_api_received_at,
        message="Ingestion request received and queued for processing.",
    )


@router.post("/v1/ingestions/{ingestion_id}/process")
def process_ingestion(
    ingestion_id: UUID,
    session: Session = Depends(get_session),
):
    svc = IngestionService(
        raw_repo=RawDataRepository(session),
        panel_repo=PanelRepository(session),
        test_repo=TestRepository(session),
    )
    result = svc.process_ingestion(ingestion_id)
    return result  # or status DTO
