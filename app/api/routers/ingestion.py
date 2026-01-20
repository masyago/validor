from fastapi import (
    APIRouter,
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
from app.core.enums import IngestionStatus
import hashlib
import io
from typing import Union

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


def get_existing_ingestion(instrument_id: str, run_id: str):
    """
    Placeholder function for database lookup.
    Returns (ingestion_id, content_sha256) if found, else (None, None).

    TODO: Replace with actual database query when DB layer is implemented.
    """
    # This will be replaced with:
    # existing = db.query(IngestionModel).filter_by(
    #     instrument_id=instrument_id, run_id=run_id
    # ).first()
    # if existing:
    #     return existing.ingestion_id, existing.content_sha256
    return None, None


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
                    "schema": {
                        "oneOf": [
                            {
                                "$ref": f"#/components/schemas/{IngestionMissingFieldResponse.__name__}"
                            },
                            {
                                "$ref": f"#/components/schemas/{IngestionContentHashMismatchResponse.__name__}"
                            },
                        ]
                    },
                },
            },
        },
    },
    dependencies=[Depends(check_content_length)],
)
async def create_ingestion(
    response: Response,
    file: Annotated[UploadFile, File()],
    metadata: IngestionMetadata = Depends(IngestionMetadata.as_form),
    # Uncomment when background tasks and database implemented:
    # background_tasks: BackgroundTasks,
    # db: Session = Depends(get_db),
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
    existing_ingestion_id, db_sha256 = get_existing_ingestion(
        metadata.instrument_id, metadata.run_id
    )

    if db_sha256 and existing_ingestion_id:  # Simulate finding record
        if db_sha256 == server_sha256_new:
            # Set the Location header for the 200 OK response
            response.headers["Location"] = (
                f"/v1/ingestions/{existing_ingestion_id}"
            )
            response.status_code = status.HTTP_200_OK
            return IngestionDuplicateOkResponse(
                existing_ingestion_id=existing_ingestion_id,
                message="The run was already submitted.",
            )

        else:
            # The exception body contains values placeholders:
            # existing_ingestion_id, existing hash. Update when db is ready.
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=IngestionDuplicateConflictResponse(
                    code="RUN_ID_CONTENT_MISMATCH",
                    retryable=False,
                    existing_ingestion_id=existing_ingestion_id,
                    conflict_key={
                        "instrument_id": metadata.instrument_id,
                        "run_id": metadata.run_id,
                    },
                    hashes={
                        "existing": db_sha256,
                        "submitted": server_sha256_new,
                    },
                    message="An ingestion already exists for the run (instrument_id, run_id) but server-produced hash differs.",
                ).model_dump(),
            )

    # No existing record. Create a new record
    new_ingestion_id = str(uuid.uuid4())
    response.headers["Location"] = f"/v1/ingestions/{new_ingestion_id}"
    return IngestionAcceptedResponse(
        ingestion_id=new_ingestion_id,
        status=IngestionStatus.PROCESSING,
        api_received_at=datetime.now(),
        message="Ingestion request received and is being processed.",
    )
