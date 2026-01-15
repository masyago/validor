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
from .schemas.ingestion import (
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

router = APIRouter()

MAX_FILE_SIZE_BYTES = 1000000  # 1 MB


async def check_content_length(content_length: int | None = Header(None)):
    """
    Checks Content-Length header against the maximum file size.
    """
    if content_length and content_length > MAX_FILE_SIZE_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
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
    "/v1/ingestions",
    response_model=IngestionAcceptedResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
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

    # placeholder: from db, retrieve server_sha256 if from a row  that has both instrument_id and run_id:
    # placeholder: from db, retrieve server_sha256 if from a row that has both instrument_id and run_id:
    # existing_ingestion = crud.get_ingestion_by_run(...)
    existing_ingestion_id_placeholder = "a7b1c3d4-e5f6-7890-1234-567890abcdef"
    db_sha256_placeholder = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"  # Placeholder for existing hash

    if db.server_sha256:  # Simulate finding record
        if db.server_sha256 == server_sha256_new:
            # Set the Location header for the 200 OK response
            response.headers["Location"] = (
                f"/v1/ingestions/{existing_ingestion_id_placeholder}"
            )
            response.status_code = status.HTTP_200_OK
            return IngestionDuplicateOkResponse(
                existing_ingestion_id=existing_ingestion_id_placeholder,
                message="The run was already submitted.",
            )

        else:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=IngestionDuplicateConflictResponse(
                    code="RUN_ID_CONTENT_MISMATCH",
                    retryable=False,
                    existing_ingestion_id="a7b1c3d4-e5f6-7890-1234-567890abcdef",
                    conflict_key={
                        "instrument_id": "CHEM-ANALYZER-XYZ-789",
                        "run_id": "RUN-20260112-1430-A",
                    },
                    hashes={
                        "existing": "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
                        "submitted": "7509e5bda0c762d2bac7f90d758b5b2263fa01ccbc542ab5e3df163be08e6ca9",
                    },
                    message="An ingestion already exists for the run (instrument_id, run_id) but server-produced hash differs.",
                ).model_dump(),
            )

    new_ingestion_id = str(uuid.uuid4())  # replace with id from db when ready
    response.headers["Location"] = f"/v1/ingestions/{new_ingestion_id}"
    return IngestionAcceptedResponse(
        ingestion_id=new_ingestion_id,
        status=IngestionStatus.PROCESSING,
        api_received_at=datetime.now(),  # replace with new_ingestion.api_received_at when db is ready
        message="Ingestion request received and is being processed.",
    )


# That's it here for core logic? Probably will add some error handling
