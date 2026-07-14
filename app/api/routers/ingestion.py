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
    Request,
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
    ReadProcessingEventOkResponse,
    ReadAiAnnotationOkResponse,
    ReadPatientMessageOkResponse,
    ApprovePatientMessageRequest,
    ReviewPatientMessageRequest,
)

from app.schemas.identifiers import PatientId

from datetime import datetime, timedelta, timezone
import os
import time
from uuid import UUID, uuid4
from app.core.ingestion_status_enums import IngestionStatus
from app.core.session_config import SESSION_COOKIE_NAME, SESSION_TTL_MINUTES
import hashlib
import io
from typing import Union, Any, Literal, Optional, cast

from sqlalchemy.orm import Session
from sqlalchemy import select, func
from sqlalchemy.exc import IntegrityError
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
from app.persistence.repositories.ai_annotation_repo import (
    AiAnnotationRepository,
)
from app.persistence.repositories.patient_message_repo import (
    PatientMessageRepository,
)
from app.persistence.repositories.patient_repo import PatientRepository
from app.services.patient_message_service import (
    PatientMessageService,
    PatientMessageNotFoundError,
    InvalidTransitionError,
)
from app.persistence.models.patient_message import PatientMessage

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

MAX_FILE_SIZE_BYTES = 10000000  # 10 MB


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


def _int_env(name: str) -> int | None:
    raw = os.getenv(name)
    if raw is None:
        return None
    raw = raw.strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


# Per-IP upload rate limit state. In-memory, per-process: correct only for a
# single API worker (the demo runs one). With multiple workers/replicas each keeps
# its own window, so the effective limit multiplies — swap this for Redis if the
# API is ever scaled out (same caveat noted in pages/api/contact.js and the
# CLAUDE.md roadmap). Keys are client IPs; values are monotonic hit timestamps.
_UPLOAD_HITS: dict[str, list[float]] = {}


def _resolve_client_ip(request: Request) -> str:
    """Best-effort client IP for rate limiting.

    Mirrors getClientIp in pages/api/contact.js: prefer the first hop of
    X-Forwarded-For (the demo reaches FastAPI through the Next.js rewrite proxy),
    else the direct peer, else "unknown".

    Note: X-Forwarded-For is spoofable while the API port is publicly exposed, so a
    determined attacker can rotate keys. This is an accepted limitation of a basic
    demo cost guard; real hardening means not publishing the API port and/or a
    trusted-proxy allowlist.
    """

    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        first = forwarded.split(",")[0].strip()
        if first:
            return first
    if request.client is not None and request.client.host:
        return request.client.host
    return "unknown"


def _enforce_upload_rate_limit_or_429(request: Request) -> None:
    """Per-IP cost guard: cap how fast one client can trigger uploads.

    Each accepted upload spends Bedrock budget (AI enrichment + patient-message
    drafting), so a public, unauthenticated endpoint is a cost-runaway target.

    Enable by setting BOTH `CLA_DEMO_MAX_UPLOADS_PER_WINDOW` and
    `CLA_DEMO_RATE_WINDOW_SECONDS` to positive integers. When either is unset/<=0
    this is disabled — the same opt-in convention as `_enforce_inflight_limit_or_429`,
    so unit/e2e tests and the batch CLI demo are unaffected unless the env is set.
    """

    max_uploads = _int_env("CLA_DEMO_MAX_UPLOADS_PER_WINDOW")
    window_seconds = _int_env("CLA_DEMO_RATE_WINDOW_SECONDS")
    if (
        max_uploads is None
        or max_uploads <= 0
        or window_seconds is None
        or window_seconds <= 0
    ):
        return

    key = _resolve_client_ip(request)
    now = time.monotonic()
    cutoff = now - window_seconds

    hits = [ts for ts in _UPLOAD_HITS.get(key, []) if ts > cutoff]

    if len(hits) >= max_uploads:
        # Oldest in-window hit determines when the client may retry.
        retry_after_s = max(1, int(hits[0] + window_seconds - now) + 1)
        # Keep the pruned list so the window keeps sliding on repeated attempts.
        _UPLOAD_HITS[key] = hits
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            headers={"Retry-After": str(retry_after_s)},
            detail={
                "code": "DEMO_RATE_LIMITED",
                "retryable": True,
                "message": (
                    "Too many uploads from your address; please slow down and "
                    "retry shortly."
                ),
                "limit": int(max_uploads),
                "window_seconds": int(window_seconds),
            },
        )

    hits.append(now)
    _UPLOAD_HITS[key] = hits


def _enforce_inflight_limit_or_429(db: Session) -> None:
    """Backpressure: refuse to enqueue more background work when saturated.

    Enable by setting `CLA_MAX_INFLIGHT_INGESTIONS` to a positive integer.
    When enabled, if the number of ingestions with status RECEIVED or PROCESSING
    is >= the limit, this raises a 429 with Retry-After.
    """

    limit = _int_env("CLA_MAX_INFLIGHT_INGESTIONS")
    if limit is None or limit <= 0:
        return

    inflight = db.execute(
        select(func.count())
        .select_from(Ingestion)
        .where(
            Ingestion.status.in_(
                [IngestionStatus.RECEIVED, IngestionStatus.PROCESSING]
            )
        )
    ).scalar_one()

    if int(inflight) >= int(limit):
        retry_after_s = _int_env("CLA_RETRY_AFTER_SECONDS") or 1
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            headers={"Retry-After": str(int(retry_after_s))},
            detail={
                "code": "INGESTION_BACKPRESSURE",
                "retryable": True,
                "message": (
                    "Server is at capacity for queued/in-flight ingestions; retry later."
                ),
                "limit": int(limit),
                "inflight": int(inflight),
            },
        )


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
    request: Request,
    background_tasks: BackgroundTasks,
    response: Response,
    file: Annotated[UploadFile, File()],
    metadata: IngestionMetadata = Depends(IngestionMetadata.as_form),
    db: Session = Depends(get_session),
):
    """
    Logic:
    - [handled by FastAPI automatically].
       Check media type. If not multipart/form-data:
        - return code 415
    - Check that all required fields present. If not, return 422 - Missing field
        - if file size exceeds limit, return 413
    - Generate content hash server_sha256
        - if content_sha256 provided
            - if content_sha256 != server_sha256:
                - return 400 Content hash mismatch
    - check if combination of instrument_id and run_id present in database
        already. if yes, retrieve server_sha256.
        - compare server_sha256_new to server_sha256.
            - if differ, return 409
            - if the same, return 200

    - return 202


    """
    # Per-IP cost guard: reject abusive clients before any work (hashing, DB
    # lookups, or enqueuing the Bedrock-spending background task). Placed before the
    # dedup check on purpose — a client re-running the demo faster than the window
    # is exactly what we want to throttle. This is distinct from the inflight check
    # below, which guards queue saturation rather than per-client abuse/cost.
    _enforce_upload_rate_limit_or_429(request)

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

    if existing_ingestion:
        if existing_ingestion.server_sha256 == server_sha256_new:
            response.headers["Location"] = (
                f"/v1/ingestions/{existing_ingestion.ingestion_id}"
            )
            response.status_code = status.HTTP_200_OK
            return IngestionDuplicateOkResponse(
                existing_ingestion_id=str(existing_ingestion.ingestion_id),
                message="The run was already submitted.",
            )

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
                message=(
                    "An ingestion already exists for the run (instrument_id, run_id) "
                    "but server-produced hash differs."
                ),
            ).model_dump(),
        )

    # Backpressure: do not accept more work if too many ingestions are queued/in-flight.
    _enforce_inflight_limit_or_429(db)

    # Create new records
    new_ingestion_id = uuid4()
    new_ingestion_api_received_at = datetime.now(timezone.utc)

    # Browser-driven uploads carry a session cookie (see /v1/session/start);
    # cookie-less callers (CLI demo, or a browser that didn't send the cookie)
    # have none and get session_id=NULL — but every SESSION ingestion still
    # gets a TTL so the periodic purge can reclaim it. Otherwise a cookie-less
    # upload would persist forever and pollute a patient's history. Only SEED
    # rows (created by the seed script, not this endpoint) are permanent.
    raw_session_id = request.cookies.get(SESSION_COOKIE_NAME)
    session_id: Optional[UUID] = None
    if raw_session_id:
        try:
            session_id = UUID(raw_session_id)
        except ValueError:
            session_id = None
    expires_at: datetime = new_ingestion_api_received_at + timedelta(
        minutes=SESSION_TTL_MINUTES
    )

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
        kind="SESSION",
        session_id=session_id,
        expires_at=expires_at,
    )
    new_raw_data_record = RawData(
        ingestion_id=new_ingestion_id,
        content_bytes=file_content,
        content_mime=file.content_type,
        content_size_bytes=len(file_content),
    )

    IngestionRepository(db).create(new_ingestion_record)
    RawDataRepository(db).create(new_raw_data_record)
    try:
        db.commit()
    except IntegrityError:
        # Race-safe idempotency: another request may have inserted the same
        # (instrument_id, run_id) after our initial existence check.
        db.rollback()
        existing_ingestion = IngestionRepository(
            db
        ).get_by_instrument_id_run_id(metadata.instrument_id, metadata.run_id)
        if existing_ingestion is None:
            raise

        if existing_ingestion.server_sha256 == server_sha256_new:
            response.headers["Location"] = (
                f"/v1/ingestions/{existing_ingestion.ingestion_id}"
            )
            response.status_code = status.HTTP_200_OK
            return IngestionDuplicateOkResponse(
                existing_ingestion_id=str(existing_ingestion.ingestion_id),
                message="The run was already submitted.",
            )

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
                message=(
                    "An ingestion already exists for the run (instrument_id, run_id) "
                    "but server-produced hash differs."
                ),
            ).model_dump(),
        )

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

    # Backpressure for manual trigger
    _enforce_inflight_limit_or_429(session)

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
    response_model_exclude_unset=True,
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
        row_kwargs: dict[str, Any] = {
            "diagnostic_report_id": dr_row.diagnostic_report_id,
            "patient_id": dr_row.patient_id,
            "panel_code": dr_row.panel_code,
            "effective_at": dr_row.effective_at,
            "normalized_at": dr_row.normalized_at,
            "status": "FINAL",
        }
        if want_json:
            row_kwargs["resource_json"] = dr_row.resource_json

        row_response = ReadDiagnosticReportsOkResponse(**row_kwargs)
        list_row_responses.append(row_response)

    return list_row_responses


@router.get(
    "/ingestions/{ingestion_id}/observations",
    response_model=list[ReadObservationsOkResponse],
    response_model_exclude_unset=True,
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
        row_kwargs: dict[str, Any] = {
            "observation_id": ob_row.observation_id,
            "diagnostic_report_id": ob_row.diagnostic_report_id,
            "patient_id": ob_row.patient_id,
            "code": ob_row.code,
            "display": ob_row.display,
            "effective_at": ob_row.effective_at,
            "normalized_at": ob_row.normalized_at,
            "value_num": ob_row.value_num,
            "value_text": ob_row.value_text,
            "comparator": ob_row.comparator,
            "unit": ob_row.unit,
            "ref_low_num": ob_row.ref_low_num,
            "ref_high_num": ob_row.ref_high_num,
            "flag_analyzer_interpretation": ob_row.flag_analyzer_interpretation,
            "flag_system_interpretation": ob_row.flag_system_interpretation,
            "discrepancy": ob_row.discrepancy,
            "status": "FINAL",
        }
        if want_json:
            row_kwargs["resource_json"] = ob_row.resource_json

        row_response = ReadObservationsOkResponse(**row_kwargs)
        list_row_responses.append(row_response)

    return list_row_responses


@router.get(
    "/ingestions/{ingestion_id}/processing-events",
    response_model=list[ReadProcessingEventOkResponse],
    response_model_exclude_unset=True,
    responses={
        status.HTTP_404_NOT_FOUND: {
            "model": PathResourceNotFoundResponse,
            "description": "Item not found",
        },
    },
)
def read_processing_events_for_ingestion_id(
    ingestion_id: UUID,
    db: Session = Depends(get_session),
) -> list[ReadProcessingEventOkResponse]:
    pe_repo = ProcessingEventRepository(db)
    pe_rows = pe_repo.list_by_ingestion_id(ingestion_id)

    if not pe_rows:
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

    out: list[ReadProcessingEventOkResponse] = []
    for row in pe_rows:
        out.append(
            ReadProcessingEventOkResponse(
                event_id=row.event_id,
                ingestion_id=row.ingestion_id,
                occurred_at=row.occurred_at,
                event_type=row.event_type.value,
                actor=row.actor.value,
                severity=row.severity.value,
                message=row.message,
                details=row.details,
            )
        )
    return out


@router.get(
    "/ingestions/{ingestion_id}/ai_annotation",
    response_model=list[ReadAiAnnotationOkResponse],
    response_model_exclude_unset=True,
    responses={
        status.HTTP_404_NOT_FOUND: {
            "model": PathResourceNotFoundResponse,
            "description": "Item not found",
        },
    },
)
def read_ai_annotations_for_ingestion_id(
    ingestion_id: UUID,
    db: Session = Depends(get_session),
) -> list[ReadAiAnnotationOkResponse]:
    ai_repo = AiAnnotationRepository(db)
    ai_rows = ai_repo.get_by_ingestion_id(ingestion_id)

    if not ai_rows:
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

    out: list[ReadAiAnnotationOkResponse] = []
    for row in ai_rows:
        out.append(
            ReadAiAnnotationOkResponse(
                ai_annotation_id=row.ai_annotation_id,
                ingestion_id=row.ingestion_id,
                annotation_type=(
                    row.annotation_type.value
                    if row.annotation_type is not None
                    else None
                ),
                content_json=row.content_json,
                provider=row.provider,
                model_id=row.model_id,
                prompt_version=row.prompt_version,
                temperature=row.temperature,
                content_schema_version=row.content_schema_version,
                input_hash=row.input_hash,
                created_at=row.created_at,
                validation_status=(
                    row.validation_status.value
                    if row.validation_status is not None
                    else None
                ),
                validated_at=row.validated_at,
                rejection_reason=row.rejection_reason,
            )
        )
    return out


def _patient_message_response(
    db: Session, message: PatientMessage
) -> ReadPatientMessageOkResponse:
    """Assemble the read model, applying synthetic PHI (name/email) only here,
    at render time — never from draft_content_json."""
    patient = PatientRepository(db).get(message.patient_id)
    return ReadPatientMessageOkResponse(
        patient_message_id=message.patient_message_id,
        ingestion_id=message.ingestion_id,
        patient_id=message.patient_id,
        patient_given_name=patient.given_name if patient else None,
        patient_family_name=patient.family_name if patient else None,
        patient_email=patient.email if patient else None,
        draft_content_json=message.draft_content_json,
        final_content_json=message.final_content_json,
        content_schema_version=message.content_schema_version,
        correlation_id=message.correlation_id,
        generation_event_id=message.generation_event_id,
        provider=message.provider,
        model_id=message.model_id,
        prompt_version=message.prompt_version,
        temperature=message.temperature,
        input_hash=message.input_hash,
        retrieved_refs_json=message.retrieved_refs_json,
        created_at=message.created_at,
        validation_status=message.validation_status.value,
        validated_at=message.validated_at,
        validation_error=message.validation_error,
        review_status=message.review_status.value,
        reviewed_by=message.reviewed_by,
        approved_by=message.approved_by,
        reviewed_at=message.reviewed_at,
        approved_at=message.approved_at,
        sent_at=message.sent_at,
        review_note=message.review_note,
        superseded_by=message.superseded_by,
    )


@router.get(
    "/ingestions/{ingestion_id}/patient_message",
    response_model=ReadPatientMessageOkResponse,
    response_model_exclude_unset=True,
    responses={
        status.HTTP_404_NOT_FOUND: {
            "model": PathResourceNotFoundResponse,
            "description": "Item not found",
        },
    },
)
def read_patient_message_for_ingestion_id(
    ingestion_id: UUID,
    db: Session = Depends(get_session),
) -> ReadPatientMessageOkResponse:
    message = PatientMessageRepository(db).get_active_by_ingestion_id(
        ingestion_id
    )
    if message is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=PathResourceNotFoundResponse(
                ingestion_id=ingestion_id,
                detail="Item not found",
            ).model_dump(mode="json", exclude_none=True),
        )
    return _patient_message_response(db, message)


def _patient_message_or_404(
    service: PatientMessageService, patient_message_id: UUID
) -> PatientMessage:
    try:
        return service.get(patient_message_id)
    except PatientMessageNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Patient message not found",
        )


@router.post(
    "/patient_messages/{patient_message_id}/approve",
    response_model=ReadPatientMessageOkResponse,
    response_model_exclude_unset=True,
)
def approve_patient_message(
    patient_message_id: UUID,
    body: ApprovePatientMessageRequest,
    db: Session = Depends(get_session),
) -> ReadPatientMessageOkResponse:
    service = PatientMessageService(db)
    _patient_message_or_404(service, patient_message_id)
    try:
        message = service.approve(
            patient_message_id,
            approved_by=body.approved_by,
            final_content_json=body.final_content_json,
        )
    except InvalidTransitionError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail=str(exc)
        )
    return _patient_message_response(db, message)


@router.post(
    "/patient_messages/{patient_message_id}/request_changes",
    response_model=ReadPatientMessageOkResponse,
    response_model_exclude_unset=True,
)
def request_changes_patient_message(
    patient_message_id: UUID,
    body: ReviewPatientMessageRequest,
    db: Session = Depends(get_session),
) -> ReadPatientMessageOkResponse:
    service = PatientMessageService(db)
    _patient_message_or_404(service, patient_message_id)
    try:
        message = service.request_changes(
            patient_message_id,
            reviewed_by=body.reviewed_by,
            note=body.note or "",
        )
    except InvalidTransitionError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail=str(exc)
        )
    return _patient_message_response(db, message)


@router.post(
    "/patient_messages/{patient_message_id}/reject",
    response_model=ReadPatientMessageOkResponse,
    response_model_exclude_unset=True,
)
def reject_patient_message(
    patient_message_id: UUID,
    body: ReviewPatientMessageRequest,
    db: Session = Depends(get_session),
) -> ReadPatientMessageOkResponse:
    service = PatientMessageService(db)
    _patient_message_or_404(service, patient_message_id)
    try:
        message = service.reject(
            patient_message_id,
            reviewed_by=body.reviewed_by,
            note=body.note,
        )
    except InvalidTransitionError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail=str(exc)
        )
    return _patient_message_response(db, message)


@router.post(
    "/patient_messages/{patient_message_id}/send",
    response_model=ReadPatientMessageOkResponse,
    response_model_exclude_unset=True,
)
def send_patient_message(
    patient_message_id: UUID,
    db: Session = Depends(get_session),
) -> ReadPatientMessageOkResponse:
    """Demo-send: flips APPROVED -> SENT. No external delivery."""
    service = PatientMessageService(db)
    _patient_message_or_404(service, patient_message_id)
    try:
        message = service.send(patient_message_id)
    except InvalidTransitionError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail=str(exc)
        )
    return _patient_message_response(db, message)


# `GET /v1/patients/{patient_id}/diagnostic-reports?include_json=1&limit=...&offset=...`
@router.get(
    "/patients/{patient_id}/diagnostic-reports",
    response_model=list[ReadDiagnosticReportsOkResponse],
    response_model_exclude_unset=True,
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
        row_kwargs: dict[str, Any] = {
            "diagnostic_report_id": dr_row.diagnostic_report_id,
            "patient_id": dr_row.patient_id,
            "panel_code": dr_row.panel_code,
            "effective_at": dr_row.effective_at,
            "normalized_at": dr_row.normalized_at,
            "status": "FINAL",
        }
        if want_json:
            row_kwargs["resource_json"] = dr_row.resource_json

        row_response = ReadDiagnosticReportsOkResponse(**row_kwargs)
        list_row_responses.append(row_response)

    return list_row_responses


# `GET /v1/patients/{patient_id}/observations?include_json=1&limit=...&offset=...`
@router.get(
    "/patients/{patient_id}/observations",
    response_model=list[ReadObservationsOkResponse],
    response_model_exclude_unset=True,
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
        row_kwargs: dict[str, Any] = {
            "observation_id": ob_row.observation_id,
            "diagnostic_report_id": ob_row.diagnostic_report_id,
            "patient_id": ob_row.patient_id,
            "code": ob_row.code,
            "display": ob_row.display,
            "effective_at": ob_row.effective_at,
            "normalized_at": ob_row.normalized_at,
            "value_num": ob_row.value_num,
            "value_text": ob_row.value_text,
            "comparator": ob_row.comparator,
            "unit": ob_row.unit,
            "ref_low_num": ob_row.ref_low_num,
            "ref_high_num": ob_row.ref_high_num,
            "flag_analyzer_interpretation": ob_row.flag_analyzer_interpretation,
            "flag_system_interpretation": ob_row.flag_system_interpretation,
            "discrepancy": ob_row.discrepancy,
            "status": "FINAL",
        }
        if want_json:
            row_kwargs["resource_json"] = ob_row.resource_json

        row_response = ReadObservationsOkResponse(**row_kwargs)
        list_row_responses.append(row_response)

    return list_row_responses
