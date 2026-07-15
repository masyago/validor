from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, Request, Response
from sqlalchemy.orm import Session

from app.api.routers.dependencies import get_session
from app.core.session_config import SESSION_COOKIE_NAME, SESSION_TTL_MINUTES
from app.persistence.repositories.ingestion_repo import IngestionRepository

router = APIRouter()


@router.post("/session/start")
def start_session(
    request: Request,
    response: Response,
    db: Session = Depends(get_session),
):
    """Ensure a session cookie exists and purge any ingestion left over
    under it — covers a reopened tab without waiting for the TTL sweep."""
    raw_session_id = request.cookies.get(SESSION_COOKIE_NAME)
    try:
        session_id = UUID(raw_session_id) if raw_session_id else uuid4()
    except ValueError:
        session_id = uuid4()

    purged = IngestionRepository(db).delete_by_session_id(session_id)

    response.set_cookie(
        SESSION_COOKIE_NAME,
        str(session_id),
        max_age=SESSION_TTL_MINUTES * 60,
        httponly=True,
        samesite="lax",
    )

    return {"session_id": str(session_id), "purged_ingestions": purged}
