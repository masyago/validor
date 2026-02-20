from __future__ import annotations

import logging
from uuid import UUID
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, func
from sqlalchemy.orm import Session

from app.core.ingestion_status_enums import IngestionStatus
from app.persistence.db import engine
from app.persistence.models.core import Ingestion
from app.persistence.models.provenance import (
    ProcessingEvent,
    ProcessingEventType,
)
from app.persistence.repositories.ingestion_repo import IngestionRepository
from app.services.ingestion_service import IngestionService


logger = logging.getLogger("uvicorn.error")


def process_ingestion_task(ingestion_id: UUID) -> None:
    logger.info(
        "process_ingestion_task started",
        extra={
            "ingestion_id": str(ingestion_id),
            "db_url": str(engine.url),
        },
    )
    session = Session(engine)
    try:
        ingestion = IngestionRepository(session).get_by_ingestion_id(ingestion_id)
        if ingestion is None:
            logger.error(
                "process_ingestion_task cannot see ingestion row",
                extra={
                    "ingestion_id": str(ingestion_id),
                    "db_url": str(engine.url),
                },
            )
            return

        svc = IngestionService(session)
        svc.process_ingestion(ingestion_id)
        session.commit()
        logger.info(
            "process_ingestion_task finished",
            extra={"ingestion_id": str(ingestion_id)},
        )
    except Exception as e:
        logger.exception(
            "process_ingestion_task crashed",
            extra={"ingestion_id": str(ingestion_id)},
        )
        session.rollback()

        # Persist FAILED status in a separate transaction
        fail_session = Session(engine)
        try:
            IngestionRepository(fail_session).mark_failed(
                ingestion_id=ingestion_id,
                error_code="exception",
                error_detail={"message": str(e), "type": type(e).__name__},
            )
            fail_session.commit()
        finally:
            fail_session.close()
    finally:
        session.close()


def reap_stuck_ingestions(
    session: Session,
    *,
    max_age_seconds: int = 15 * 60,
    limit: int = 50,
    dry_run: bool = False,
) -> dict[str, int | list[str]]:
    """
    Best-effort recovery for ingestions stuck in PROCESSING.

    Strategy:
    - Find PROCESSING ingestions whose latest ProcessingEvent.occurred_at is older
      than `now - max_age_seconds`.
    - If normalization already succeeded (NORMALIZATION_SUCCEEDED event present) but ingestion status is still PROCESSING, mark it COMPLETED (heal).
    - Otherwise, requeue to RECEIVED and re-run processing.
    """

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(seconds=max_age_seconds)

    last_event_subq = (
        select(
            ProcessingEvent.ingestion_id.label("ingestion_id"),
            func.max(ProcessingEvent.occurred_at).label("last_occurred_at"),
        )
        .group_by(ProcessingEvent.ingestion_id)
        .subquery()
    )

    stmt = (
        select(Ingestion.ingestion_id)
        .outerjoin(
            last_event_subq,
            last_event_subq.c.ingestion_id == Ingestion.ingestion_id,
        )
        .where(Ingestion.status == IngestionStatus.PROCESSING)
        .where(
            (last_event_subq.c.last_occurred_at < cutoff)
            | (
                (last_event_subq.c.last_occurred_at.is_(None))
                & (Ingestion.api_received_at < cutoff)
            )
        )
        .limit(limit)
    )

    ingestion_ids = list(session.scalars(stmt))

    repo = IngestionRepository(session)
    completed = 0
    retried = 0
    failed_retries = 0
    acted_on: list[str] = []

    for ingestion_id in ingestion_ids:
        try:
            norm_success = session.execute(
                select(ProcessingEvent.event_id)
                .where(ProcessingEvent.ingestion_id == ingestion_id)
                .where(
                    ProcessingEvent.event_type.in_(
                        [
                            ProcessingEventType.NORMALIZATION_SUCCEEDED,
                            ProcessingEventType.NORMALIZATION_SUCCEEDED_WITH_WARNINGS,
                        ]
                    )
                )
                .limit(1)
            ).first()

            if norm_success is not None:
                if not dry_run:
                    repo.mark_completed(ingestion_id)
                    session.commit()
                completed += 1
                acted_on.append(str(ingestion_id))
                continue

            if not dry_run:
                if not repo.requeue_processing(ingestion_id):
                    continue
                session.commit()

                svc = IngestionService(session)
                svc.process_ingestion(ingestion_id)

            retried += 1
            acted_on.append(str(ingestion_id))
        except Exception:
            failed_retries += 1
            session.rollback()

    return {
        "considered": len(ingestion_ids),
        "completed": completed,
        "retried": retried,
        "failed_retries": failed_retries,
        "ingestion_ids": acted_on,
    }


def reap_stuck_ingestions_task(
    *,
    max_age_seconds: int = 15 * 60,
    limit: int = 50,
    dry_run: bool = False,
) -> dict[str, int | list[str]]:
    session = Session(engine)
    try:
        return reap_stuck_ingestions(
            session,
            max_age_seconds=max_age_seconds,
            limit=limit,
            dry_run=dry_run,
        )
    finally:
        session.close()
