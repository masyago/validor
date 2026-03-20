from __future__ import annotations

import logging
import os
import time
from uuid import UUID
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, func
from sqlalchemy.orm import Session

from app.core.ingestion_status_enums import IngestionStatus
from app.persistence.db import engine
from app.persistence.models.core import Ingestion, RawData
from app.persistence.models.provenance import (
    ProcessingEvent,
    ProcessingEventType,
)
from app.persistence.repositories.ingestion_repo import IngestionRepository
from app.services.ingestion_service import IngestionService


logger = logging.getLogger("uvicorn.error")


def _bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


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
        ingestion = IngestionRepository(session).get_by_ingestion_id(
            ingestion_id
        )
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

        def _raw_content_size_bytes() -> int | None:
            return session.execute(
                select(RawData.content_size_bytes).where(
                    RawData.ingestion_id == ingestion_id
                )
            ).scalar_one_or_none()

        def _append_benchmark_row_if_enabled(
            *,
            measured_at_utc: datetime,
            wall_time_s: float | None,
            sql_query_count: int | None,
            sql_total_db_time_s: float | None,
            sql_top_by_total_time,
            sql_top_by_count,
        ) -> None:
            csv_path = os.getenv("CLA_BENCHMARK_RESULTS_CSV")
            if not csv_path:
                return

            try:
                from app.metrics.benchmark_csv_reporter import (
                    append_benchmark_row,
                )

                api_base_url = os.getenv("CLA_API_BASE_URL")
                dataset = os.getenv("CLA_BENCHMARK_DATASET")
                git_sha = os.getenv("CLA_GIT_SHA") or os.getenv("GIT_SHA")

                api_received_at = getattr(ingestion, "api_received_at", None)
                end_to_end_s = (
                    None
                    if api_received_at is None
                    else (measured_at_utc - api_received_at).total_seconds()
                )

                append_benchmark_row(
                    csv_path=csv_path,
                    measured_at=measured_at_utc,
                    git_sha=git_sha,
                    api_base_url=api_base_url,
                    dataset=dataset,
                    source_filename=getattr(
                        ingestion, "source_filename", None
                    ),
                    ingestion_id=str(ingestion_id),
                    instrument_id=getattr(ingestion, "instrument_id", None),
                    run_id=getattr(ingestion, "run_id", None),
                    uploader_id=getattr(ingestion, "uploader_id", None),
                    spec_version=getattr(ingestion, "spec_version", None),
                    status=getattr(ingestion, "status", None),
                    idempotency_disposition=getattr(
                        ingestion, "ingestion_idempotency_disposition", None
                    ),
                    error_code=getattr(ingestion, "error_code", None),
                    content_size_bytes=_raw_content_size_bytes(),
                    server_sha256=getattr(ingestion, "server_sha256", None),
                    submitted_sha256=getattr(
                        ingestion, "submitted_sha256", None
                    ),
                    uploader_received_at=getattr(
                        ingestion, "uploader_received_at", None
                    ),
                    api_received_at=api_received_at,
                    end_to_end_s=end_to_end_s,
                    wall_time_s=wall_time_s,
                    sql_query_count=sql_query_count,
                    sql_total_db_time_s=sql_total_db_time_s,
                    sql_top_by_total_time=sql_top_by_total_time,
                    sql_top_by_count=sql_top_by_count,
                )
            except Exception:
                logger.exception(
                    "benchmark_results_csv_append_failed",
                    extra={"ingestion_id": str(ingestion_id)},
                )

        if _bool_env("CLA_QUERY_METRICS", False):
            from app.metrics.sqlalchemy_query_metrics import collect_queries

            wall_start = time.perf_counter()
            with collect_queries() as qc:
                svc.process_ingestion(ingestion_id)
                session.commit()
            wall_s = time.perf_counter() - wall_start

            measured_at_utc = datetime.now(timezone.utc)

            _append_benchmark_row_if_enabled(
                measured_at_utc=measured_at_utc,
                wall_time_s=wall_s,
                sql_query_count=qc.query_count,
                sql_total_db_time_s=qc.total_db_time_s,
                sql_top_by_total_time=qc.top_by_total_time(10),
                sql_top_by_count=qc.top_by_count(10),
            )

            logger.info(
                "query_metrics",
                extra={
                    "ingestion_id": str(ingestion_id),
                    "wall_time_s": wall_s,
                    "sql_query_count": qc.query_count,
                    "sql_total_db_time_s": qc.total_db_time_s,
                    "sql_top_by_total_time": qc.top_by_total_time(10),
                    "sql_top_by_count": qc.top_by_count(10),
                },
            )
        else:
            svc.process_ingestion(ingestion_id)
            session.commit()

            measured_at_utc = datetime.now(timezone.utc)
            _append_benchmark_row_if_enabled(
                measured_at_utc=measured_at_utc,
                wall_time_s=None,
                sql_query_count=None,
                sql_total_db_time_s=None,
                sql_top_by_total_time=None,
                sql_top_by_count=None,
            )
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
