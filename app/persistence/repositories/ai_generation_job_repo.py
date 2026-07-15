from __future__ import annotations

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.sql import func

from app.persistence.models.patient import AiGenerationJob, AiGenerationJobType


class AiGenerationJobRepository:
    """
    Trusted-side correlation map. `create` mints the token->patient mapping;
    `consume` recovers `patient_id` by LOOKING UP the token (one-time use),
    never by decoding it.
    """

    def __init__(self, session: Session):
        self.session = session

    def create(
        self,
        *,
        correlation_id: UUID,
        job_type: AiGenerationJobType,
        patient_id: str,
        ingestion_id: UUID,
    ) -> AiGenerationJob:
        job = AiGenerationJob(
            correlation_id=correlation_id,
            job_type=job_type,
            patient_id=patient_id,
            ingestion_id=ingestion_id,
        )
        self.session.add(job)
        self.session.flush()
        return job

    def get(self, correlation_id: UUID) -> AiGenerationJob | None:
        stmt = select(AiGenerationJob).where(
            AiGenerationJob.correlation_id == correlation_id
        )
        return self.session.scalars(stmt).one_or_none()

    def consume(self, correlation_id: UUID) -> AiGenerationJob | None:
        """
        Resolve the token and mark it consumed. Returns the job only if it
        existed and had not already been consumed (one-time use).
        """
        job = self.get(correlation_id)
        if job is None or job.consumed_at is not None:
            return None
        job.consumed_at = func.now()
        self.session.flush()
        return job
