from __future__ import annotations

from uuid import UUID

from sqlalchemy import asc, select
from sqlalchemy.orm import Session

from app.persistence.models.patient_message import (
    PatientMessage,
    PatientMessageReviewStatus,
)

_INACTIVE_REVIEW_STATUSES = (
    PatientMessageReviewStatus.SUPERSEDED,
    PatientMessageReviewStatus.REJECTED,
)


class PatientMessageRepository:
    def __init__(self, session: Session):
        self.session = session

    def create(self, patient_message: PatientMessage) -> PatientMessage:
        self.session.add(patient_message)
        self.session.flush()
        return patient_message

    def get(self, patient_message_id: UUID) -> PatientMessage | None:
        stmt = select(PatientMessage).where(
            PatientMessage.patient_message_id == patient_message_id
        )
        return self.session.scalars(stmt).one_or_none()

    def get_active_by_ingestion_id(
        self, ingestion_id: UUID
    ) -> PatientMessage | None:
        """The single non-superseded, non-rejected message for an ingestion
        (enforced by the partial unique index)."""
        stmt = (
            select(PatientMessage)
            .where(PatientMessage.ingestion_id == ingestion_id)
            .where(
                PatientMessage.review_status.notin_(_INACTIVE_REVIEW_STATUSES)
            )
        )
        return self.session.scalars(stmt).one_or_none()

    def list_by_ingestion_id(
        self, ingestion_id: UUID
    ) -> list[PatientMessage]:
        stmt = (
            select(PatientMessage)
            .where(PatientMessage.ingestion_id == ingestion_id)
            .order_by(asc(PatientMessage.created_at))
        )
        return list(self.session.scalars(stmt).all())
