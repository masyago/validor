from __future__ import annotations

from uuid import UUID

from sqlalchemy import asc, select
from sqlalchemy.orm import Session

from app.persistence.models.ai import AiAnnotation


class AiAnnotationRepository:
    def __init__(self, session: Session):
        self.session = session

    def create(self, ai_annotation: AiAnnotation) -> AiAnnotation:
        self.session.add(ai_annotation)
        self.session.flush()
        return ai_annotation

    def get_by_ingestion_id(self, ingestion_id: UUID) -> list[AiAnnotation]:
        stmt = (
            select(AiAnnotation)
            .where(AiAnnotation.ingestion_id == ingestion_id)
            .order_by(asc(AiAnnotation.ai_annotation_id))
        )
        return list(self.session.scalars(stmt).all())
