from sqlalchemy.orm import Session
from sqlalchemy import select
from uuid import UUID
from datetime import datetime, timezone

from app.persistence.models.core import RawData, Ingestion
from app.core.ingestion_status_enums import IngestionStatus


class RawDataRepository:
    def __init__(self, session: Session):
        self.session = session

    def get_by_ingestion_id(self, ingestion_id: UUID) -> RawData | None:
        stmt = select(RawData).where(RawData.ingestion_id == ingestion_id)
        return self.session.scalars(stmt).one_or_none()

    def create(self, raw_data: RawData) -> RawData:
        self.session.add(raw_data)
        self.session.flush()
        return raw_data

    def get_content_bytes(self, ingestion_id: UUID) -> bytes:
        stmt = select(RawData).where(RawData.ingestion_id == ingestion_id)
        result = self.session.scalars(stmt).one()
        return result.content_bytes
