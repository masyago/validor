from sqlalchemy.orm import Session
from sqlalchemy import select
from uuid import UUID
from datetime import datetime, timezone

from app.persistence.models.core import RawData, Ingestion
from app.core.ingestion_status_enums import IngestionStatus


class IngestionRepository:
    def __init__(self, session: Session):
        self.session = session

    def get_by_ingestion_id(self, ingestion_id: UUID) -> Ingestion | None:
        stmt = select(Ingestion).where(Ingestion.ingestion_id == ingestion_id)
        return self.session.scalars(stmt).one_or_none()

    def get_by_instrument_id_run_id(
        self, instrument_id: str, run_id: str
    ) -> Ingestion | None:
        stmt = select(Ingestion).where(
            Ingestion.instrument_id == instrument_id,
            Ingestion.run_id == run_id,
        )
        return self.session.scalars(stmt).one_or_none()

    def create(self, ingestion: Ingestion) -> Ingestion:
        self.session.add(ingestion)
        self.session.flush()
        return ingestion

    # For Background workers to claim the process
    def claim_for_processing(self, ingestion_id: UUID) -> bool:
        stmt = select(Ingestion).where(
            Ingestion.ingestion_id == ingestion_id,
            Ingestion.status == IngestionStatus.RECEIVED,
        )
        ingestion = self.session.scalars(stmt).one_or_none()
        if not ingestion:
            return False

        ingestion.status = IngestionStatus.PROCESSING
        # Think if I want to add it to table schema
        # ingestion.processing_started_at = datetime.now(timezone.utc)
        self.session.flush()
        return True

    def mark_failed_validation(
        self, ingestion_id: UUID, error_code, error_detail
    ) -> bool:

        stmt = select(Ingestion).where(Ingestion.ingestion_id == ingestion_id)
        ingestion = self.session.scalars(stmt).one_or_none()
        if not ingestion:
            return False

        ingestion.error_code = error_code
        ingestion.error_detail = error_detail
        ingestion.status = IngestionStatus.FAILED_VALIDATION

        self.session.flush()
        return True
