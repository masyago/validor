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


"""
Methods to add:
- do we need something to retrieve specific values from the result row (eg hash?)
- add a new row 
"""


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


"""
Methods to add:
- add a new row 
- get CSV file (for further parsing, normalization)
"""


# class PanelRepository:
#     def __init__(self, session: Session):
#         self.session = session

#     def create(self, panel: Panel) -> Panel:
#         self.session.add(panel)
#         self.session.flush()
#         return panel


"""
Methods to add:
- add a new row 
"""


# class TestRepository:
#     def __init__(self, session: Session):
#         self.session = session

#     def create(self, test: Test) -> Test:
#         self.session.add(test)
#         self.session.flush()
#         return test
