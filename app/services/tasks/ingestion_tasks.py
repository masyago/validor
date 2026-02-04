from uuid import UUID
from sqlalchemy.orm import Session
from app.persistence.db import engine
from app.persistence.repositories.raw_data_repo import RawDataRepository
from app.persistence.repositories.ingestion_repo import IngestionRepository
from app.persistence.repositories.panel_repo import PanelRepository
from app.persistence.repositories.test_repo import TestRepository
from app.services.ingestion_service import IngestionService


def process_ingestion_task(ingestion_id: UUID) -> None:
    session = Session(engine)
    try:
        svc = IngestionService(
            raw_repo=RawDataRepository(session),
            ingestion_repo=IngestionRepository(session),
            panel_repo=PanelRepository(session),
            test_repo=TestRepository(session),
        )
        svc.process_ingestion(ingestion_id)
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
