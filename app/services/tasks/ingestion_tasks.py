from uuid import UUID
from sqlalchemy.orm import Session
from app.persistence.db import engine
from app.persistence.repositories.ingestion_repo import (
    RawDataRepository,
    IngestionRepository,
    PanelRepository,
    TestRepository,
)
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
