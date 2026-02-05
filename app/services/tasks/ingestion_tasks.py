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
        svc = IngestionService(session)
        svc.process_ingestion(ingestion_id)
        session.commit()
    except Exception as e:
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

        raise
    finally:
        session.close()
