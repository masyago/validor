from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy.orm import sessionmaker

from app.core.ingestion_status_enums import IngestionStatus
from app.persistence.db import engine
from app.persistence.models.core import Ingestion, RawData


def main() -> None:
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    try:
        new_ingestion_id = uuid4()
        ing = Ingestion(
            ingestion_id=new_ingestion_id,
            instrument_id="SIM",
            run_id="warmup",
            uploader_id="local",
            spec_version="1",
            uploader_received_at=datetime(2026, 3, 25, tzinfo=timezone.utc),
            api_received_at=datetime.now(timezone.utc),
            submitted_sha256=None,
            server_sha256="deadbeef",
            status=IngestionStatus.RECEIVED,
            source_filename="warmup.csv",
        )
        raw = RawData(
            ingestion_id=new_ingestion_id,
            content_bytes=b"abc",
            content_mime="text/csv",
            content_size_bytes=3,
        )
        session.add(ing)
        session.add(raw)
        session.commit()
        print("commit ok")
    finally:
        session.close()


if __name__ == "__main__":
    main()
