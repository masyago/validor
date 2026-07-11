import os
from contextlib import asynccontextmanager

from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from fastapi import FastAPI
from sqlalchemy.orm import Session

from app.api.routers.ingestion import router as ingestion_router
from app.api.routers.session import router as session_router
from app.persistence.db import engine
from app.persistence.repositories.ingestion_repo import IngestionRepository

load_dotenv()

SESSION_PURGE_INTERVAL_MINUTES = int(
    os.getenv("SESSION_PURGE_INTERVAL_MINUTES", "5")
)

scheduler = BackgroundScheduler()


def purge_expired_sessions() -> None:
    with Session(engine) as session:
        deleted = IngestionRepository(session).delete_expired_sessions()
        session.commit()
        if deleted:
            print(f"purge_expired_sessions: deleted {deleted} ingestion(s)")


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.add_job(
        purge_expired_sessions,
        "interval",
        minutes=SESSION_PURGE_INTERVAL_MINUTES,
        id="purge_expired_sessions",
    )
    scheduler.start()
    yield
    scheduler.shutdown(wait=False)


app = FastAPI(lifespan=lifespan)

app.include_router(ingestion_router, prefix="/v1")
app.include_router(session_router, prefix="/v1")


@app.get("/")
def main():
    return {"message": "Hello from clinical-lab-analyzer!"}


if __name__ == "__main__":
    main()
