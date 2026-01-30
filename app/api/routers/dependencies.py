from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from collections.abc import Generator
import os

# Use an environment variable to determine the database URL
# Default to development DB if not in testing environment
if os.getenv("ENV") == "testing":
    DATABASE_URL = "postgresql://user:password@localhost/test_cla"
else:
    DATABASE_URL = "postgresql+psycopg://localhost:5432/cla"


DATABASE_URL = "postgresql+psycopg://localhost:5432/cla"

engine = create_engine(DATABASE_URL, echo=True)


def get_session() -> Generator[Session, None, None]:
    session = Session(engine)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
