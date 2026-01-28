from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import os

# Use an environment variable to determine the database URL
# Default to development DB if not in testing environment
if os.getenv("ENV") == "testing":
    DATABASE_URL = "postgresql://user:password@localhost/test_cla"
else:
    DATABASE_URL = "postgresql+psycopg://localhost:5432/cla"


DATABASE_URL = "postgresql+psycopg://localhost:5432/cla"

engine = create_engine(DATABASE_URL, echo=True)


def get_session():
    with Session(engine) as session:
        yield session
