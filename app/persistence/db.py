from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from collections.abc import Generator
import os


def _bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _default_database_url() -> str:
    if os.getenv("ENV") == "testing":
        return os.getenv(
            "TEST_DATABASE_URL",
            "postgresql+psycopg://postgres:postgres@localhost:5432/test_cla",
        )
    return "postgresql+psycopg://postgres:postgres@localhost:5432/cla"


def _normalize_database_url(url: str) -> str:
    """
    Normalize provider URLs to a SQLAlchemy URL that uses psycopg.
    Render and other providers use either "postgres://" or "postgres://",
    need to convert them to "postgresql+psycopg://"

    """

    url = url.strip()

    if url.startswith("postgres://"):
        url = "postgresql://" + url.removeprefix("postgres://")

    if url.startswith("postgresql://"):
        url = "postgresql+psycopg://" + url.removeprefix("postgresql://")

    return url


DATABASE_URL = _normalize_database_url(
    os.getenv("DATABASE_URL") or _default_database_url()
)

_default_echo = os.getenv("ENV") != "testing"
engine = create_engine(
    DATABASE_URL, echo=_bool_env("SQLALCHEMY_ECHO", _default_echo)
)

# Optional query metrics (totals + fingerprints) for benchmark runs.
# Enable with: CLA_QUERY_METRICS=1
if _bool_env("CLA_QUERY_METRICS", False):
    from app.metrics.sqlalchemy_query_metrics import install_listeners

    install_listeners(engine)
