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
        return "postgresql+psycopg://localhost:5432/test_cla"
    return "postgresql+psycopg://localhost:5432/cla"


DATABASE_URL = os.getenv("DATABASE_URL") or _default_database_url()

_default_echo = os.getenv("ENV") != "testing"
engine = create_engine(
    DATABASE_URL, echo=_bool_env("SQLALCHEMY_ECHO", _default_echo)
)
