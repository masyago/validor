from sqlalchemy.orm import relationship, DeclarativeBase, mapped_column, Mapped
from sqlalchemy.sql.sqltypes import TIMESTAMP
from sqlalchemy.sql.expression import text
from sqlalchemy import (
    Column,
    Integer,
    String,
    LargeBinary,
    Integer,
    Text,
    BigInteger,
    func,
    Uuid,
    text,
    ForeignKey,
    Enum as SqlEnum,
    Numeric,
    UniqueConstraint,
    Index,
    CheckConstraint,
)
import uuid
from typing import Optional
from sqlalchemy.dialects.postgresql import JSONB, ENUM
import enum
from datetime import datetime
from app.persistence.base import Base


# class IngestionIdempotencyDisposition(enum.Enum):
#     CREATED = "CREATED"
#     DUPLICATE_IDENTICAL = "DUPLICATE_IDENTICAL"
#     CONFLICT = "CONFLICT"


# ingestion_idempotency_enum = SqlEnum(
#     IngestionIdempotencyDisposition,
#     name="ingestion_idempotency_disposition_enum",
#     create_type=True,
# )


class Ingestion(Base):
    __tablename__ = "ingestion"
    __table_args__ = (
        UniqueConstraint(
            "instrument_id", "run_id", name="unique_instrument_run"
        ),
        CheckConstraint(
            "(ingestion_idempotency_disposition IS NULL OR ingestion_idempotency_disposition IN ('CREATED', 'DUPLICATE_IDENTICAL', 'CONFLICT'))",
            name="ck_ingestion_idempotency_disposition",
        ),
    )

    ingestion_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, primary_key=True, default=uuid.uuid4
    )
    instrument_id: Mapped[str] = mapped_column(Text, nullable=False)
    run_id: Mapped[str] = mapped_column(Text, nullable=False)
    uploader_id: Mapped[str] = mapped_column(Text, nullable=False)
    spec_version: Mapped[str] = mapped_column(Text, nullable=False)
    uploader_received_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )
    api_received_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )
    submitted_sha256: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True
    )
    server_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    error_code: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error_detail: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    source_filename: Mapped[str] = mapped_column(Text, nullable=False)
    ingestion_idempotency_disposition: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True
    )


class RawData(Base):
    __tablename__ = "raw_data"

    ingestion_id: Mapped[uuid.UUID] = mapped_column(
        Uuid,
        ForeignKey("ingestion.ingestion_id"),
        primary_key=True,
        default=uuid.uuid4,  # Generate UUID in Python
    )  # FK to ingestion table
    content_bytes: Mapped[bytes] = mapped_column(
        LargeBinary, nullable=False
    )  # BYTEA
    content_mime: Mapped[str] = mapped_column(
        Text, default="text/csv", nullable=False
    )
    content_size_bytes: Mapped[Optional[int]] = mapped_column(
        BigInteger, nullable=True
    )
