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
)
import uuid
from typing import Optional
from sqlalchemy.dialects.postgresql import JSONB, ENUM
import enum
from datetime import datetime
from pgvector.sqlalchemy import Vector
from app.persistence.base import Base

# Replace with CheckConstraint
# class VectorSourceType(enum.Enum):
#     DOCUMENT = "DOCUMENT"
#     OBSERVATION = "OBSERVATION"
#     DIAGNOSTIC_REPORT = "DIAGNOSTIC_REPORT"


# vector_source_type_enum = SqlEnum(
#     VectorSourceType,
#     name="vector_source_type_enum",
#     create_type=True,  # Set to False after first migration
# )


class VectorStore(Base):
    __tablename__ = "vector_store"
    __table_args__ = (
        UniqueConstraint(
            "source_type",
            "source_id",
            "chunk_index",
            "embedding_model",
            "pipeline_version",
            name="unique_vector_source_chunk_model_pipeline",
        ),
    )

    embedding_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, primary_key=True, default=uuid.uuid4
    )
    embedding: Mapped[list[float]] = mapped_column(
        Vector(768), nullable=False
    )  # change vector dimension depending on the model

    # Update with CheckConstraint
    source_type: Mapped[VectorSourceType] = mapped_column(
        vector_source_type_enum, nullable=False
    )
    source_id: Mapped[uuid.UUID] = mapped_column(Uuid, nullable=False)
    chunk_index: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0
    )
    chunk_text: Mapped[str] = mapped_column(Text, nullable=False)
    content_hash: Mapped[str] = mapped_column(Text, nullable=False)

    embedding_model: Mapped[str] = mapped_column(Text, nullable=False)
    pipeline_version: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )


class DocumentTargetType(enum.Enum):
    PANEL = "PANEL"
    ANALYTE = "ANALYTE"


document_target_type_enum = SqlEnum(
    DocumentTargetType,
    name="document_target_type_enum",
    create_type=True,  # Set to False after first migration
)


class Document(Base):
    __tablename__ = "document"

    doc_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, primary_key=True, default=uuid.uuid4
    )
    title: Mapped[str] = mapped_column(Text, nullable=False)
    target_type: Mapped[DocumentTargetType] = mapped_column(
        document_target_type_enum, nullable=False
    )
    target_code: Mapped[str] = mapped_column(Text, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    content_format: Mapped[str] = mapped_column(
        Text, nullable=False, default="text/plain"
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )
    last_updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )
    content_hash: Mapped[str] = mapped_column(Text, nullable=False)


# AI Annotation model and supporting classes


# Replace with CheckConstraints
# class AIAnnotationTargetType(enum.Enum):
#     DIAGNOSTIC_REPORT = "DIAGNOSTIC_REPORT"
#     OBSERVATION = "OBSERVATION"


# ai_annotation_target_type_enum = SqlEnum(
#     AIAnnotationTargetType,
#     name="ai_annotation_target_type_enum",
#     create_type=True,
# )


# class AIAnnotationType(enum.Enum):
#     ANOMALY_FLAG = "anomaly_flag"
#     POSSIBLE_INTERFERENCE = "possible_interference"
#     FOLLOWUP_SUGGESTION = "followup_suggestion"


# ai_annotation_type_enum = SqlEnum(
#     AIAnnotationType,
#     name="ai_annotation_type_enum",
#     create_type=True,
# )


# class AIAnnotationValidationStatus(enum.Enum):
#     PENDING = "PENDING"
#     ACCEPTED = "ACCEPTED"
#     REJECTED = "REJECTED"


# ai_annotation_validation_status_enum = SqlEnum(
#     AIAnnotationValidationStatus,
#     name="ai_annotation_validation_status_enum",
#     create_type=True,
# )


class AiAnnotation(Base):
    __tablename__ = "ai_annotation"
    __table_args__ = (
        Index("ix_ai_annotation_target_type_id", "target_type", "target_id"),
        Index("ix_ai_annotation_annotation_type", "annotation_type"),
        Index("ix_ai_annotation_validation_status", "validation_status"),
    )

    # Identifiers
    ai_annotation_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, primary_key=True, default=uuid.uuid4
    )
    ingestion_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("ingestion.ingestion_id"), nullable=False
    )

    # Update with CheckConstraint
    target_type: Mapped[Optional[AIAnnotationTargetType]] = mapped_column(
        ai_annotation_target_type_enum, nullable=True
    )
    target_id: Mapped[Optional[uuid.UUID]] = mapped_column(Uuid, nullable=True)

    # Annotation

    # Update with CheckConstraint
    annotation_type: Mapped[Optional[AIAnnotationType]] = mapped_column(
        ai_annotation_type_enum, nullable=True
    )
    content_json: Mapped[dict] = mapped_column(JSONB, nullable=False)

    # Traceability
    provider: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    model_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    prompt_version: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    temperature: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    content_schema_version: Mapped[str] = mapped_column(Text, nullable=False)
    input_hash: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )

    # Status

    # Update with CheckConstraint
    validation_status: Mapped[Optional[AIAnnotationValidationStatus]] = (
        mapped_column(ai_annotation_validation_status_enum, nullable=True)
    )
    validated_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    rejection_reason: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True
    )
