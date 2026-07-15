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
    Boolean,
)
import uuid
from typing import Optional
from sqlalchemy.dialects.postgresql import JSONB, ENUM
import enum
from datetime import datetime

from pgvector.sqlalchemy import Vector
from app.persistence.base import Base


class ChunkType(enum.Enum):
    IDENTITY = "IDENTITY"
    CLINICAL_CONTEXT = "CLINICAL_CONTEXT"
    REF_RANGES = "REF_RANGES"
    INTERPRETATION = "INTERPRETATION"
    CONFOUNDERS = "CONFOUNDERS"


chunk_type_enum = SqlEnum(
    ChunkType,
    name="chunk_type_enum",
    create_type=True,  # Set to False after first migration
)


# Index note: Default params ok for demo corpus; tune m and ef_construction
# based on recall benchmarks before scaling beyond 10k rows.
class VectorStore(Base):
    __tablename__ = "vector_store"
    __table_args__ = (
        UniqueConstraint(
            "source_id",
            "chunk_index",
            "embedding_model",
            "pipeline_version",
            name="uq_vector_chunk_model_pipeline",
        ),
        Index(
            "ix_vector_store_embedding_hnsw",
            "embedding",
            postgresql_using="hnsw",
            postgresql_with={"m": 16, "ef_construction": 64},
            postgresql_ops={"embedding": "vector_cosine_ops"},
        ),
    )

    embedding_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, primary_key=True, default=uuid.uuid4
    )
    embedding: Mapped[list[float]] = mapped_column(
        Vector(1536), nullable=False
    )  # model: text-embedding-3-small

    source_id: Mapped[uuid.UUID] = mapped_column(Uuid, nullable=False)
    chunk_index: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0
    )
    chunk_type: Mapped[ChunkType] = mapped_column(
        chunk_type_enum, nullable=False
    )
    chunk_text: Mapped[str] = mapped_column(Text, nullable=False)
    content_hash: Mapped[str] = mapped_column(
        String(64), nullable=False, index=True
    )

    embedding_model: Mapped[str] = mapped_column(Text, nullable=False)
    pipeline_version: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    is_current: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True
    )

    embedding_model: Mapped[str] = mapped_column(Text, nullable=False)
    pipeline_version: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    is_current: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True
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
    content_hash: Mapped[str] = mapped_column(
        Text, nullable=False, unique=True
    )


# AI Annotation model and supporting classes


class AIAnnotationType(enum.Enum):
    ANOMALY_FLAG = "anomaly_flag"
    POSSIBLE_INTERFERENCE = "possible_interference"
    FOLLOWUP_SUGGESTION = "followup_suggestion"


ai_annotation_type_enum = SqlEnum(
    AIAnnotationType,
    name="ai_annotation_type_enum",
    create_type=True,
)


class AIAnnotationValidationStatus(enum.Enum):
    PENDING = "PENDING"
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"


ai_annotation_validation_status_enum = SqlEnum(
    AIAnnotationValidationStatus,
    name="ai_annotation_validation_status_enum",
    create_type=True,
)


class AiAnnotation(Base):
    __tablename__ = "ai_annotation"
    __table_args__ = (
        Index("ix_ai_annotation_annotation_type", "annotation_type"),
        Index("ix_ai_annotation_validation_status", "validation_status"),
    )

    # Identifiers
    ai_annotation_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, primary_key=True, default=uuid.uuid4
    )
    ingestion_id: Mapped[uuid.UUID] = mapped_column(
        Uuid,
        ForeignKey("ingestion.ingestion_id", ondelete="CASCADE"),
        nullable=False,
    )

    # Annotation
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
    # Job-scoped token minted on the trusted side; the de-identification
    # boundary that keeps patient_id/PHI out of the AI layer. See
    # AiGenerationJob and app/ai/ai_orchestration.py.
    correlation_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        Uuid, nullable=True
    )
    created_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )

    # Status

    validation_status: Mapped[Optional[AIAnnotationValidationStatus]] = (
        mapped_column(ai_annotation_validation_status_enum, nullable=True)
    )
    validated_at: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    rejection_reason: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True
    )
