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
from app.persistence.base import Base


# Replace with CheckConstraint (=CHECK in postgres)
# class ResultComparator(enum.Enum):
#     LESS = "<"
#     LESS_OR_EQUAL = "<="
#     GREATER = ">"
#     GREATER_OR_EQUAL = ">="
#     EQUAL = "="


# result_comparator_enum = SqlEnum(
#     ResultComparator,
#     name="result_comparator_enum",
#     create_type=True,  # TODO: Set to False after first migration
# )


class Panel(Base):

    __tablename__ = "panel"
    __table_args__ = (
        UniqueConstraint(
            "ingestion_id",
            "panel_code",
            "sample_id",
            name="unique_ingestion_panel_sample",
        ),
    )

    panel_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, primary_key=True, default=uuid.uuid4
    )
    ingestion_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("ingestion.ingestion_id"), nullable=False
    )
    patient_id: Mapped[str] = mapped_column(Text, nullable=False)
    panel_code: Mapped[str] = mapped_column(Text, nullable=False)
    sample_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    collection_timestamp: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )


class Test(Base):
    __tablename__ = "test"
    __table_args__ = (
        UniqueConstraint(
            "panel_id",
            "test_code",
            "analyte_type",
            name="unique_panel_test_analyte",
        ),
    )

    test_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, primary_key=True, default=uuid.uuid4
    )
    panel_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("panel.panel_id"), nullable=False
    )
    row_number: Mapped[int] = mapped_column(Integer, nullable=False)
    test_code: Mapped[str] = mapped_column(Text, nullable=False)
    test_name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    analyte_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    result_raw: Mapped[str] = mapped_column(Text, nullable=False)
    units_raw: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    result_value_num: Mapped[Optional[float]] = mapped_column(
        Numeric, nullable=True
    )
    # Change this line
    result_comparator: Mapped[Optional[ResultComparator]] = mapped_column(
        result_comparator_enum, nullable=True
    )

    ref_low_raw: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    ref_high_raw: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    flag: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
