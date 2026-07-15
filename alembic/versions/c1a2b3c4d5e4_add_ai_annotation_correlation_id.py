"""add correlation_id to ai_annotation

Revision ID: c1a2b3c4d5e4
Revises: c1a2b3c4d5e3
Create Date: 2026-07-08 09:03:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "c1a2b3c4d5e4"
down_revision: Union[str, Sequence[str], None] = "c1a2b3c4d5e3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "ai_annotation",
        sa.Column("correlation_id", sa.Uuid(), nullable=True),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("ai_annotation", "correlation_id")
