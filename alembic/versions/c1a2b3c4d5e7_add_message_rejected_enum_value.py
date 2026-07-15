"""add MESSAGE_REJECTED event type

Revision ID: c1a2b3c4d5e7
Revises: c1a2b3c4d5e6
Create Date: 2026-07-13 00:00:00.000000

Native PostgreSQL enum values must be added with ALTER TYPE ... ADD VALUE.
Autogenerate does NOT emit these. ADD VALUE cannot run inside a transaction
block, so we use Alembic's autocommit_block.
"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "c1a2b3c4d5e7"
down_revision: Union[str, Sequence[str], None] = "c1a2b3c4d5e6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    with op.get_context().autocommit_block():
        op.execute(
            "ALTER TYPE processing_event_type_enum "
            "ADD VALUE IF NOT EXISTS 'MESSAGE_REJECTED'"
        )


def downgrade() -> None:
    """Downgrade schema.

    PostgreSQL cannot drop individual enum values without recreating the type
    and rewriting every dependent column. For a demo this is not worth the risk;
    the added value is harmless if unused, so the downgrade is a no-op.
    """
    pass
