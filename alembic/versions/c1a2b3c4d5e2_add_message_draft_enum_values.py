"""add MESSAGE_DRAFT_* event types, MESSAGE_DRAFTER actor, PATIENT_MESSAGE target

Revision ID: c1a2b3c4d5e2
Revises: c1a2b3c4d5e1
Create Date: 2026-07-08 09:01:00.000000

Native PostgreSQL enum values must be added with ALTER TYPE ... ADD VALUE.
Autogenerate does NOT emit these. ADD VALUE cannot run inside a transaction
block, so we use Alembic's autocommit_block. Keeping this isolated from the
table-create migration (c1a2b3c4d5e5) means the new values are committed before
any later migration references them.
"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "c1a2b3c4d5e2"
down_revision: Union[str, Sequence[str], None] = "c1a2b3c4d5e1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_EVENT_TYPE_VALUES = (
    "MESSAGE_DRAFT_STARTED",
    "MESSAGE_DRAFT_SKIPPED",
    "MESSAGE_DRAFT_SUCCEEDED",
    "MESSAGE_DRAFT_FAILED",
    "MESSAGE_SENT",
)


def upgrade() -> None:
    """Upgrade schema."""
    with op.get_context().autocommit_block():
        for value in _EVENT_TYPE_VALUES:
            op.execute(
                "ALTER TYPE processing_event_type_enum "
                f"ADD VALUE IF NOT EXISTS '{value}'"
            )
        op.execute(
            "ALTER TYPE processing_event_actor_enum "
            "ADD VALUE IF NOT EXISTS 'message-drafter'"
        )
        op.execute(
            "ALTER TYPE processing_event_target_type_enum "
            "ADD VALUE IF NOT EXISTS 'patient_message'"
        )


def downgrade() -> None:
    """Downgrade schema.

    PostgreSQL cannot drop individual enum values without recreating the type
    and rewriting every dependent column. For a demo this is not worth the risk;
    the added values are harmless if unused, so the downgrade is a no-op.
    """
    pass
