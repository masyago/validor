"""add patient table and patient_id foreign keys

Revision ID: c1a2b3c4d5e1
Revises: f7ee3fee1314
Create Date: 2026-07-08 09:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "c1a2b3c4d5e1"
down_revision: Union[str, Sequence[str], None] = "f7ee3fee1314"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "patient",
        sa.Column("patient_id", sa.Text(), nullable=False),
        sa.Column("given_name", sa.Text(), nullable=False),
        sa.Column("family_name", sa.Text(), nullable=False),
        sa.Column("email", sa.Text(), nullable=True),
        sa.Column(
            "is_synthetic",
            sa.Boolean(),
            server_default=sa.text("true"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("patient_id"),
    )

    # Backfill a synthetic patient row for every patient_id already present on
    # panel/diagnostic_report/observation so the FKs below hold. Placeholder
    # names; is_synthetic defaults TRUE (no real PHI).
    op.execute(
        """
        INSERT INTO patient (patient_id, given_name, family_name, is_synthetic)
        SELECT DISTINCT patient_id, 'Synthetic', patient_id, TRUE
        FROM (
            SELECT patient_id FROM panel
            UNION
            SELECT patient_id FROM diagnostic_report
            UNION
            SELECT patient_id FROM observation
        ) s
        ON CONFLICT (patient_id) DO NOTHING
        """
    )

    op.create_foreign_key(
        "fk_panel_patient_id_patient",
        "panel",
        "patient",
        ["patient_id"],
        ["patient_id"],
    )
    op.create_foreign_key(
        "fk_diagnostic_report_patient_id_patient",
        "diagnostic_report",
        "patient",
        ["patient_id"],
        ["patient_id"],
    )
    op.create_foreign_key(
        "fk_observation_patient_id_patient",
        "observation",
        "patient",
        ["patient_id"],
        ["patient_id"],
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint(
        "fk_observation_patient_id_patient", "observation", type_="foreignkey"
    )
    op.drop_constraint(
        "fk_diagnostic_report_patient_id_patient",
        "diagnostic_report",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_panel_patient_id_patient", "panel", type_="foreignkey"
    )
    op.drop_table("patient")
