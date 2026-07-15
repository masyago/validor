from __future__ import annotations

from typing import Optional

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from app.persistence.models.patient import Patient


class PatientRepository:
    def __init__(self, session: Session):
        self.session = session

    def get(self, patient_id: str) -> Patient | None:
        stmt = select(Patient).where(Patient.patient_id == patient_id)
        return self.session.scalars(stmt).one_or_none()

    def upsert(
        self,
        *,
        patient_id: str,
        given_name: str,
        family_name: str,
        email: Optional[str] = None,
        is_synthetic: bool = True,
    ) -> Patient:
        """
        Insert a patient, or refresh the synthetic demographics if the row
        already exists. Idempotent, and updating on conflict lets a re-ingestion
        heal placeholder rows left by the backfill migration (given_name
        'Synthetic', family_name = patient_id).
        """
        insert_stmt = pg_insert(Patient).values(
            patient_id=patient_id,
            given_name=given_name,
            family_name=family_name,
            email=email,
            is_synthetic=is_synthetic,
        )
        stmt = insert_stmt.on_conflict_do_update(
            index_elements=["patient_id"],
            set_={
                "given_name": insert_stmt.excluded.given_name,
                "family_name": insert_stmt.excluded.family_name,
                "email": insert_stmt.excluded.email,
                "is_synthetic": insert_stmt.excluded.is_synthetic,
            },
        )
        self.session.execute(stmt)
        self.session.flush()
        return self.get(patient_id)
