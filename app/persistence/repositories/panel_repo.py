from sqlalchemy.orm import Session
from sqlalchemy import select
from uuid import UUID

from app.persistence.models.parsing import Panel
from app.schemas.identifiers import PatientId


class PanelRepository:
    def __init__(self, session: Session):
        self.session = session

    def get_by_panel_id(self, panel_id: UUID) -> Panel | None:
        stmt = select(Panel).where(Panel.panel_id == panel_id)
        return self.session.scalars(stmt).one_or_none()

    # Returns zero or multiple rows. If zero rows, returns an empty list
    def get_by_ingestion_id(self, ingestion_id: UUID) -> list[Panel]:
        stmt = select(Panel).where(Panel.ingestion_id == ingestion_id)
        return list(self.session.scalars(stmt).all())

    # Returns zero or multiple rows. If zero rows, returns an empty list
    def get_by_patient_id(self, patient_id: PatientId) -> list[Panel]:
        stmt = select(Panel).where(Panel.patient_id == patient_id)
        return list(self.session.scalars(stmt).all())

    def create(self, panel: Panel) -> Panel:
        self.session.add(panel)
        self.session.flush()
        return panel
