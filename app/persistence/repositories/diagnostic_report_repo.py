from sqlalchemy.orm import Session
from sqlalchemy import select, update
from uuid import UUID

from app.persistence.models.normalization import DiagnosticReport


class DiagnosticReportRepository:
    def __init__(self, session: Session):
        self.session = session

    # Returns one or zero rows
    def get_by_diagnostic_report_id(
        self, diagnostic_report_id: UUID
    ) -> DiagnosticReport | None:
        stmt = select(DiagnosticReport).where(
            DiagnosticReport.diagnostic_report_id == diagnostic_report_id
        )
        return self.session.scalars(stmt).one_or_none()

    # Returns zero or multiple rows. If zero rows, returns an empty list
    def get_by_ingestion_id(
        self, ingestion_id: UUID
    ) -> list[DiagnosticReport]:
        stmt = select(DiagnosticReport).where(
            DiagnosticReport.ingestion_id == ingestion_id
        )
        return list(self.session.scalars(stmt).all())

    # Returns one or zero rows
    def get_by_panel_id(self, panel_id: UUID) -> DiagnosticReport | None:
        stmt = select(DiagnosticReport).where(
            DiagnosticReport.panel_id == panel_id
        )
        return self.session.scalars(stmt).one_or_none()

    def create(self, diagnostic_report: DiagnosticReport) -> DiagnosticReport:
        self.session.add(diagnostic_report)
        self.session.flush()
        return diagnostic_report

    def update_resource_json(
        self, diagnostic_report_id: UUID, resource_json: dict | None
    ):
        stmt = (
            update(DiagnosticReport)
            .where(
                DiagnosticReport.diagnostic_report_id == diagnostic_report_id
            )
            .values(resource_json=resource_json)
            .execution_options(synchronize_session="fetch")
        )
        self.session.execute(stmt)
