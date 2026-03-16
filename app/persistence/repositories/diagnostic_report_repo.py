from sqlalchemy.orm import Session
from sqlalchemy import select, update, desc, asc
from uuid import UUID
from sqlalchemy.dialects.postgresql import insert

from app.persistence.models.normalization import DiagnosticReport
from app.schemas.identifiers import PatientId


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
        stmt = (
            select(DiagnosticReport)
            .where(DiagnosticReport.ingestion_id == ingestion_id)
            .order_by(asc(DiagnosticReport.diagnostic_report_id))
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

    def upsert_from_payload(self, payload: dict) -> tuple[UUID, bool]:
        """
        Postgres insert-first idempotent write keyed by unique(panel_id).

        Returns: (diagnostic_report_id, inserted)
        """
        insert_stmt = (
            insert(DiagnosticReport)
            .values(**payload)
            .on_conflict_do_nothing(
                index_elements=[DiagnosticReport.panel_id],
            )
            .returning(DiagnosticReport.diagnostic_report_id)
        )

        inserted_id = self.session.execute(insert_stmt).scalar_one_or_none()
        if inserted_id is not None:
            return inserted_id, True

        existing_id = self.session.execute(
            select(DiagnosticReport.diagnostic_report_id).where(
                DiagnosticReport.panel_id == payload["panel_id"]
            )
        ).scalar_one_or_none()

        if existing_id is None:
            raise RuntimeError(
                f"DiagnosticReport upsert failed to fetch existing row for panel_id={payload.get('panel_id')}"
            )

        return existing_id, False

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

    def get_by_patient_id(
        self, patient_id: PatientId
    ) -> list[DiagnosticReport]:
        """
        Returns zero or multiple rows. If zero rows, returns an empty list.
        Results are ordered by (1) effective_at datetime in descending order (new first). If datetime the same, they're additionally ordered by diagnostic_report_id (to preserve order of results).
        """
        stmt = (
            select(DiagnosticReport)
            .where(DiagnosticReport.patient_id == patient_id)
            .order_by(
                desc(DiagnosticReport.effective_at),
                asc(DiagnosticReport.diagnostic_report_id),
            )
        )
        return list(self.session.scalars(stmt).all())
