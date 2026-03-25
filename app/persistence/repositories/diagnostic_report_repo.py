from typing import Any

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

    def upsert_many_from_payloads(
        self, params: list[dict[str, Any]]
    ) -> tuple[dict[UUID, UUID], int]:
        """
        Bulk Postgres insert-first idempotent write keyed by unique(panel_id).

        Returns: (diagnostic_report_id, inserted)
        """
        if not params:
            return {}, 0

        # INSERT many (executemany) and return ids for newly inserted rows.
        insert_stmt = (
            insert(DiagnosticReport)
            .values(params)
            .on_conflict_do_nothing(
                index_elements=[DiagnosticReport.panel_id],
            )
            .returning(
                DiagnosticReport.panel_id,
                DiagnosticReport.diagnostic_report_id,
            )
        )
        inserted_rows = list(self.session.execute(insert_stmt).all())
        inserted_by_panel_id: dict[UUID, UUID] = {
            row[0]: row[1] for row in inserted_rows
        }
        inserted_count = len(inserted_by_panel_id)

        # Fetch ids for conflict rows in ONE query.
        requested_panel_ids: list[UUID] = [p["panel_id"] for p in params]
        missing_panel_ids = [
            pid
            for pid in requested_panel_ids
            if pid not in inserted_by_panel_id
        ]

        # If all rows requested were inserted
        if not missing_panel_ids:
            return inserted_by_panel_id, inserted_count

        # If some rows with requested panel_id already existed
        # Fetch DR rows that already existed per requested panel_id
        existing_rows = list(
            self.session.execute(
                select(
                    DiagnosticReport.panel_id,
                    DiagnosticReport.diagnostic_report_id,
                ).where(DiagnosticReport.panel_id.in_(missing_panel_ids))
            ).all()
        )

        existing_by_panel_id: dict[UUID, UUID] = {
            row[0]: row[1] for row in existing_rows
        }

        by_panel_id = dict(inserted_by_panel_id)
        by_panel_id.update(existing_by_panel_id)

        # Ensure all requested panel_id's were resolved
        unresolved = [
            pid for pid in requested_panel_ids if pid not in by_panel_id
        ]
        if unresolved:
            raise RuntimeError(
                "DiagnosticReport bulk upsert failed to resolve diagnostic_report_id for panel_id(s): "
                + ", ".join(str(p) for p in unresolved[:20])
            )

        return by_panel_id, inserted_count

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
