from sqlalchemy.orm import Session
from sqlalchemy import select, update, desc, asc, bindparam
from uuid import UUID
from sqlalchemy.dialects.postgresql import insert
from typing import Any

from app.persistence.models.normalization import Observation
from app.schemas.identifiers import PatientId

"""
Methods:
- retrieve a row by observation_id (one or zero)
- retrieve a row by diagnostic_report_id (zero or more)
- retrieve a row by test_id (one or zero)
- retrieve rows by ingestion_id (zero or more)
- add a new row
"""


class ObservationRepository:
    def __init__(self, session: Session):
        self.session = session

    # Returns one or zero rows
    def get_by_observation_id(
        self, observation_id: UUID
    ) -> Observation | None:
        stmt = select(Observation).where(
            Observation.observation_id == observation_id
        )
        return self.session.scalars(stmt).one_or_none()

    # Returns zero or multiple rows. If zero rows, returns an empty list
    def get_by_diagnostic_report_id(
        self, diagnostic_report_id: UUID
    ) -> list[Observation]:
        stmt = select(Observation).where(
            Observation.diagnostic_report_id == diagnostic_report_id
        )
        return list(self.session.scalars(stmt).all())

    # Returns one or zero rows
    def get_by_test_id(self, test_id: UUID) -> Observation | None:
        stmt = select(Observation).where(Observation.test_id == test_id)
        return self.session.scalars(stmt).one_or_none()

    def get_by_test_id_list(self, test_ids: list[UUID]) -> list[Observation]:
        if not test_ids:
            return []
        stmt = select(Observation).where(Observation.test_id.in_(test_ids))
        return list(self.session.scalars(stmt).all())

    # Returns zero or multiple rows. If zero rows, returns an empty list
    def get_by_ingestion_id(self, ingestion_id: UUID) -> list[Observation]:
        stmt = (
            select(Observation)
            .where(Observation.ingestion_id == ingestion_id)
            .order_by(asc(Observation.observation_id))
        )
        return list(self.session.scalars(stmt).all())

    def create(self, observation: Observation) -> Observation:
        self.session.add(observation)
        self.session.flush()
        return observation

    def upsert_from_payload(self, payload: dict) -> tuple[UUID, bool]:
        """
        Postgres insert-first idempotent write keyed by unique(test_id).

        Returns: (observation_id, inserted)
        - inserted=True if a new row was inserted
        - inserted=False if the row already existed

        NOTE: Does NOT overwrite resource_json (Phase 2 owns it).
        """
        insert_stmt = (
            insert(Observation)
            .values(**payload)
            .on_conflict_do_nothing(
                index_elements=[Observation.test_id],
            )
            .returning(Observation.observation_id)
        )

        inserted_id = self.session.execute(insert_stmt).scalar_one_or_none()
        if inserted_id is not None:
            return inserted_id, True

        # In inserted_id in None - Conflict path: row already exists, fetch its id
        existing_id = self.session.execute(
            select(Observation.observation_id).where(
                Observation.test_id == payload["test_id"]
            )
        ).scalar_one_or_none()

        if existing_id is None:
            # Extremely unlikely unless the row was deleted between statements.
            raise RuntimeError(
                f"Observation upsert failed to fetch existing row for test_id={payload.get('test_id')}"
            )

        return existing_id, False

    def upsert_many_from_payload(
        self, params: list[dict[str, Any]]
    ) -> tuple[dict[UUID, UUID], int]:
        """
        Bulk insert-first idempotent write keyed by unique(test_id).

        Returns:
        - by_test_id: mapping of test_id -> observation_id for ALL input rows
          (both newly inserted and pre-existing)
        - inserted_count: number of rows newly inserted in this call

        NOTE: Does NOT overwrite resource_json (Phase 2 owns it).
        """

        if not params:
            return {}, 0

        # INSERT many (executemany) and return ids for newly inserted rows.
        insert_stmt = (
            insert(Observation)
            .values(params)
            .on_conflict_do_nothing(index_elements=[Observation.test_id])
            .returning(Observation.test_id, Observation.observation_id)
        )

        inserted_rows = list(self.session.execute(insert_stmt).all())
        inserted_by_test_id: dict[UUID, UUID] = {
            row[0]: row[1] for row in inserted_rows
        }
        inserted_count = len(inserted_by_test_id)

        # Fetch ids for conflict rows in ONE query.
        requested_test_ids: list[UUID] = [p["test_id"] for p in params]
        missing_test_ids = [
            tid for tid in requested_test_ids if tid not in inserted_by_test_id
        ]
        if not missing_test_ids:
            return inserted_by_test_id, inserted_count

        existing_rows = list(
            self.session.execute(
                select(Observation.test_id, Observation.observation_id).where(
                    Observation.test_id.in_(missing_test_ids)
                )
            ).all()
        )
        existing_by_test_id: dict[UUID, UUID] = {
            row[0]: row[1] for row in existing_rows
        }

        by_test_id = dict(inserted_by_test_id)
        by_test_id.update(existing_by_test_id)

        # Safety: ensure all requested test_ids were resolved.
        unresolved = [tid for tid in requested_test_ids if tid not in by_test_id]
        if unresolved:
            raise RuntimeError(
                "Observation bulk upsert failed to resolve observation_id for test_id(s): "
                + ", ".join(str(t) for t in unresolved[:20])
            )

        return by_test_id, inserted_count

    def update_resource_json(
        self, observation_id: UUID, resource_json: dict | None
    ):
        stmt = (
            update(Observation)
            .where(Observation.observation_id == observation_id)
            .values(resource_json=resource_json)
            .execution_options(synchronize_session="fetch")
        )
        self.session.execute(stmt)

    def update_many_resource_json(self, params: list[dict[str, Any]]):
        stmt = (
            update(Observation)
            .where(Observation.observation_id == bindparam("observation_id"))
            .values(resource_json=bindparam("resource_json"))
            .execution_options(synchronize_session=False)
        )
        self.session.execute(stmt, params)

    def get_by_patient_id(self, patient_id: PatientId) -> list[Observation]:
        """
        Returns zero or multiple rows. If zero rows, returns an empty list.
        Results are ordered by (1) effective_at datetime in descending order (new first). If datetime is the same, results are additionally ordered by observation_id to have reproducible order of the results.
        """
        stmt = (
            select(Observation)
            .where(Observation.patient_id == patient_id)
            .order_by(
                desc(Observation.effective_at),
                asc(Observation.observation_id),
            )
        )
        return list(self.session.scalars(stmt).all())
