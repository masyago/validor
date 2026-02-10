from sqlalchemy.orm import Session
from sqlalchemy import select
from uuid import UUID

from app.persistence.models.normalization import Observation

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

    # Returns zero or multiple rows. If zero rows, returns an empty list
    def get_by_ingestion_id(self, ingestion_id: UUID) -> list[Observation]:
        stmt = select(Observation).where(
            Observation.ingestion_id == ingestion_id
        )
        return list(self.session.scalars(stmt).all())

    def create(self, observation: Observation) -> Observation:
        self.session.add(observation)
        self.session.flush()
        return observation
