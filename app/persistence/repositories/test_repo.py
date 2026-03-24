"""
Methods to add:
- add a new row
- retrieve a row by test_id
- retrieve rows (zero or more) by panel_id
"""

from sqlalchemy.orm import Session
from sqlalchemy import select
from uuid import UUID

from app.persistence.models.parsing import Test


class TestRepository:
    def __init__(self, session: Session):
        self.session = session

    def get_by_test_id(self, test_id: UUID) -> Test | None:
        stmt = select(Test).where(Test.test_id == test_id)
        return self.session.scalars(stmt).one_or_none()

    def get_by_panel_id(self, panel_id: UUID) -> list[Test]:
        stmt = (
            select(Test)
            .where(Test.panel_id == panel_id)
            .order_by(Test.test_id)
        )
        return list(self.session.scalars(stmt).all())

    def get_by_panel_ids(self, panel_ids: list[UUID]) -> list[Test]:
        if not panel_ids:
            return []

        stmt = (
            select(Test)
            .where(Test.panel_id.in_(panel_ids))
            .order_by(Test.panel_id, Test.test_id)
        )
        return list(self.session.scalars(stmt).all())

    def create(self, test: Test) -> Test:
        self.session.add(test)
        self.session.flush()
        return test
