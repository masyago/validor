import pytest

from app.services.validator import PanelValidation, TestValidation
from datetime import datetime, timezone
from app.services.validator import RowValidationError
from app.services.ingestion_service import IngestionService
from typing import Any


@pytest.fixture
def ingestion_service(db_session):
    return IngestionService(db_session)


class TestIngestionServiceUnit:
    def test_errors_to_json_converts_row_validation_errors(
        self, db_session, ingestion_service
    ):
        """Three data types used in errors: RowValidationError, dict, error
        message. Data type converted into JSON dicts without errors.
        """
        errors: list[Any] = [
            RowValidationError(
                row_number=3,
                field="test_code",
                message="required field missing",
            ),
            {"row_number": 4, "field": "result", "message": "already a dict"},
            ValueError("unexpected error"),
        ]

        out = ingestion_service._errors_to_json(errors)

        assert out == [
            {
                "row_number": 3,
                "field": "test_code",
                "message": "required field missing",
            },
            {"row_number": 4, "field": "result", "message": "already a dict"},
            {"message": "unexpected error"},
        ]


class TestIngestionServiceIntegration:

    def test_process_ingestion_happy_path_data_persists(
        self, db_session, ingestion_service
    ):
        pass

    def test_process_ingestion_validation_failure_persists_nothing_and_marks_failed_validation(
        self, db_session, ingestion_service
    ):
        pass
