from app.services.parser import CanonicalAnalyzerCsvParser
from app.services.validator import (
    PanelValidation,
    TestValidation,
    RowValidationError,
)
from app.persistence.repositories.raw_data_repo import RawDataRepository
from app.persistence.repositories.ingestion_repo import IngestionRepository
from app.persistence.repositories.panel_repo import PanelRepository
from app.persistence.repositories.test_repo import TestRepository
from app.persistence.models.parsing import Panel, Test

import uuid
from dataclasses import dataclass
from typing import Any, Optional

PanelPayload = dict[str, Any]
TestPayload = dict[str, Any]


@dataclass(frozen=True)
class PanelPackage:
    """
    Validated persistence unit: one Panel row and N associated Test rows
    """

    panel_payload: PanelPayload
    test_payloads: list[TestPayload]
    group_key: Optional[tuple[str, str | None, Any]] = (
        None  # (panel_code, sample_id, collection_timestamp)
    )


class IngestionService:
    def __init__(self, session):  # sessions received from API
        self.raw_repo = RawDataRepository(session)
        self.ingestion_repo = IngestionRepository(session)
        self.panel_repo = PanelRepository(session)
        self.test_repo = TestRepository(session)

    def _errors_to_json(self, errors: list[Any]) -> list[dict[str, Any]]:
        """
        Convert RowValidationError (and similar) objects into JSON-safe dicts.
        """
        out: list[dict[str, Any]] = []
        for e in errors:
            if isinstance(e, RowValidationError):
                out.append(
                    {
                        "row_number": e.row_number,
                        "field": e.field,
                        "message": e.message,
                    }
                )
            elif isinstance(e, dict):
                out.append(e)
            else:
                out.append({"message": str(e)})
        return out

    def get_csv_file(self, ingestion_id):
        csv_content_bytes = self.raw_repo.get_content_bytes(ingestion_id)
        return csv_content_bytes

    def parse_csv_file(self, csv_content_bytes):
        rows = CanonicalAnalyzerCsvParser().parse(csv_content_bytes)
        return rows

    def validate_panel_tests(
        self, rows: list[dict[str, str]]
    ) -> tuple[list[PanelPackage], list[RowValidationError]]:
        """
        Returns:
        - panel_packages: validated payload for persistence (Panel and Test) excluding ingestion_id (Panel) and panel_id (Test)
        OR
        - if validation error present, returns validation_errors: list[RowValidationError]
        """
        validation_errors = []
        panel_validation = PanelValidation()
        test_validation = TestValidation()

        groups, group_errors = panel_validation.determine_panels(rows)

        # change ingestion status to FAILED VALIDATION.
        # send the errors to Ingestion model/db table
        validation_errors.extend(group_errors)

        panel_packages: list[PanelPackage] = []

        for key, group in groups.items():
            panel_payload = group["panel_payload"]
            panel_rows = group["panel_rows"]

            test_payloads: list[TestPayload] = []
            for row_number, row in panel_rows:
                test_payload, test_errors = test_validation.build_test_payload(
                    row, row_number
                )
                if test_errors:
                    validation_errors.extend(test_errors)
                    continue

                # if no errors, payload must exist
                assert test_payload is not None
                test_payloads.append(test_payload)

            panel_packages.append(
                PanelPackage(
                    panel_payload=panel_payload,
                    test_payloads=test_payloads,
                    group_key=key,
                )
            )

        return panel_packages, validation_errors

    def generate_payload_for_db(self, rows: list[dict[str, str]]):
        """
        Convenience wrapper: validates and returns only the payload needed for persistence.
        """
        panel_packages, validation_errors = self.validate_panel_tests(rows)

        if validation_errors:
            return None, validation_errors

        return panel_packages, []

    def insert_panel_test_data(
        self,
        ingestion_id,
        panel_packages: list[PanelPackage] | None,
        validation_errors: list[Any],
    ) -> bool:
        """
        If validation errors exist, persist no Panel and Tests.
        """
        if validation_errors:
            error_code = "validation_error"
            error_detail = {
                "validation_errors": self._errors_to_json(validation_errors)
            }

            self.ingestion_repo.mark_failed_validation(
                ingestion_id=ingestion_id,
                error_code=error_code,
                error_detail=error_detail,
            )
            return False

        if panel_packages is not None:
            for panel_package in panel_packages:
                panel = Panel(
                    ingestion_id=ingestion_id, **panel_package.panel_payload
                )
                panel = self.panel_repo.create(panel)

                for test_payload in panel_package.test_payloads:
                    test = Test(panel_id=panel.panel_id, **test_payload)
                    self.test_repo.create(test)

        return True

    def process_ingestion(self, ingestion_id):
        if not self.ingestion_repo.claim_for_processing(ingestion_id):
            return  # already claimed or not in a processable state

        try:
            # TODO(provenance): log event "csv_fetch_started"
            csv_content = self.get_csv_file(ingestion_id)
            # TODO(provenance): log event "csv_fetch_succeeded"

            # TODO(provenance): log event "csv_parse_started"
            csv_rows = self.parse_csv_file(csv_content)
            # TODO(provenance): log event "csv_parse_succeeded" (include row count)

            # TODO(provenance): log event "validation_started"
            panel_packages, validation_errors = self.generate_payload_for_db(
                csv_rows
            )
            # TODO(provenance): log event "validation_finished" (include error count)

            ok = self.insert_panel_test_data(
                ingestion_id, panel_packages, validation_errors
            )
            if not ok:
                # status is now FAILED_VALIDATION; nothing else to do in this pipeline run
                # TODO(provenance): log event "persistence_skipped_validation_failed"
                return

            # Next pipeline steps:
            # TODO(provenance): log event "normalization_started"
            # TODO: normalize to FHIR
            # TODO(provenance): log event "normalization_finished"
            # TODO(provenance): log event "ai_augmentation_started"
            # TODO: AI augmentation
            # TODO(provenance): log event "ai_augmentation_finished"
            # Eventually:
            # TODO: self.ingestion_repo.mark_completed(ingestion_id)

            return

        except Exception:
            # Optional: if you add ingestion_repo.mark_failed(...), call it here.
            # Keep re-raising so ingestion_tasks.py rolls back.
            raise
