from app.services.parser import CanonicalAnalyzerCsvParser
from app.services.validator import PanelValidation, TestValidation
from app.persistence.repositories.raw_data_repo import RawDataRepository

import uuid


class IngestionService:
    def __init__(self):
        self.raw_repo = RawDataRepository(session)  # how to pass sessions?
        self.ingestion_repo = ingestion_repo
        self.panel_repo = panel_repo
        self.test_repo = test_repo

    def process_ingestion(self, ingestion_id):
        if not self.ingestion_repo.claim_for_processing(ingestion_id):
            return  # already claimed or not in a processable state

    def get_csv_file(self, ingestion_id):
        csv_content_bytes = self.raw_repo.get_content_bytes(ingestion_id)
        return csv_content_bytes

    def parse_csv_file(self, csv_content_bytes):
        rows = CanonicalAnalyzerCsvParser().parse(csv_content_bytes)
        return rows

    def generate_panel_payload(self, rows, ingestion_id):
        all_panels_payload = []
        panel_validation = PanelValidation()
        groups, group_errors = panel_validation.determine_panels(rows)
        # Get one row from each group and get the payload for the row
        for group_rows in groups.values():
            row_number = group_rows[0][
                0
            ]  # get row number for first row of the group
            row = group_rows[0][1]  # get one (first) row from the group
            panel_csv_payload, errors = panel_validation.build_panel_payload(
                row, row_number
            )

            panel_id = uuid.uuid4()
            panel_payload_for_db = {
                "panel_id": panel_id,
                "ingestion_id": ingestion_id,
                "patient_id": panel_csv_payload["patient_id"],
                "panel_code": panel_csv_payload["panel_code"],
                "sample_id": panel_csv_payload["sample_id"],
                "collection_timestamp": panel_csv_payload[
                    "collection_timestamp"
                ],
            }
            all_panels_payload.append(panel_payload_for_db)

        return all_panels_payload

    def generate_test_payload(self):
        pass

    def insert_panel_data(self):
        pass

    def insert_test_data(self):
        pass

    def mark_ingestion_complete_or_failed(self):
        pass
