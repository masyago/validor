"""
Docstring for app.services.parser
Parses data from CSV and prepares it for database.
"""

import csv
import io

# from app.persistence.repositories.raw_data_repo import RawDataRepository

# raw_data_repo = RawDataRepository()

# csv_bytes = raw_repo.get_content_bytes(ingestion_id)


class CanonicalAnalyzerCsvParser:
    def parse(self, content_bytes: bytes) -> list[dict]:
        data = csv_bytes.decode("utf-8")
        f = io.StringIO(data)
        reader = csv.DictReader(
            f
        )  # each row is a dict where key is a column name
        # for row in reader:
        #     print(row)
        return list(reader)


csv_bytes = b"name,age\nAlice,\nBob,25"
print(CanonicalAnalyzerCsvParser().parse(csv_bytes))
