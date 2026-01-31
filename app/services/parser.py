"""
Parses data from CSV into a list of dictionaries, from bytes to rows.
No domain logic.
"""

import csv
import io


class CanonicalAnalyzerCsvParser:
    def parse(self, content_bytes: bytes) -> list[dict[str, str]]:
        data = content_bytes.decode("utf-8-sig")
        f = io.StringIO(data)
        # Each CSV row is a dict where key is a column name
        reader = csv.DictReader(f)

        rows: list[dict[str, str]] = []
        for raw_row in reader:
            # DictReader values should already be strings, but normalize anyway
            normalized = {
                str(k): (
                    v.strip()
                    if isinstance(v, str)
                    else ("" if v is None else str(v))
                )
                for k, v in raw_row.items()
                if k is not None
            }
            rows.append(normalized)

        return rows
