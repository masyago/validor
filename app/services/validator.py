"""
Validation logic from parsed CSV to Panel and Test model
- CSV is parsed into a list of dicts. key: column name, value: value from CSV,
empty string if not present.
Check for required fields, append any missing required field to a list, so it can add to to error message.
Panel Model - Required fields from CSV:
- patient_id. confirm that format is  "PAT-{uuid.uuid4()"
- panel_code.
- sample_id. confirm format "SAM-{uuid.uuid4()"
- collection_timestamp. check that it's a datetime data type, not in the future

Test Model - Required fields from CSV:
- test_code
- result. try to convert text to a number (after removing potential result_comparator), if ok - assign to result_value_num. Assign result_comparator if present

- if result_value_num present, check that the value is not negative.

"""

from __future__ import annotations
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
import re
import uuid
from typing import Any


@dataclass
class RowValidationError:
    row_number: int  # start with 1
    field: str
    message: str


class PanelValidation:
    # Required CSV fields to include in Panel
    PANEL_REQUIRED_FIELDS = [
        "patient_id",
        "panel_code",
        "collection_timestamp",
    ]

    # Optional CSV fields to include in Panel
    PANEL_OPTIONAL_FIELDS = ["sample_id"]

    def _validate_prefixed_uuid(
        self, value: str, *, field: str, prefix: str, row_number: int
    ) -> RowValidationError | None:

        if not value.startswith(prefix):
            return RowValidationError(
                row_number=row_number,
                field=field,
                message=f"must start with {prefix}",
            )
        else:
            try:
                uuid.UUID(value.removeprefix(prefix))
            except ValueError:
                return RowValidationError(
                    row_number=row_number,
                    field=field,
                    message=f"id format must be '{prefix}<uuid>'",
                )
        return None

    def _validate_timestamp(
        self, timestamp_string: str, *, field: str, row_number: int
    ) -> RowValidationError | None:
        try:
            collection_timestamp = datetime.fromisoformat(timestamp_string)

            # Guardrail for a naive timestamp
            if collection_timestamp.tzinfo is None:
                collection_timestamp = collection_timestamp.replace(
                    tzinfo=timezone.utc
                )

            if collection_timestamp > datetime.now(timezone.utc):
                return RowValidationError(
                    row_number=row_number,
                    field=field,
                    message=f"cannot be in future",
                )

        except ValueError:
            return RowValidationError(
                row_number=row_number,
                field=field,
                message=f"must be ISO 8601 datetime",
            )
        return None

    # Payload per each row in CSV
    def build_panel_payload(self, row: dict[str, str], row_number: int):
        errors: list[RowValidationError] = []

        def require(field: str) -> str | None:
            val = row.get(field, "")
            if val is None or val.strip() == "":
                errors.append(
                    RowValidationError(
                        row_number=row_number,
                        field=field,
                        message="required field missing",
                    )
                )
                return None
            return val.strip()

        def optional(field: str) -> str | None:
            val = row.get(field, "")
            if val is None:
                return None
            s = val.strip()
            return s if s != "" else None

        patient_id = require("patient_id")
        panel_code = require("panel_code")
        timestamp_raw = require("collection_timestamp")
        sample_id = optional("sample_id")

        if patient_id is None or panel_code is None or timestamp_raw is None:
            return None, errors

        patient_id_error = self._validate_prefixed_uuid(
            patient_id,
            field="patient_id",
            prefix="PAT-",
            row_number=row_number,
        )

        if patient_id_error:
            errors.append(patient_id_error)

        if sample_id is not None:
            sample_id_error = self._validate_prefixed_uuid(
                sample_id,
                field="sample_id",
                prefix="SAM-",
                row_number=row_number,
            )

            if sample_id_error:
                errors.append(sample_id_error)

        timestamp_error = self._validate_timestamp(
            timestamp_raw, field="collection_timestamp", row_number=row_number
        )

        if timestamp_error:
            errors.append(timestamp_error)

        if errors:
            return None, errors

        payload = {
            "patient_id": patient_id,
            "panel_code": panel_code,
            "sample_id": sample_id,
            "collection_timestamp": datetime.fromisoformat(timestamp_raw),
        }

        # Normalize naive timestamps to UTC in the payload
        ts = payload["collection_timestamp"]
        if isinstance(ts, datetime) and ts.tzinfo is None:
            payload["collection_timestamp"] = ts.replace(tzinfo=timezone.utc)

        return payload, errors

    def determine_panels(self, rows: list[dict[str, str]]) -> tuple[
        dict[tuple[str, str | None, datetime], dict[str, Any]],
        list[RowValidationError],
    ]:

        errors: list[RowValidationError] = []

        """
        Groups dict structure:
         - key is a tuple (panel_code, sample_id, collection_timestamp)
         - values is a dict:
            - "panel_payload": {payload here}
            - "panel_rows": [(row_num, row)... list of rows from CSV]
        """
        groups: dict[tuple[str, str | None, datetime], dict[str, Any]] = (
            defaultdict(lambda: {"panel_payload": None, "panel_rows": []})
        )

        # Track patient_id per group to enforce consistency
        patient_id_by_key: dict[tuple[str, str | None, datetime], str] = {}

        for row_number, row in enumerate(rows, start=1):
            payload, row_errors = self.build_panel_payload(row, row_number)
            if row_errors:
                errors.extend(row_errors)
                continue
            assert payload is not None

            key = (
                payload["panel_code"],  # str
                payload["sample_id"],  # str | None
                payload["collection_timestamp"],  # datetime
            )

            # Consistency check: same patient_id within a panel group
            patient_id = payload["patient_id"]
            existing_patient_id = patient_id_by_key.get(key)
            if existing_patient_id is None:
                patient_id_by_key[key] = patient_id
            elif existing_patient_id != patient_id:
                errors.append(
                    RowValidationError(
                        row_number=row_number,
                        field="patient_id",
                        message=(
                            "patient_id must be consistent for the same "
                            "(panel_code, sample_id, collection_timestamp) group"
                        ),
                    )
                )
                continue
            if groups[key]["panel_payload"] is None:
                groups[key]["panel_payload"] = payload

            groups[key]["panel_rows"].append((row_number, row))

        return dict(groups), errors


class TestValidation:
    # For reference only:

    # RESULT_COMPARATORS: "<", "<=", ">", ">=", "="
    # REQUIRED_FIELDS: ["test_code", "result"]
    # OPTIONAL_FIELDS:
    #     "test_name",
    #     "analyte_type",
    #     "units_raw",
    #     "ref_low_raw",
    #     "ref_low_raw",
    #     "flag"

    def _parse_result_field(self, result_raw: str) -> tuple[str | None, str]:
        s = result_raw.strip()
        comparator_pattern = r"^(>=|<=|[>=<])"
        match = re.match(comparator_pattern, s)

        if match:
            result_comparator = match.group(0)
            end_index = match.end()
            remainder = s[end_index:].strip()
            return result_comparator, remainder

        return None, s

    def _parse_result_numeric(self, remainder: str) -> float | None:
        try:
            result_value_num = float(remainder)
            return result_value_num
        except ValueError:
            return None

    def build_test_payload(
        self, row: dict[str, str], row_number: int
    ) -> tuple[dict[str, Any] | None, list[RowValidationError]]:
        errors: list[RowValidationError] = []

        def require(field: str) -> str | None:
            val = row.get(field, "")
            if val is None or val.strip() == "":
                errors.append(
                    RowValidationError(
                        row_number=row_number,
                        field=field,
                        message="required field missing",
                    )
                )
                return None
            return val.strip()

        def optional(field: str) -> str | None:
            val = row.get(field, "")
            if val is None:
                return None
            s = val.strip()
            return s if s != "" else None

        test_code = require("test_code")
        result_raw = require("result")

        if result_raw is not None:
            result_comparator, remainder = self._parse_result_field(result_raw)
            result_value_num = self._parse_result_numeric(remainder)

            if result_value_num is not None and result_value_num < 0:
                errors.append(
                    RowValidationError(
                        row_number=row_number,
                        field="result",
                        message="numeric result cannot be negative",
                    )
                )

        if errors:
            return None, errors

        payload = {
            "row_number": row_number,
            "test_code": test_code,
            "test_name": optional("test_name"),
            "analyte_type": optional("analyte_type"),
            "result_raw": result_raw,
            "units_raw": optional("units"),
            "result_value_num": result_value_num,
            "result_comparator": result_comparator,
            "ref_low_raw": optional("reference_range_low"),
            "ref_high_raw": optional("reference_range_high"),
            "flag": optional("flag"),
        }

        return payload, []
