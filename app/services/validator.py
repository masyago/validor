"""
Validation logic from parsed CSV to Panel and Test model
- CSV is parsed into a list of dicts. key: column name, value: value from CSV,
empty string if not present.
Check for required fields, append any missing required field to a list, so it can add to to error message.
Panel Model - Required fields from CSV:
- patient_id. confirm that format is  "PAT-{uuid.uuid4()"
- panel_code.
- sample_id. confirm format "SAM-{uuid.uuid4()"
- collection_stamp. check that it's a datetime data type, not in the future

Test Model - Required fields from CSV:
- test_code
- result_raw. try to convert text to a number (after removing potential result_comparator), if ok - assign to result_value_num. Assign result_comparator if present

- if result_value_num present, check that the value is not negative.

"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import re
import uuid
from typing import Any, Optional



@dataclass
class RowValidationError:
    row_number: int  # start with 1
    field: str
    message: str


class PanelRowValidation:
    """
    Step 1: validate one row's panel fields and build a payload usable by Panel(**payload).
    Step 2 (later): group rows into panels and enforce consistency within each group.
    """
    # Required CSV fields to include in Panel
    PANEL_REQUIRED_FIELDS = ["patient_id", "panel_code", "collection_timestamp"]

    # Optional CSV fields to include in Panel
    PANEL_OPTIONAL_FIELDS = ["sample_id"]
    
    def _validate_prefixed_uuid(
            self,
            value: str,
            *,
            field: str,
            prefix: str, 
            row_number: int) -> RowValidationError | None:

        if not value.startswith(prefix):
            return RowValidationError(row_number=row_number,
                                       field=field,
                                       message=f"must start with {prefix}") 
        else:
            try:
                uuid.UUID(value.removeprefix(prefix))
            except ValueError:
                return RowValidationError(
                    row_number=row_number,
                    field=field,
                    message=f"id format must be '{prefix}-<uuid>'")
        return None


    def _validate_timestamp(self,
                            timestamp_string: str,
                            *,
                            field: str,
                            row_number: int) -> RowValidationError | None:
        try:
            collection_timestamp = datetime.fromisoformat(timestamp_string)
            if collection_timestamp > datetime.now():
                return RowValidationError(
                    row_number=row_number,
                    field=field,
                    message=f"cannot be in future")
            
        except ValueError:
            return RowValidationError(
                    row_number=row_number,
                    field=field,
                    message=f"must be ISO 8601 datetime")

    # Payload per each row in CSV
    def build_panel_payload(self, row, row_number):
        errors: list[RowValidationError] = []
        
        def require(field: str) -> str | None:
            val = row.get(field, "")
            if val is None or val.strip() == "":
                errors.append(RowValidationError(row_number=row_number, field=field, message="required field missing"))
                return None
            return val.strip()
    
        patient_id = require("patient_id")
        panel_code = require("panel_code")
        timestamp_raw = require("collection_timestamp")
        sample_id = row["sample_id"]

        if patient_id is None or panel_code is None or timestamp_raw is None:
            return None, errors
    
        patient_id_error = self._validate_prefixed_uuid(
            patient_id,
            field = "patient_id",
            prefix="PAT-",
            row_number=row_number)
        
        if patient_id_error:
            errors.append(patient_id_error)
        
        sample_id_error = self._validate_prefixed_uuid(
            sample_id,
            field = "sample_id",
            prefix="SAM-",
            row_number=row_number)
        
        if sample_id:
            if sample_id_error:
                errors.append(sample_id_error)
        
        timestamp_error = self._validate_timestamp(
            timestamp_raw,
            field="collection_timestamp",
            row_number=row_number)
        
        if timestamp_error:
            errors.append(timestamp_error)

        if errors:
            return None, errors
        

        



    def determine_panels(self, rows):
        self.results = set()
        for row in rows:
            for result in self.results:
                unique_panel_row = {
                    "panel_code": row["panel_code"],
                    "sample_id": row["sample_id"],
                    "timestamp": row["timestamp"],
                }
                if unique_panel_row not in self.results:
                    self.results.add(unique_panel_row)

        # check that patient_id is consistent for the panel_code+sample_id+timestamp combo
        for 

    def prepare_validation_output_per_panel_row(self):

        return {
            "patient_id": row["patient_id"],
            "panel_code": row["panel_code"],
            "sample_id": row["sample_id"],
            "collection_timestamp": row["collection_timestamp"],
            "errors": PanelRowValidation.errors
        }


class TestValidation:
    pass
