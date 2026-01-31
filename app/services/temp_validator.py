"""
Validation logic from parsed CSV to Panel and Test model.

CSV schema (from csv_uploader/csv_generator.py):
- run_id
- sample_id
- patient_id
- panel_code
- test_code
- test_name
- analyte_type
- result
- units
- reference_range_low
- reference_range_high
- flag
- collection_timestamp
- instrument_id

Notes:
- One CSV may contain multiple panels. Rows are grouped into panels by:
  (patient_id, sample_id, panel_code, collection_timestamp)
- collection_timestamp produced by the generator is timezone-naive; we normalize
  naive timestamps to UTC to satisfy TIMESTAMP(timezone=True).

We output constructor-ready kwargs for:
- Panel(**panel_kwargs)  (excluding ingestion_id)
- Test(**test_kwargs)    (excluding x)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import re
import uuid
from typing import Any, Optional


_ALLOWED_COMPARATORS = {"<", "<=", ">", ">=", "="}
_COMPARATOR_RE = re.compile(r"^\s*(<=|>=|<|>|=)\s*(.+?)\s*$")


@dataclass(frozen=True)
class RowValidationError:
    row: Optional[
        int
    ]  # 1-based CSV data row index (excluding header), if known
    field: str
    message: str


@dataclass(frozen=True)
class PanelBatch:
    panel_kwargs: dict[str, Any]
    tests_kwargs: list[dict[str, Any]]
    row_indices: list[int]


@dataclass(frozen=True)
class MultiValidationResult:
    ok: bool
    panels: list[PanelBatch]
    errors: list[RowValidationError]


class PanelValidation:
    @staticmethod
    def _get_required_str(
        row: dict[str, Any],
        key: str,
        *,
        errors: list[RowValidationError],
        row_index: int,
    ) -> Optional[str]:
        raw = row.get(key, "")
        val = raw.strip() if isinstance(raw, str) else raw
        if not val:
            errors.append(
                RowValidationError(
                    row=row_index, field=key, message="field required"
                )
            )
            return None
        if not isinstance(val, str):
            errors.append(
                RowValidationError(
                    row=row_index, field=key, message="must be a string"
                )
            )
            return None
        return val

    @staticmethod
    def _validate_prefixed_uuid(
        value: str,
        *,
        prefix: str,
        field: str,
        errors: list[RowValidationError],
        row_index: int,
    ) -> None:
        if not value.startswith(prefix):
            errors.append(
                RowValidationError(
                    row=row_index,
                    field=field,
                    message=f"must start with '{prefix}'",
                )
            )
            return
        try:
            uuid.UUID(value[len(prefix) :])
        except Exception:
            errors.append(
                RowValidationError(
                    row=row_index,
                    field=field,
                    message=f"must be '{prefix}<uuid>'",
                )
            )

    @staticmethod
    def _parse_collection_timestamp(
        row: dict[str, Any],
        *,
        errors: list[RowValidationError],
        row_index: int,
        now: datetime,
    ) -> Optional[datetime]:
        raw = row.get("collection_timestamp", "")
        val = raw.strip() if isinstance(raw, str) else raw
        if not val:
            errors.append(
                RowValidationError(
                    row=row_index,
                    field="collection_timestamp",
                    message="field required",
                )
            )
            return None
        if not isinstance(val, str):
            errors.append(
                RowValidationError(
                    row=row_index,
                    field="collection_timestamp",
                    message="must be ISO 8601 string",
                )
            )
            return None

        try:
            dt = datetime.fromisoformat(val)
        except ValueError:
            errors.append(
                RowValidationError(
                    row=row_index,
                    field="collection_timestamp",
                    message="must be ISO 8601 datetime (e.g. 2026-01-28T12:49:18.500606+00:00)",
                )
            )
            return None

        # Generator produces naive timestamps; normalize to UTC.
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        if dt > now:
            errors.append(
                RowValidationError(
                    row=row_index,
                    field="collection_timestamp",
                    message="cannot be in the future",
                )
            )
            return None

        return dt

    @classmethod
    def validate_row_panel_fields(
        cls,
        row: dict[str, Any],
        *,
        errors: list[RowValidationError],
        row_index: int,
        now: datetime,
    ) -> Optional[dict[str, Any]]:
        patient_id = cls._get_required_str(
            row, "patient_id", errors=errors, row_index=row_index
        )
        sample_id = cls._get_required_str(
            row, "sample_id", errors=errors, row_index=row_index
        )
        panel_code = cls._get_required_str(
            row, "panel_code", errors=errors, row_index=row_index
        )
        collection_timestamp = cls._parse_collection_timestamp(
            row, errors=errors, row_index=row_index, now=now
        )

        if patient_id:
            cls._validate_prefixed_uuid(
                patient_id,
                prefix="PAT-",
                field="patient_id",
                errors=errors,
                row_index=row_index,
            )
        if sample_id:
            cls._validate_prefixed_uuid(
                sample_id,
                prefix="SAM-",
                field="sample_id",
                errors=errors,
                row_index=row_index,
            )

        if not (
            patient_id and sample_id and panel_code and collection_timestamp
        ):
            return None

        return {
            "patient_id": patient_id,
            "panel_code": panel_code,
            "sample_id": sample_id,
            "collection_timestamp": collection_timestamp,
        }

    @staticmethod
    def panel_group_key(
        panel_kwargs: dict[str, Any],
    ) -> tuple[str, str, str, str]:
        dt: datetime = panel_kwargs["collection_timestamp"]
        return (
            panel_kwargs["patient_id"],
            panel_kwargs["sample_id"],
            panel_kwargs["panel_code"],
            dt.isoformat(),
        )


class TestValidation:
    @staticmethod
    def _get_optional_str(row: dict[str, Any], key: str) -> Optional[str]:
        raw = row.get(key, None)
        if raw is None:
            return None
        if isinstance(raw, str):
            s = raw.strip()
            return s if s != "" else None
        # If generator ever emits non-strings, coerce to str for raw fields
        return str(raw)

    @staticmethod
    def _parse_result(
        raw_value: Any,
        *,
        errors: list[RowValidationError],
        row_index: int,
    ) -> tuple[str, Optional[str], Optional[float]]:
        """
        Input comes from CSV column `result`.

        Returns (result_raw, result_comparator, result_value_num)
        - result_raw: always stored as text
        - comparator: one of <,<=,>,>=,= or None
        - value_num: float if parseable else None
        """
        if raw_value is None:
            errors.append(
                RowValidationError(
                    row=row_index, field="result", message="field required"
                )
            )
            return "", None, None

        # CSV DictReader yields strings, but keep this robust.
        if isinstance(raw_value, (int, float, Decimal)):
            result_raw = str(raw_value)
        elif isinstance(raw_value, str):
            result_raw = raw_value.strip()
        else:
            result_raw = str(raw_value).strip()

        if result_raw == "":
            errors.append(
                RowValidationError(
                    row=row_index, field="result", message="field required"
                )
            )
            return "", None, None

        comparator: Optional[str] = None
        numeric_token = result_raw

        m = _COMPARATOR_RE.match(result_raw)
        if m:
            comparator = m.group(1)
            numeric_token = m.group(2).strip()
            if comparator not in _ALLOWED_COMPARATORS:
                errors.append(
                    RowValidationError(
                        row=row_index,
                        field="result_comparator",
                        message=f"invalid '{comparator}'",
                    )
                )
                comparator = None

        try:
            d = Decimal(numeric_token.replace(",", ""))
        except (InvalidOperation, ValueError):
            return (
                result_raw,
                comparator,
                None,
            )  # non-numeric is allowed; stays text-only

        try:
            value_num = float(d)
        except (OverflowError, ValueError):
            errors.append(
                RowValidationError(
                    row=row_index,
                    field="result_value_num",
                    message="numeric value out of range",
                )
            )
            return result_raw, comparator, None

        return result_raw, comparator, value_num

    @classmethod
    def validate_row_test_fields(
        cls,
        row: dict[str, Any],
        *,
        errors: list[RowValidationError],
        row_index: int,
    ) -> Optional[dict[str, Any]]:
        test_code = row.get("test_code", "")
        test_code = (
            test_code.strip() if isinstance(test_code, str) else test_code
        )
        if not test_code or not isinstance(test_code, str):
            errors.append(
                RowValidationError(
                    row=row_index, field="test_code", message="field required"
                )
            )
            return None

        result_raw, comparator, value_num = cls._parse_result(
            row.get("result", ""), errors=errors, row_index=row_index
        )

        if value_num is not None and value_num < 0:
            errors.append(
                RowValidationError(
                    row=row_index,
                    field="result_value_num",
                    message="must be non-negative",
                )
            )

        # Map CSV column names -> ORM model field names
        return {
            # row_number is required by the ORM model; we assign it later per-panel.
            "test_code": test_code,
            "test_name": cls._get_optional_str(row, "test_name"),
            "analyte_type": cls._get_optional_str(row, "analyte_type"),
            "result_raw": result_raw,
            "units_raw": cls._get_optional_str(row, "units"),
            "result_value_num": value_num,
            "result_comparator": comparator,
            "ref_low_raw": cls._get_optional_str(row, "reference_range_low"),
            "ref_high_raw": cls._get_optional_str(row, "reference_range_high"),
            "flag": cls._get_optional_str(row, "flag"),
        }


def validate_panels_and_tests(
    rows: list[dict[str, Any]],
    *,
    now: Optional[datetime] = None,
) -> MultiValidationResult:
    """
    Validates a CSV where rows may belong to different panels.

    Persistence pattern:
      for batch in result.panels:
          panel = Panel(ingestion_id=..., **batch.panel_kwargs)
          session.add(panel); session.flush()
          for t in batch.tests_kwargs:
              session.add(Test(panel_id=panel.panel_id, **t))
    """
    errors: list[RowValidationError] = []
    if now is None:
        now = datetime.now(timezone.utc)

    if not rows:
        return MultiValidationResult(
            ok=False,
            panels=[],
            errors=[
                RowValidationError(
                    row=None, field="csv", message="no data rows present"
                )
            ],
        )

    # buckets[key] = {"panel_kwargs": {...}, "rows": [(row_index, row_dict), ...]}
    buckets: dict[tuple[str, str, str, str], dict[str, Any]] = {}

    for i, row in enumerate(rows, start=1):
        panel_kwargs = PanelValidation.validate_row_panel_fields(
            row, errors=errors, row_index=i, now=now
        )

        if panel_kwargs is None:
            # Still validate test fields to surface row-level issues.
            _ = TestValidation.validate_row_test_fields(
                row, errors=errors, row_index=i
            )
            continue

        key = PanelValidation.panel_group_key(panel_kwargs)
        if key not in buckets:
            buckets[key] = {"panel_kwargs": panel_kwargs, "rows": []}
        buckets[key]["rows"].append((i, row))

    panel_batches: list[PanelBatch] = []
    for bucket in buckets.values():
        tests_kwargs: list[dict[str, Any]] = []
        row_indices: list[int] = []

        # Assign row_number sequentially *within the panel*
        row_num = 1
        for i, row in bucket["rows"]:
            row_indices.append(i)
            test_kwargs = TestValidation.validate_row_test_fields(
                row, errors=errors, row_index=i
            )
            if test_kwargs is None:
                continue
            test_kwargs["row_number"] = row_num
            row_num += 1
            tests_kwargs.append(test_kwargs)

        panel_batches.append(
            PanelBatch(
                panel_kwargs=bucket["panel_kwargs"],
                tests_kwargs=tests_kwargs,
                row_indices=row_indices,
            )
        )

    return MultiValidationResult(
        ok=(len(errors) == 0), panels=panel_batches, errors=errors
    )
