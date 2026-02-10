from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional, TypeVar


@dataclass
class NormalizationError:
    model: str
    field: str
    message: str


def parse_str_to_num(s: str) -> float | None:
    try:
        return float(s)
    except ValueError:
        return None


def optional(val: Any) -> str | None:
    if val is None:
        return None
    if isinstance(val, str):
        s = val.strip()
        return s if s else None
    s = str(val).strip()
    return s if s else None


T = TypeVar("T")


def require_non_null(
    model: str, field: str, val: T | None, errors: list[NormalizationError]
) -> T | None:
    if val is None:
        errors.append(
            NormalizationError(
                model=model, field=field, message="required field missing"
            )
        )
        return None
    return val


def require_str(
    model: str, field: str, val: Any, errors: list[NormalizationError]
) -> Optional[str]:
    s = optional(val)
    if s is None:
        errors.append(
            NormalizationError(
                model=model, field=field, message="required field missing"
            )
        )
        return None
    return s


def require_aware_datetime(
    model: str, field: str, val: Any, errors: list[NormalizationError]
) -> Optional[datetime]:
    if not isinstance(val, datetime):
        errors.append(
            NormalizationError(
                model=model, field=field, message="expected datetime"
            )
        )
        return None
    if val.tzinfo is None:
        return val.replace(tzinfo=timezone.utc)
    return val
