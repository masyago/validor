from __future__ import annotations

import uuid
from typing import Annotated

from pydantic import AfterValidator


def normalize_patient_id(value: str) -> str:
    """
    Normalize a patient identifier of the form `PAT-<uuid>`.

    - Accepts any case for the `PAT-` prefix (e.g., `pat-...`, `Pat-...`).
    - Canonicalizes to `PAT-<uuid>` where the UUID is in canonical lowercase.
    """

    if not isinstance(value, str):
        raise ValueError("patient_id must be a string")

    candidate = value.strip()
    prefix, sep, rest = candidate.partition("-")
    if sep != "-":
        raise ValueError("patient_id must be in the form PAT-<uuid>")

    if prefix.lower() != "pat":
        raise ValueError("patient_id must start with PAT-")

    try:
        parsed_uuid = uuid.UUID(rest)
    except ValueError as exc:
        raise ValueError("patient_id must be in the form PAT-<uuid>") from exc

    return f"PAT-{parsed_uuid}"  # uuid.UUID -> canonical lowercase


# Applies validation function after string validation
PatientId = Annotated[str, AfterValidator(normalize_patient_id)]
