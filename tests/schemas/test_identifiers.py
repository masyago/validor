import uuid

import pytest
from pydantic import TypeAdapter

from app.schemas.identifiers import PatientId, normalize_patient_id


def test_normalize_patient_id_accepts_case_insensitive_prefix():
    u = uuid.uuid4()
    assert normalize_patient_id(f"PAT-{u}") == f"PAT-{u}"
    assert normalize_patient_id(f"pat-{u}") == f"PAT-{u}"
    assert normalize_patient_id(f"PaT-{u}") == f"PAT-{u}"


def test_patient_id_type_adapter_validates_and_normalizes() -> None:
    u = uuid.uuid4()
    adapter = TypeAdapter(PatientId)

    assert adapter.validate_python(f"PAT-{u}") == f"PAT-{u}"
    assert adapter.validate_python(f"  pat-{u}  ") == f"PAT-{u}"

    with pytest.raises(Exception):
        adapter.validate_python(f"PX-{u}")

    with pytest.raises(Exception):
        adapter.validate_python("PAT-not-a-uuid")


def test_normalize_patient_id_strips_whitespace_and_canonicalizes_uuid():
    # Uppercase input UUID should normalize to canonical lowercase.
    u = uuid.uuid4()
    assert normalize_patient_id(f"  PAT-{str(u).upper()}  ") == f"PAT-{u}"


@pytest.mark.parametrize(
    "value",
    [
        "PAT",  # missing hyphen and uuid
        "PAT-",  # missing uuid
        "PA-123",  # wrong prefix
        "PAT-not-a-uuid",
        "PAT-1234",
    ],
)
def test_normalize_patient_id_rejects_invalid_values(value: str):
    with pytest.raises(ValueError):
        normalize_patient_id(value)
