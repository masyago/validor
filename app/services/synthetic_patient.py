"""
Synthetic demographics for a patient_id.

The demo has no real patient names/emails anywhere in the pipeline (the parser
only reads patient_id). To render a recognizable "To:" line without inventing or
storing real PHI, we use a single well-known placeholder identity (Jane Doe) for
every patient. Rows are flagged `is_synthetic=TRUE`.
"""
from __future__ import annotations

_DEMO_GIVEN_NAME = "Jane"
_DEMO_FAMILY_NAME = "Doe"
_DEMO_EMAIL_DOMAIN = "validor.demo"


def synthetic_patient_fields(patient_id: str) -> dict[str, str]:
    """Return the synthetic {given_name, family_name, email} for a patient_id.

    Always the same placeholder identity (Jane Doe) — no real PHI. Email uses a
    non-routable demo domain so nothing could ever be delivered.
    """
    local = f"{_DEMO_GIVEN_NAME}.{_DEMO_FAMILY_NAME}".lower()
    return {
        "given_name": _DEMO_GIVEN_NAME,
        "family_name": _DEMO_FAMILY_NAME,
        "email": f"{local}@{_DEMO_EMAIL_DOMAIN}",
    }
