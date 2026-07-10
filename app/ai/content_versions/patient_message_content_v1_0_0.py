"""
Patient message content schema version: v1.0.0.
For schema changes, create a new version.

This is the schema-enforced shape of the LLM's patient-message draft, stored
immutably in patient_message.draft_content_json. It contains NO PHI: the
patient's name/email are applied only at render/send time, never baked in here.
"""

from __future__ import annotations

from pydantic import BaseModel, Field, model_validator

CONTENT_SCHEMA_VERSION = "v1.0.0"


class ResultNote(BaseModel):
    """One plain-language note about a single analyte result."""

    analyte_code: str
    plain_language: str


class PatientMessageContent(BaseModel):
    # A generic salutation with no name (name is applied at render time).
    greeting: str
    # Plain-language, reassuring overview of the result set.
    summary: str
    # Per-result plain-language notes (may be empty if nothing warrants comment).
    result_notes: list[ResultNote] = Field(default_factory=list)
    # What the patient should do next (e.g. "your clinician will follow up").
    next_steps: str
    # Non-diagnostic disclaimer; the message is clinician-reviewed before sending.
    disclaimer: str

    @model_validator(mode="after")
    def validate_non_empty(self) -> "PatientMessageContent":
        for field_name in ("greeting", "summary", "next_steps", "disclaimer"):
            if not getattr(self, field_name).strip():
                raise ValueError(f"{field_name} must not be empty")
        return self


def parse_patient_message_content(raw_json: str) -> PatientMessageContent:
    return PatientMessageContent.model_validate_json(raw_json)


class RejectedPatientMessageAudit(BaseModel):
    raw_llm_response: str
    rejection_reason: str


def build_rejected_patient_message_audit(
    *,
    raw_llm_response: str,
    rejection_reason: str,
) -> RejectedPatientMessageAudit:
    return RejectedPatientMessageAudit(
        raw_llm_response=raw_llm_response,
        rejection_reason=rejection_reason,
    )
