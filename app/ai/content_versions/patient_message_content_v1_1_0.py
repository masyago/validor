"""
Patient message content schema version: v1.1.0.
For schema changes, create a new version.

This is the schema-enforced shape of the LLM's patient-message draft, stored
immutably in patient_message.draft_content_json. It contains NO PHI: the
patient's name/email, the clinic name, and the clinician signature are all
applied only at render/send time, never baked in here.

v1.1.0 shapes the draft as a clinical letter: a subject line, an opening, a
plain-language roll-up of the in-range results, a list of abnormal findings that
warrant follow-up, and an adaptive recommendation.
"""

from __future__ import annotations

from pydantic import BaseModel, Field, model_validator

CONTENT_SCHEMA_VERSION = "v1.1.0"


class AbnormalFinding(BaseModel):
    """One out-of-range (or notably trending) result worth following up on."""

    # Friendly heading, e.g. "Liver enzymes (ALT and AST)".
    title: str
    # Analyte codes grounding this finding, e.g. ["ALT", "AST"].
    analyte_codes: list[str] = Field(default_factory=list)
    # Plain-language paragraph: what the analyte measures, how it deviated, any
    # trend, and that follow-up/monitoring is recommended. No diagnosis.
    explanation: str

    @model_validator(mode="after")
    def validate_non_empty(self) -> "AbnormalFinding":
        for field_name in ("title", "explanation"):
            if not getattr(self, field_name).strip():
                raise ValueError(f"{field_name} must not be empty")
        return self


class PatientMessageContent(BaseModel):
    # Subject topic only — no date and no name (both applied at render time).
    subject: str
    # One or two sentences: results are available, here is a summary.
    opening: str
    # Plain-language roll-up of in-range results. May be empty if none normal.
    normal_summary: str = ""
    # Numbered follow-up items; empty when everything is in range.
    abnormal_findings: list[AbnormalFinding] = Field(default_factory=list)
    # Adaptive next-steps guidance framed around clinician follow-up. No clinic
    # name and no identifiers — the renderer owns the contact/signature block.
    recommendation: str

    @model_validator(mode="after")
    def validate_non_empty(self) -> "PatientMessageContent":
        for field_name in ("subject", "opening", "recommendation"):
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
