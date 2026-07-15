"""
Content schema version: v1.0.0.
For schema changes, create a new version.
"""

from __future__ import annotations

from pydantic import BaseModel, Field, model_validator

CONTENT_SCHEMA_VERSION = "v1.0.0"


class AnalyteFinding(BaseModel):
    analyte_code: str
    description: str
    trend_direction: str
    confidence: float = Field(ge=0.0, le=1.0)


class AIAnnotationContent(BaseModel):
    annotation_type: str
    secondary_types: list[str]
    summary: str
    analyte_findings: list[AnalyteFinding] = Field(min_length=1)
    requires_review: bool
    review_priority: str

    @model_validator(mode="after")
    def validate_priority_consistency(self) -> "AIAnnotationContent":
        if not self.requires_review and self.review_priority != "routine":
            raise ValueError(
                "review_priority must be 'routine' when requires_review is false"
            )
        return self


def parse_ai_annotation_content(raw_json: str) -> AIAnnotationContent:
    return AIAnnotationContent.model_validate_json(raw_json)


class RejectedAIAnnotationAudit(BaseModel):
    raw_llm_response: str
    rejection_reason: str


def build_rejected_ai_annotation_audit(
    *,
    raw_llm_response: str,
    rejection_reason: str,
) -> RejectedAIAnnotationAudit:
    return RejectedAIAnnotationAudit(
        raw_llm_response=raw_llm_response,
        rejection_reason=rejection_reason,
    )
