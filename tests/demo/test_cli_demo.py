from __future__ import annotations

import io

from csv_uploader.cli_rich import make_console
from demo.cli_demo import _print_ingestion_processing_status


class _FakeResponse:
    def __init__(self, status_code: int, payload):
        self.status_code = status_code
        self._payload = payload
        self.ok = self.status_code < 400

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, responses: list[_FakeResponse]):
        self._responses = list(responses)

    def get(self, *args, **kwargs):
        if not self._responses:
            raise AssertionError("No more fake responses")
        return self._responses.pop(0)


def test_cli_demo_prints_ai_annotation_section_for_completed_ingestion():
    buffer = io.StringIO()
    console = make_console(
        file=buffer,
        force_terminal=False,
        color_system=None,
        highlight=False,
    )
    session = _FakeSession(
        [
            _FakeResponse(
                200,
                [
                    {
                        "event_type": "PARSE_SUCCEEDED",
                    },
                    {
                        "event_type": "VALIDATION_SUCCEEDED",
                    },
                    {
                        "event_type": "NORMALIZATION_SUCCEEDED",
                    },
                    {
                        "event_type": "FHIR_JSON_GENERATION_SUCCEEDED",
                    },
                    {
                        "event_type": "AI_ENRICHMENT_STARTED",
                    },
                    {
                        "event_type": "AI_ENRICHMENT_SUCCEEDED",
                    },
                ],
            ),
            _FakeResponse(
                200,
                [
                    {
                        "ai_annotation_id": "annot-123",
                        "ingestion_id": "ing-123",
                        "annotation_type": "anomaly_flag",
                        "provider": "amazon_bedrock",
                        "model_id": "claude-haiku-4-5",
                        "validation_status": "ACCEPTED",
                        "rejection_reason": None,
                        "content_json": {
                            "annotation_type": "anomaly_flag",
                            "summary": "Pattern suggests follow-up review.",
                            "requires_review": True,
                            "review_priority": "routine",
                            "analyte_findings": [
                                {
                                    "analyte_code": "TG",
                                    "description": "Elevated triglycerides may reflect recent dietary intake.",
                                    "trend_direction": "first result",
                                    "confidence": 0.65,
                                },
                                {
                                    "analyte_code": "HDL",
                                    "description": "Values above reference range warrant verification.",
                                    "trend_direction": "first result",
                                    "confidence": 0.65,
                                },
                            ],
                        },
                    }
                ],
            ),
            _FakeResponse(
                200,
                [
                    {
                        "code": "TG",
                        "value_num": 184.0,
                        "unit": "mg/dL",
                        "ref_low_num": 0.0,
                        "ref_high_num": 150.0,
                        "flag_system_interpretation": "HIGH",
                    },
                    {
                        "code": "HDL",
                        "value_num": 111.07,
                        "unit": "mg/dL",
                        "ref_low_num": 40.0,
                        "ref_high_num": 100.0,
                        "flag_system_interpretation": "HIGH",
                    },
                ],
            ),
            _FakeResponse(
                200,
                {
                    "patient_message_id": "msg-123",
                    "ingestion_id": "ing-123",
                    "patient_id": "PAT-1",
                    "patient_given_name": "Sam",
                    "patient_family_name": "Rivera",
                    "patient_email": "sam.rivera@demo.invalid",
                    "validation_status": "ACCEPTED",
                    "review_status": "PENDING_REVIEW",
                    "provider": "amazon_bedrock",
                    "model_id": "claude-haiku-4-5",
                    "correlation_id": "corr-123",
                    "draft_content_json": {
                        "subject": "Blood test results",
                        "opening": "Your results are now available. Here is a summary.",
                        "normal_summary": "Most of your results were normal.",
                        "abnormal_findings": [
                            {
                                "title": "Triglycerides",
                                "analyte_codes": ["TG"],
                                "explanation": "Triglycerides were a little high and will be monitored.",
                            }
                        ],
                        "recommendation": "Your care team will follow up if needed.",
                    },
                    "final_content_json": None,
                    "content_schema_version": "v1.1.0",
                },
            ),
        ]
    )

    _print_ingestion_processing_status(
        ingestion_id="ing-123",
        config={"api_base_url": "http://localhost:8000"},
        session=session,  # type: ignore[arg-type]
        console_out=console,
        status_payload_override={"status": "COMPLETED"},
    )

    output = buffer.getvalue()

    assert (
        "AI annotations: http://localhost:8000/v1/ingestions/ing-123/ai_annotation"
        in output
    )
    assert "AI ANNOTATION" in output
    assert "validation_status ACCEPTED" in output
    assert "annotation_type" in output
    assert "anomaly_flag" in output
    assert "provider / model" in output
    assert "amazon_bedrock / claude-haiku-4-5" in output
    assert "requires_review" in output
    assert "yes" in output
    assert "review_priority" in output
    assert "routine" in output
    assert "Summary" in output
    assert "Pattern suggests follow-up review." in output
    assert "TG" in output
    assert "HIGH" in output
    assert "184 mg/dL" in output
    assert "confidence 0.65" in output
    assert "trend: first result" in output

    # Patient message section
    assert "PATIENT MESSAGE" in output
    assert "review_status" in output
    assert "PENDING_REVIEW" in output
    assert "Sam Rivera" in output
    assert "Subject: Blood test results" in output
    assert "Most of your results were normal." in output
    assert "Triglycerides" in output
    assert "Your care team will follow up if needed." in output
