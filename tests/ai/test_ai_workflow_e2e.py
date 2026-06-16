from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
import requests
from langchain_core.messages import AIMessage, BaseMessage

from app.ai.ai_orchestration import (
    AIEnrichmentRequest,
    ObservationContext,
    orchestrate_ai_enrichment,
)
from dotenv import load_dotenv

# Points to test environment file
load_dotenv(dotenv_path=".env.test")

pytestmark = pytest.mark.e2e


DEFAULT_OPENAI_CHAT_MODEL = "gpt-4.1-mini"


class OpenAIChatCompletionsLLM:
    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        base_url: str,
        timeout: int = 120,
    ) -> None:
        self._model = model
        self._timeout = timeout
        self._endpoint = base_url.rstrip("/") + "/chat/completions"
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            }
        )

    def invoke(self, messages: list[BaseMessage]) -> AIMessage:
        payload = {
            "model": self._model,
            "temperature": 0,
            "messages": [_to_openai_message(message) for message in messages],
            "response_format": {"type": "json_object"},
        }
        response = self._session.post(
            self._endpoint,
            json=payload,
            timeout=self._timeout,
        )
        response.raise_for_status()

        content = (
            response.json()
            .get("choices", [{}])[0]
            .get("message", {})
            .get("content")
        )
        if not isinstance(content, str) or not content.strip():
            raise ValueError(
                "OpenAI chat completion did not return text content."
            )

        return AIMessage(content=content)


def _to_openai_message(message: BaseMessage) -> dict[str, str]:
    role = "assistant"
    if message.type == "system":
        role = "system"
    elif message.type == "human":
        role = "user"

    content = message.content
    if isinstance(content, str):
        text = content
    else:
        text = str(content)

    return {"role": role, "content": text}


def _render_prompt(messages: list[BaseMessage]) -> str:
    parts: list[str] = []
    for message in messages:
        role = message.type.upper()
        parts.append(f"[{role}]\n{message.content}")
    return "\n\n".join(parts)


def _build_lipid_workflow_request() -> AIEnrichmentRequest:
    now = datetime.now(timezone.utc).replace(microsecond=0)

    current_observations = [
        ObservationContext(
            code="TC",
            display="Total Cholesterol",
            value_num=245,
            value_text=None,
            unit="mg/dL",
            ref_low_num=0,
            ref_high_num=200,
            interpretation="HIGH",
            effective_at=now,
        ),
        ObservationContext(
            code="LDL",
            display="Low-Density Lipoprotein",
            value_num=165,
            value_text=None,
            unit="mg/dL",
            ref_low_num=0,
            ref_high_num=100,
            interpretation="HIGH",
            effective_at=now,
        ),
        ObservationContext(
            code="HDL",
            display="High-Density Lipoprotein",
            value_num=38,
            value_text=None,
            unit="mg/dL",
            ref_low_num=40,
            ref_high_num=100,
            interpretation="LOW",
            effective_at=now,
        ),
        ObservationContext(
            code="TG",
            display="Triglycerides",
            value_num=180,
            value_text=None,
            unit="mg/dL",
            ref_low_num=0,
            ref_high_num=150,
            interpretation="HIGH",
            effective_at=now,
        ),
    ]

    historical_observations = [
        ObservationContext(
            code="TC",
            display="Total Cholesterol",
            value_num=210,
            value_text=None,
            unit="mg/dL",
            ref_low_num=0,
            ref_high_num=200,
            interpretation="HIGH",
            effective_at=now - timedelta(days=90),
        ),
        ObservationContext(
            code="TC",
            display="Total Cholesterol",
            value_num=228,
            value_text=None,
            unit="mg/dL",
            ref_low_num=0,
            ref_high_num=200,
            interpretation="HIGH",
            effective_at=now - timedelta(days=45),
        ),
        ObservationContext(
            code="TC",
            display="Total Cholesterol",
            value_num=236,
            value_text=None,
            unit="mg/dL",
            ref_low_num=0,
            ref_high_num=200,
            interpretation="HIGH",
            effective_at=now - timedelta(days=14),
        ),
    ]

    return AIEnrichmentRequest(
        ingestion_id=uuid4(),
        patient_id="PATIENT-LIPID-001",
        panel_codes=["LIPID"],
        collected_at=now,
        current_observations=current_observations,
        historical_observations=historical_observations,
    )


# To run: OPENAI_API_KEY=... uv run pytest test_ai_workflow_e2e.py -m e2e -q
# To see printed outputs, replace `-q` flag with `-s`.
# To see output live: uv run pytest tests/ai/test_ai_workflow_e2e.py -m e2e --capture=tee-sys
def test_e2e_ai_workflow_prints_prompt_and_validated_annotation() -> None:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        pytest.skip("OPENAI_API_KEY is required for the AI workflow e2e test.")

    request = _build_lipid_workflow_request()
    llm = OpenAIChatCompletionsLLM(
        api_key=api_key,
        model=os.getenv("OPENAI_CHAT_MODEL", DEFAULT_OPENAI_CHAT_MODEL),
        base_url=os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1"),
    )

    result = orchestrate_ai_enrichment(request, llm=llm)

    full_prompt = _render_prompt(result.prompt_messages)
    print("\n=== FULL PROMPT START ===\n")
    print(full_prompt)
    print("\n=== FULL PROMPT END ===\n")

    print("\n=== VALIDATED ANNOTATION START ===\n")
    if result.llm_response_content is not None:
        print(result.llm_response_content.model_dump_json(indent=2))
    else:
        print("No validated annotation returned.")
    print("\n=== VALIDATED ANNOTATION END ===\n")

    assert "Panels: LIPID" in full_prompt
    assert "Patient Historical Results" in full_prompt
    assert result.llm_response_text is not None
    assert result.llm_response_content is not None
    assert result.llm_response_content.analyte_findings
