"""
Prompt version: v1.0.0.
For changes, create a new version.


AI Annotation Prompt Construction

Wires together:
  - System instructions (role, non-authoritative framing, output schema)
  - Current lab results with reference ranges and flag status
  - Historical results for the same patient (raw, from DB)
  - Retrieved RAG guideline chunks (from Vector Store)
  - JSON output format instructions (matching ai_annotation.content_json schema)
"""

from __future__ import annotations

from typing import Any

from langchain_core.prompts import (
    ChatPromptTemplate,
    HumanMessagePromptTemplate,
    SystemMessagePromptTemplate,
)
from pydantic import BaseModel, Field

from app.ai.content_versions.ai_annotation_content_v1_0_0 import (
    AIAnnotationContent,
    CONTENT_SCHEMA_VERSION,
    parse_ai_annotation_content,
)

PROMPT_VERSION = "v1.0.0"


# ---------------------------------------------------------------------------
# Output schema — internal format for ai_annotation.content_json (JSONB)
#
# Design principles:
#   - Internal only: no FHIR serialisation burden placed on the LLM.
#   - Human readable: a clinician reviewer must understand the JSON without a
#     codebook. Every field uses plain English keys and prose values.
#   - Structured for the Service Layer: machine-actionable fields (requires_review,
#     review_priority, confidence) are top-level so the Service Layer can act on
#     them without parsing nested prose.
#   - Confidence is per-finding, not per-annotation: different analytes in the
#     same panel may warrant very different certainty levels.
#
# The Service Layer re-validates every LLM response against this schema before
# persisting. The LLM's requires_review and review_priority values are advisory;
# the Service Layer may upgrade (never downgrade) them based on flag severity.
# ---------------------------------------------------------------------------

ANNOTATION_JSON_SCHEMA = {
    "type": "object",
    "required": [
        "annotation_type",
        "secondary_types",
        "summary",
        "analyte_findings",
        "requires_review",
        "review_priority",
    ],
    "additionalProperties": False,
    "properties": {
        "annotation_type": {
            "type": "string",
            "enum": [
                "anomaly_flag",
                "possible_interference",
                "followup_suggestion",
            ],
        },
        "secondary_types": {
            "type": "array",
            "items": {
                "type": "string",
                "enum": [
                    "anomaly_flag",
                    "possible_interference",
                    "followup_suggestion",
                ],
            },
            "uniqueItems": True,
        },
        # -----------------------------------------------------------------
        # Summary — panel-level narrative, the first thing a reviewer reads
        # -----------------------------------------------------------------
        "summary": {
            "type": "string",
            "description": (
                "Two to four sentences. Open by stating what was analysed: which "
                "panel(s), how many results, and whether historical data was "
                "available for comparison. Then give the overall picture — what "
                "is stable, what is moving, and what stands out. Be specific: "
                "name analytes, include values and units, and note the timeframe "
                "of any trend. Example: 'Most recent results for 3 panels (Lipid, "
                "Basic Metabolic, Liver Function) were analysed and compared "
                "against prior results. The majority of analytes are stable. "
                "Total cholesterol has risen from 120 to 175 mg/dL over 2 months "
                "and warrants monitoring.' Do NOT include diagnoses or treatment "
                "recommendations."
            ),
        },
        # -----------------------------------------------------------------
        # Per-analyte findings — only analytes that warrant comment
        # -----------------------------------------------------------------
        "analyte_findings": {
            "type": "array",
            "minItems": 1,
            "description": (
                "One entry per analyte outside reference range, showing a notable "
                "trend, or carrying a confidence concern. Omit analytes that are "
                "within range, stable, and unremarkable."
            ),
            "items": {
                "type": "object",
                "required": [
                    "analyte_code",
                    "description",
                    "trend_direction",
                    "confidence",
                ],
                "additionalProperties": False,
                "properties": {
                    "analyte_code": {
                        "type": "string",
                        "description": "Analyte code exactly as provided in the results table.",
                    },
                    # Single prose field — plain English, all context in one place
                    "description": {
                        "type": "string",
                        "description": (
                            "Plain-language description covering: current value and "
                            "unit, reference range, flag status, trend over time "
                            "(with values and dates where history is available), and "
                            "potential clinical meaning of the pattern. Two to four "
                            "sentences. Example: 'Total cholesterol is 175 mg/dL "
                            "(reference: 0–200 mg/dL), currently within range but "
                            "has risen from 120 mg/dL over the past 2 months. The "
                            "upward trend may warrant monitoring if it continues "
                            "toward the upper limit.' No diagnoses or treatment "
                            "recommendations."
                        ),
                    },
                    "trend_direction": {
                        "type": "string",
                        "enum": [
                            "increasing",
                            "decreasing",
                            "stable",
                            "first_result",
                            "indeterminate",
                        ],
                        "description": (
                            "Direction relative to the patient's own historical baseline. "
                            "'first_result': no prior values exist. "
                            "'indeterminate': history present but too sparse or variable "
                            "to establish a direction."
                        ),
                    },
                    "confidence": {
                        "type": "number",
                        "minimum": 0.0,
                        "maximum": 1.0,
                        "description": (
                            "Confidence in this individual finding (0.0–1.0). "
                            "Lower values reflect sparse history, borderline results, "
                            "or limited guideline coverage. Findings below 0.7 will "
                            "cause the Service Layer to set requires_review = true."
                        ),
                    },
                },
            },
        },
        # -----------------------------------------------------------------
        # Review triage
        # -----------------------------------------------------------------
        "requires_review": {
            "type": "boolean",
            "description": (
                "True if any finding has a CRITICAL_LOW or CRITICAL_HIGH flag, "
                "or if any finding confidence is below 0.7. The Service Layer "
                "may upgrade this value based on flag severity."
            ),
        },
        "review_priority": {
            "type": "string",
            "enum": ["routine", "urgent", "critical"],
            "description": (
                "'routine': no time-sensitive findings. "
                "'urgent': findings warrant same-day review. "
                "'critical': CRITICAL_LOW or CRITICAL_HIGH present — review before "
                "result is released. "
                "If requires_review is false, this must be 'routine'."
            ),
        },
    },
}


import json

OUTPUT_SCHEMA_STR = json.dumps(ANNOTATION_JSON_SCHEMA, indent=2)


# ---------------------------------------------------------------------------
# System prompt  —  role + non-authoritative framing + output contract
# ---------------------------------------------------------------------------

SYSTEM_TEMPLATE = """\
You are a clinical pathologist reviewing structured chemistry analyzer \
output within an automated data pipeline. \
Your role is to provide structured, non-authoritative annotations on chemistry \
analyzer results to support — not replace — clinician review.

## Constraints
- Your output is a structured annotation for clinician review. You are not \
the reviewing clinician for this case.
- You are NOT a licensed clinician and your output is NOT medical advice.
- Your annotations are informational flags for review, never diagnostic conclusions.
- Do NOT suggest diagnoses, treatments, or clinical actions.
- Do NOT speculate beyond what is supported by the provided reference ranges, \
patient history, and guideline excerpts.
- If evidence is insufficient to form a finding, set confidence below 0.7 and \
note the limitation in `analyte_findings`["description"]
- Do NOT include any patient-identifiable information (name, DOB, MRN) in your output.

## Output Contract
You MUST respond with a single, raw JSON object that validates against the \
following JSON Schema. Do NOT wrap it in markdown fences or add any preamble.

{output_schema}
"""

SYSTEM_PROMPT = SystemMessagePromptTemplate.from_template(
    SYSTEM_TEMPLATE,
    partial_variables={"output_schema": OUTPUT_SCHEMA_STR},
)


# ---------------------------------------------------------------------------
# Ingestion prompt:  current results + history + RAG chunks + format reminder
# ---------------------------------------------------------------------------

LAB_CONTEXT_TEMPLATE = """\
## Current Lab Results  (Ingestion ID: {ingestion_id})
Panels: {panel_codes}
Collected: {collected_at}

{current_results_table}
<!-- Format: analyte_code | value | unit | reference_low | reference_high | flag (NORMAL/LOW/HIGH/CRITICAL_LOW/CRITICAL_HIGH) | date -->

---

## Patient Historical Results  (same analytes, last {history_window_days} days)
{historical_results}
<!-- Raw rows from the observations table, ordered by collected_at DESC.
     Use these to assess trends. Absence of history means first result. -->

---

## Retrieved Clinical Guidelines  (RAG context — {rag_chunk_count} chunk(s))
{rag_chunks}
<!-- Semantic search results from the Vector Store.
     These are reference range documents and interpretation guidelines.
     Treat them as supporting evidence, not ground truth for this patient. -->

---

## Task
Analyse the current results in the context of the reference ranges, the \
patient's historical trend, and the guideline excerpts above.

Produce ONE `ai_annotation` JSON object.  Rules:
1. Choose the single most clinically relevant `annotation_type` for the overall panel.
2. Include one `analyte_findings` entry per analyte that warrants comment.  \
   Skip analytes that are normal with no notable trend.
3. Set `requires_review` to true if ANY finding has flag CRITICAL_LOW or CRITICAL_HIGH, \
   or if your confidence is below 0.7.
4. Do not invent reference ranges — use only the values provided above.
5. Do not reference the patient by name or any identifier other than analyte codes.

Respond with the raw JSON object only.
"""

INGESTION_PROMPT = HumanMessagePromptTemplate.from_template(
    LAB_CONTEXT_TEMPLATE
)


# ---------------------------------------------------------------------------
# Assembled ChatPromptTemplate
# ---------------------------------------------------------------------------

ANNOTATION_PROMPT = ChatPromptTemplate.from_messages(
    [SYSTEM_PROMPT, INGESTION_PROMPT]
)
"""
Input variables required by this template
------------------------------------------
ingestion_id         : str   — UUID of the ingestion record (non-PHI, for traceability)
panel_codes          : str   — comma-delimited panel codes for this ingestion, e.g. "CHEM14, BMP"
collected_at         : str   — ISO-8601 datetime string
current_results_table: str   — pipe-delimited table (see format comment in template)
history_window_days  : int   — lookback window used when querying historical rows
historical_results   : str   — formatted historical observations (or "No prior results.")
rag_chunk_count      : int   — number of chunks retrieved
rag_chunks           : str   — concatenated guideline chunks with source metadata
"""


# ---------------------------------------------------------------------------
# Helper: format current results into the pipe-delimited table the prompt expects
# ---------------------------------------------------------------------------


class ObservationRow(BaseModel):
    """Minimal domain model for a single analyte result passed to the prompt."""

    analyte_code: str
    value: float | str
    unit: str | None
    reference_low: float | None = Field(default=None)
    reference_high: float | None = Field(default=None)
    flag: str  # NORMAL | LOW | HIGH | CRITICAL_LOW | CRITICAL_HIGH
    date: str  # effective_at


def format_panel_codes(panel_codes: list[str]) -> str:
    ordered_unique_codes = list(dict.fromkeys(panel_codes))
    if not ordered_unique_codes:
        return "Unknown panel"
    return ", ".join(ordered_unique_codes)


def format_current_results(observations: list[ObservationRow]) -> str:
    """
    Serialise a list of ObservationRow instances into the pipe-delimited
    table format expected by HUMAN_TEMPLATE.

    Example output line:
        GLU | 5.8 | mmol/L | 3.9 | 6.1 | NORMAL
    """
    header = "analyte_code | value | unit | reference_low | reference_high | flag | date"
    separator = "-" * len(header)
    rows = [header, separator]
    for obs in observations:
        low = (
            str(obs.reference_low) if obs.reference_low is not None else "N/A"
        )
        high = (
            str(obs.reference_high)
            if obs.reference_high is not None
            else "N/A"
        )
        rows.append(
            f"{obs.analyte_code} | {obs.value} | {obs.unit} | {low} | {high} | {obs.flag} | {obs.date}"
        )
    return "\n".join(rows)


# ---------------------------------------------------------------------------
# Helper: format retrieved RAG chunks
# ---------------------------------------------------------------------------


class RagChunk(BaseModel):
    """Single chunk returned from the Vector Store semantic search."""

    source_type: str  # DOCUMENT | OBSERVATION | DIAGNOSTIC_REPORT
    source_id: str  # PK of the source record
    chunk_index: int
    chunk_text: str
    similarity_score: float  # cosine similarity from pgvector


def format_rag_chunks(chunks: list[RagChunk]) -> str:
    """
    Render retrieved chunks for inclusion in the prompt.
    Strips source_id to avoid PHI leakage when source_type is OBSERVATION.
    """
    if not chunks:
        return "No guideline context retrieved."

    parts: list[str] = []
    for i, chunk in enumerate(chunks, start=1):
        # Omit source_id for OBSERVATION chunks — those may contain patient data
        source_ref = (
            f"{chunk.source_type}[chunk {chunk.chunk_index}]"
            if chunk.source_type == "OBSERVATION"
            else f"{chunk.source_type}/{chunk.source_id}[chunk {chunk.chunk_index}]"
        )
        parts.append(
            f"[{i}] ({source_ref}, similarity={chunk.similarity_score:.3f})\n"
            f"{chunk.chunk_text.strip()}"
        )
    return "\n\n".join(parts)


# ---------------------------------------------------------------------------
# Helper: format historical observations (raw DB rows)
# ---------------------------------------------------------------------------


class HistoricalObservation(BaseModel):
    analyte_code: str
    value: float | str
    unit: str | None
    collected_at: str  # ISO-8601
    flag: str
    date: str  # effective_at


def format_historical_results(history: list[HistoricalObservation]) -> str:
    """
    Render historical rows grouped by analyte, newest first.
    Returns a human-readable string for inclusion in the prompt.
    """
    if not history:
        return "No prior results found for this patient within the lookback window."

    # Group by analyte
    grouped: dict[str, list[HistoricalObservation]] = {}
    for row in history:
        grouped.setdefault(row.analyte_code, []).append(row)

    parts: list[str] = []
    for analyte, rows in grouped.items():
        rows_sorted = sorted(rows, key=lambda r: r.collected_at, reverse=True)
        header = f"{analyte}:"
        lines = [
            f"  {r.collected_at[:10]}  {r.value} {r.unit or 'N/A'}  [{r.flag}]"
            for r in rows_sorted
        ]
        parts.append(header + "\n" + "\n".join(lines))
    return "\n\n".join(parts)


# ---------------------------------------------------------------------------
# Convenience builder: assemble all prompt inputs in one call
# ---------------------------------------------------------------------------


def build_annotation_prompt_inputs(
    *,
    ingestion_id: str,
    panel_codes: list[str],
    collected_at: str,
    observations: list[ObservationRow],
    historical_observations: list[HistoricalObservation],
    rag_chunks: list[RagChunk],
    history_window_days: int = 90,
) -> dict[str, Any]:
    """
    Produce the kwargs dict to pass into ANNOTATION_PROMPT.format_messages(**kwargs).

    Usage
    -----
        inputs = build_annotation_prompt_inputs(
            ingestion_id=str(ingestion.ingestion_id),
            panel_codes=[report.panel_code for report in diagnostic_reports],
            collected_at=ingestion.collected_at.isoformat(),
            observations=observation_rows,
            historical_observations=history_rows,
            rag_chunks=retrieved_chunks,
        )
        messages = ANNOTATION_PROMPT.format_messages(**inputs)
        response = llm.invoke(messages)   # LangChain Bedrock chat model
    """
    return {
        "ingestion_id": ingestion_id,
        "panel_codes": format_panel_codes(panel_codes),
        "collected_at": collected_at,
        "current_results_table": format_current_results(observations),
        "history_window_days": history_window_days,
        "historical_results": format_historical_results(
            historical_observations
        ),
        "rag_chunk_count": len(rag_chunks),
        "rag_chunks": format_rag_chunks(rag_chunks),
    }
