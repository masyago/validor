from app.ai.ai_annotation_prompt import (
    HistoricalObservation,
    ObservationRow,
    build_annotation_prompt_inputs,
)


def test_build_annotation_prompt_inputs_formats_all_panel_codes() -> None:
    inputs = build_annotation_prompt_inputs(
        ingestion_id="ing-123",
        panel_codes=["BMP", "LIPID", "BMP"],
        collected_at="2026-06-11T12:00:00+00:00",
        observations=[
            ObservationRow(
                analyte_code="GLU",
                value=5.8,
                unit="mmol/L",
                reference_low=3.9,
                reference_high=6.1,
                flag="NORMAL",
                date="2026-06-11T12:00:00+00:00",
            )
        ],
        historical_observations=[
            HistoricalObservation(
                analyte_code="GLU",
                value=5.5,
                unit="mmol/L",
                collected_at="2026-05-11T12:00:00+00:00",
                flag="NORMAL",
                date="2026-05-11T12:00:00+00:00",
            )
        ],
        rag_chunks=[],
    )

    assert inputs["panel_codes"] == "BMP, LIPID"
