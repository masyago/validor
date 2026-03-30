from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.domain.fhir.base import CodeableConcept, Reference, Quantity
from app.domain.fhir.r4.obs_dr_v1 import ObservationR4


def test_observation_allows_value_quantity_and_value_string_together() -> None:
    model = ObservationR4(
        id="obs-1",
        code=CodeableConcept.from_code_display(
            code="GLU",
            display="Glucose",
            system="https://example.org/system",
        ),
        subject=Reference(reference="Patient/PAT-1"),
        effectiveDateTime="2026-01-01T00:00:00+00:00",
        valueQuantity=Quantity(
            value=1.23,
            unit="mg/dL",
            system="http://unitsofmeasure.org",
            code="mg/dL",
        ),
        valueString="NEGATIVE",
    )

    dumped = model.model_dump(exclude_none=True)
    assert dumped["resourceType"] == "Observation"
    assert "valueQuantity" in dumped
    assert "valueString" in dumped


def test_observation_requires_value_quantity_or_value_string() -> None:
    with pytest.raises(ValidationError) as excinfo:
        ObservationR4(
            id="obs-1",
            code=CodeableConcept.from_code_display(
                code="GLU",
                display="Glucose",
                system="https://example.org/system",
            ),
            subject=Reference(reference="Patient/PAT-1"),
            effectiveDateTime="2026-01-01T00:00:00+00:00",
            valueQuantity=None,
            valueString=None,
        )

    # The model validator raises ValueError; Pydantic surfaces it as ValidationError.
    assert "valueQuantity or valueString" in str(excinfo.value)
