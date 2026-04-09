"""
Defines FHIR R4 serializers and minimal Pydantic models for DiagnosticReport
and Observation resources.
"""

from __future__ import annotations
from typing import Literal, Optional, Any, Sequence
from pydantic import model_validator, ValidationError, Field
from uuid import UUID
from datetime import datetime, timezone
from app.domain.fhir.base import (
    Annotation,
    Coding,
    CodeableConcept,
    FHIRBaseModel,
    Meta,
    ObservationReferenceRange,
    Reference,
    Quantity,
)

from app.persistence.models.normalization import Observation, DiagnosticReport

FHIRObservationStatus = Literal[
    "registered",
    "preliminary",
    "final",
    "amended",
    "corrected",
    "cancelled",
    "entered-in-error",
    "unknown",
]

FHIRDiagnosticReportStatus = Literal[
    "registered",
    "partial",
    "preliminary",
    "final",
    "amended",
    "corrected",
    "appended",
    "cancelled",
    "entered-in-error",
    "unknown",
]


def _as_utc_iso(dt: datetime) -> str:
    """
    FHIR dateTime is ISO 8601 (includes timezone)
    """
    if dt.tzinfo is None:
        # Defensive: normalized db tables contain timestamps with timezones already but still treat naive.
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat(timespec="seconds")


def _patient_ref(patient_id: str) -> str:
    return f"Patient/{patient_id}"


def _resource_ref(resource_type: str, resource_id: str | UUID) -> str:
    rid = str(resource_id)
    return f"{resource_type}/{rid}"


class ObservationR4(FHIRBaseModel):
    resourceType: Literal["Observation"] = "Observation"
    id: str  # Observation.observation_id value

    meta: Optional[Meta] = None  # Observation.normalized_at value
    status: FHIRObservationStatus = "final"

    code: CodeableConcept  # Observation.code, Observation.display
    subject: Reference  # Observation.patient_id value
    effectiveDateTime: str  # Observation.effective_at

    # One of the following should be set
    valueQuantity: Optional[Quantity] = (
        None  # Observation.value_num (and unit?)
    )
    valueString: Optional[str] = None  # Observation.value_text

    referenceRange: Optional[list[ObservationReferenceRange]] = None

    # value from Observation.flag_system_interpretation
    interpretation: Optional[list[CodeableConcept]] = None

    # Can include  Observation.flag_analyzer_interpretation,
    # Observation.discrepancy_note
    note: Optional[list[Annotation]] = None

    @model_validator(mode="after")  # validates after model instantiation
    def _validate_value_choice(self) -> "ObservationR4":
        # Require at least one value field
        if self.valueQuantity is None and self.valueString is None:
            raise ValueError(
                "Observation must have valueQuantity or valueString"
            )
        return self


class DiagnosticReportR4(FHIRBaseModel):
    resourceType: Literal["DiagnosticReport"] = "DiagnosticReport"
    id: str  # DiagnosticReport.diagnostic_report_id value

    meta: Optional[Meta] = None  #  DiagnosticReport.normalized_at
    status: FHIRDiagnosticReportStatus = "final"

    code: CodeableConcept  #  DiagnosticReport.panel_code
    subject: Reference  #  DiagnosticReport.patient_id

    effectiveDateTime: str  # DiagnosticReport.effective_at

    # "result" references Observations produced for this report/panel
    result: list[Reference] = Field(default_factory=list)


# SERIALIZER


class R4ObsDrV1Serializer:
    """
    FHIR R4 serializer for Observation and DiagnosticReport.
    Maps normalized database rows and returns FHIR JSON-ready dicts.
    """

    # versioning
    fhir_release: str = "R4"
    serializer_version: str = "obs-dr-v1"
    full_version: str = "fhir-r4-obs-dr-v1"

    # URI's
    INTERNAL_FLAG_SYSTEM = "https://example.org/fhir/flag-system/lab-flag"
    INTERNAL_CODE_SYSTEM = (
        "https://example.org/fhir/code-system/canonical-analyte"
    )
    UCUM_SYSTEM = "http://unitsofmeasure.org"

    def make_observation(self, ob: Observation) -> dict[str, Any]:
        """
        Maps normalized Observation row to an R4 Observation dict.
        """

        value_quantity: Optional[Quantity] = None
        value_string: Optional[str] = None

        if getattr(ob, "value_num") is not None:
            value_quantity = Quantity(
                value=float(getattr(ob, "value_num")),
                unit=getattr(ob, "unit"),
                system=self.UCUM_SYSTEM if getattr(ob, "unit") else None,
                code=getattr(ob, "unit"),
                comparator=getattr(ob, "comparator"),
            )
        elif getattr(ob, "value_text") is not None:
            value_string = str(getattr(ob, "value_text"))

        # Reference range
        ref_range: Optional[list[ObservationReferenceRange]] = None
        low = getattr(ob, "ref_low_num")
        high = getattr(ob, "ref_high_num")
        unit = getattr(ob, "unit")

        if low is not None or high is not None:
            if low is not None:
                low = Quantity(
                    value=float(low),
                    unit=unit,
                    system=self.UCUM_SYSTEM if unit else None,
                    code=unit,
                )
            else:
                low = None

            if high is not None:
                high = Quantity(
                    value=float(high),
                    unit=unit,
                    system=self.UCUM_SYSTEM if unit else None,
                    code=unit,
                )

            else:
                high = None

            ref_range = [ObservationReferenceRange(low=low, high=high)]

        # Interpretation (system-derived flag)
        interpretation: Optional[list[CodeableConcept]] = None
        sys_flag = getattr(ob, "flag_system_interpretation", None)
        if sys_flag:
            interpretation = [
                CodeableConcept(
                    coding=[
                        Coding(
                            system=self.INTERNAL_FLAG_SYSTEM,
                            code=sys_flag,
                            display=sys_flag,
                        )
                    ],
                    text=sys_flag,
                )
            ]

        # Notes: analyzer flag and discrepancy details
        notes: list[Annotation] = []
        analyzer_flag = getattr(ob, "flag_analyzer_interpretation", None)
        discrepancy = getattr(ob, "discrepancy", None)

        if analyzer_flag:
            notes.append(
                Annotation(
                    text=f"Analyzer interpretation flag: {analyzer_flag}"
                )
            )

        if discrepancy:
            notes.append(Annotation(text=f"Flag discrepancy: {discrepancy}"))

        note_field: Optional[list[Annotation]] = notes or None

        last_updated_iso = _as_utc_iso(getattr(ob, "normalized_at"))

        model = ObservationR4(
            id=str(getattr(ob, "observation_id")),
            meta=Meta(lastUpdated=last_updated_iso),
            status="final",
            code=CodeableConcept.from_code_display(
                code=str(getattr(ob, "code")),
                display=getattr(ob, "display"),
                system=self.INTERNAL_CODE_SYSTEM,
            ),
            subject=Reference(
                reference=_patient_ref(str(getattr(ob, "patient_id")))
            ),
            effectiveDateTime=_as_utc_iso(getattr(ob, "effective_at")),
            valueQuantity=value_quantity,
            valueString=value_string,
            referenceRange=ref_range,
            interpretation=interpretation,
            note=note_field,
        )

        return model.model_dump(exclude_none=True)

    def make_diagnostic_report(
        self, dr: DiagnosticReport, observations: Sequence[Any]
    ) -> dict[str, Any]:
        """
        Map one normalized DiagnosticReport row (and related Observations) to
        an R4 DiagnosticReport dict.
        """

        result_refs = [
            Reference(
                reference=_resource_ref(
                    "Observation", getattr(ob, "observation_id")
                )
            )
            for ob in observations
        ]

        normalized_at = getattr(dr, "normalized_at", None)
        if normalized_at is None:
            # Phase 1/2 pipeline invariants: normalized_at should be set during
            # Phase 1. If it's missing, treat this as a deterministic data
            # issue so Phase 2 can mark the resource as a JSON failure.
            raise ValueError("DiagnosticReport.normalized_at is required")

        last_updated_iso = _as_utc_iso(normalized_at)

        model = DiagnosticReportR4(
            id=str(getattr(dr, "diagnostic_report_id")),
            meta=Meta(lastUpdated=last_updated_iso),
            status="final",
            code=CodeableConcept.from_code_display(
                code=str(getattr(dr, "panel_code")),
                display=str(getattr(dr, "panel_code")),
                system="https://example.org/fhir/codeable-concept/panel-code",
            ),
            subject=Reference(
                reference=_patient_ref(str(getattr(dr, "patient_id")))
            ),
            effectiveDateTime=_as_utc_iso(getattr(dr, "effective_at")),
            result=result_refs,
        )
        return model.model_dump(exclude_none=True)
