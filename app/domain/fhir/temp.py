### FOR REFERENCE ONLY. DO NOT USE.

"""
app/domain/fhir/r4/obs_dr_v1.py

FHIR R4 serializers + minimal Pydantic models for:
- Observation
- DiagnosticReport

Scope: canonical quantitative chemistry-like lab tests.
This module is PURE: no DB sessions, no repositories, no services.
It accepts normalized ORM rows (or dataclass-like objects) and emits dict JSON.

Pydantic v2 required.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional, Sequence, Union
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator


# ----------------------------
# Shared helpers (pure)
# ----------------------------


def _as_utc_iso(dt: datetime) -> str:
    """
    FHIR dateTime is ISO 8601. We keep timezone info.
    Ensure dt is timezone-aware in upstream (your columns are TIMESTAMP(timezone=True)).
    """
    if dt.tzinfo is None:
        # Defensive: treat naive as UTC rather than emitting invalid ambiguous timestamps.
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat(timespec="seconds")


def _patient_ref(patient_id: str) -> str:
    # You are not building Patient resources; still emit a valid Reference.
    return f"Patient/{patient_id}"


def _resource_ref(resource_type: str, resource_id: Union[str, UUID]) -> str:
    rid = str(resource_id)
    return f"{resource_type}/{rid}"


# ---------------------------
# Base / primitives (minimal R4 subset)
# (You can move these to domain/fhir/base.py later if you want.)
# ----------------------------


class FHIRBaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class Meta(FHIRBaseModel):
    lastUpdated: Optional[str] = (
        None  # FHIR instant, we use ISO dateTime string
    )


class Reference(FHIRBaseModel):
    reference: str


class Coding(FHIRBaseModel):
    system: Optional[str] = None
    code: Optional[str] = None
    display: Optional[str] = None


class CodeableConcept(FHIRBaseModel):
    coding: Optional[List[Coding]] = None
    text: Optional[str] = None

    @staticmethod
    def from_code_display(
        code: str,
        display: Optional[str] = None,
        system: Optional[str] = None,
    ) -> "CodeableConcept":
        return CodeableConcept(
            coding=[Coding(system=system, code=code, display=display)],
            text=display or code,
        )


class Quantity(FHIRBaseModel):
    value: Optional[float] = None
    unit: Optional[str] = None
    system: Optional[str] = None
    code: Optional[str] = None
    comparator: Optional[Literal["<", "<=", ">=", ">", "="]] = None


class Annotation(FHIRBaseModel):
    text: str
    time: Optional[str] = None


class ObservationReferenceRange(FHIRBaseModel):
    low: Optional[Quantity] = None
    high: Optional[Quantity] = None
    text: Optional[str] = None


# ----------------------------
# Resource models (R4 minimal)
# ----------------------------

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


class ObservationR4(FHIRBaseModel):
    resourceType: Literal["Observation"] = "Observation"
    id: str

    meta: Optional[Meta] = None
    status: FHIRObservationStatus = "final"

    code: CodeableConcept
    subject: Reference
    effectiveDateTime: str

    # One of the following should be set for your use-case.
    valueQuantity: Optional[Quantity] = None
    valueString: Optional[str] = None

    referenceRange: Optional[List[ObservationReferenceRange]] = None

    # Interpretation is CodeableConcept in FHIR; we'll emit an internal coding.
    interpretation: Optional[List[CodeableConcept]] = None

    # Notes: analyzer flag, discrepancies, etc.
    note: Optional[List[Annotation]] = None

    # Optional linkage if you want it (DiagnosticReport.result is primary linkage).
    derivedFrom: Optional[List[Reference]] = None

    @model_validator(mode="after")
    def _validate_value_choice(self) -> "ObservationR4":
        # Require at least one value field in your canonical chemistry scope.
        if self.valueQuantity is None and self.valueString is None:
            raise ValueError(
                "Observation must have valueQuantity or valueString"
            )
        # Disallow both at once for simplicity.
        if self.valueQuantity is not None and self.valueString is not None:
            raise ValueError(
                "Observation must not have both valueQuantity and valueString"
            )
        return self


class DiagnosticReportR4(FHIRBaseModel):
    resourceType: Literal["DiagnosticReport"] = "DiagnosticReport"
    id: str

    meta: Optional[Meta] = None
    status: FHIRDiagnosticReportStatus = "final"

    code: CodeableConcept
    subject: Reference

    effectiveDateTime: str

    # "result" references Observations produced for this report/panel
    result: List[Reference] = Field(default_factory=list)

    # We intentionally omit "issued" in v1 because you are not storing true lab-issued time.
    # issued: Optional[str] = None


# ----------------------------
# Row protocol (light typing)
# You can pass in SQLAlchemy ORM instances as long as they have these attrs.
# ----------------------------


@dataclass(frozen=True)
class DiagnosticReportRow:
    diagnostic_report_id: UUID
    patient_id: str
    panel_code: str
    effective_at: datetime
    normalized_at: datetime


@dataclass(frozen=True)
class ObservationRow:
    observation_id: UUID
    diagnostic_report_id: UUID
    patient_id: str
    code: str
    display: Optional[str]
    effective_at: datetime
    normalized_at: datetime

    value_num: Optional[float]
    value_text: Optional[str]
    comparator: Optional[str]
    unit: Optional[str]

    ref_low_num: Optional[float]
    ref_high_num: Optional[float]

    flag_analyzer_interpretation: Optional[str]
    flag_system_interpretation: Optional[str]
    discrepancy: Optional[str]


# ----------------------------
# Serializer (versioned)
# ----------------------------


class R4ObsDrV1Serializer:
    """
    FHIR R4 Observation + DiagnosticReport serializer (v1).

    - Pure mapping: accepts normalized DB rows and emits FHIR JSON-ready dicts.
    - Stores processing time in meta.lastUpdated (normalized_at).
    """

    fhir_release: str = "R4"
    serializer_version: str = "obs-dr-v1"
    full_version: str = "fhir-r4-obs-dr-v1"

    # You can optionally set these to something more realistic later.
    INTERNAL_FLAG_SYSTEM = "urn:example:lab-flag"
    INTERNAL_CODE_SYSTEM = "urn:example:analyte-code"
    UCUM_SYSTEM = "http://unitsofmeasure.org"

    def make_observation(
        self, ob: Any, *, report_ref: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Map one normalized Observation row to an R4 Observation dict.
        `ob` may be an ORM row; required attributes are used directly.
        """

        # Value choice
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
        rr: Optional[List[ObservationReferenceRange]] = None
        low = getattr(ob, "ref_low_num")
        high = getattr(ob, "ref_high_num")
        unit = getattr(ob, "unit")

        if low is not None or high is not None:
            rr = [
                ObservationReferenceRange(
                    low=(
                        Quantity(
                            value=float(low),
                            unit=unit,
                            system=self.UCUM_SYSTEM if unit else None,
                            code=unit,
                        )
                        if low is not None
                        else None
                    ),
                    high=(
                        Quantity(
                            value=float(high),
                            unit=unit,
                            system=self.UCUM_SYSTEM if unit else None,
                            code=unit,
                        )
                        if high is not None
                        else None
                    ),
                )
            ]

        # Interpretation (system-derived flag)
        interp: Optional[List[CodeableConcept]] = None
        sys_flag = getattr(ob, "flag_system_interpretation", None)
        if sys_flag:
            interp = [
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
        notes: List[Annotation] = []
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

        note_field: Optional[List[Annotation]] = notes or None

        derived_from: Optional[List[Reference]] = None
        if report_ref:
            derived_from = [Reference(reference=report_ref)]

        model = ObservationR4(
            id=str(getattr(ob, "observation_id")),
            meta=Meta(lastUpdated=_as_utc_iso(getattr(ob, "normalized_at"))),
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
            referenceRange=rr,
            interpretation=interp,
            note=note_field,
            derivedFrom=derived_from,
        )
        return model.model_dump(exclude_none=True)

    def make_diagnostic_report(
        self,
        dr: Any,
        observations: Sequence[Any],
    ) -> Dict[str, Any]:
        """
        Map one normalized DiagnosticReport row + its Observations to an R4 DiagnosticReport dict.
        """

        result_refs = [
            Reference(
                reference=_resource_ref(
                    "Observation", getattr(ob, "observation_id")
                )
            )
            for ob in observations
        ]

        model = DiagnosticReportR4(
            id=str(getattr(dr, "diagnostic_report_id")),
            meta=Meta(lastUpdated=_as_utc_iso(getattr(dr, "normalized_at"))),
            status="final",
            code=CodeableConcept.from_code_display(
                code=str(getattr(dr, "panel_code")),
                display=str(getattr(dr, "panel_code")),
                system="urn:example:panel-code",
            ),
            subject=Reference(
                reference=_patient_ref(str(getattr(dr, "patient_id")))
            ),
            effectiveDateTime=_as_utc_iso(getattr(dr, "effective_at")),
            result=result_refs,
        )
        return model.model_dump(exclude_none=True)
