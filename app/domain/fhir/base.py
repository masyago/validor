from pydantic import BaseModel, ConfigDict
from typing import Optional, Literal
from datetime import datetime, timezone


class FHIRBaseModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",  # extra fields not permitted
        populate_by_name=True,  # fields can be populated using aliases
    )


# Define subset of FHIR data type Pydantic models


class Annotation(FHIRBaseModel):
    text: str
    time: Optional[str] = None


class Coding(FHIRBaseModel):
    system: Optional[str]
    code: Optional[str]
    display: Optional[str]


class CodeableConcept(FHIRBaseModel):
    coding: list[Coding]
    text: Optional[str]

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


class Meta(FHIRBaseModel):
    lastUpdated: Optional[str] = None  # FHIR `instant` data type


class Quantity(FHIRBaseModel):
    value: Optional[float] = None
    unit: Optional[str] = None
    system: Optional[str] = None
    code: Optional[str] = None  # computer-readable unit
    comparator: Optional[Literal["<", "<=", ">=", ">", "="]] = None


class ObservationReferenceRange(FHIRBaseModel):
    low: Optional[Quantity] = None
    high: Optional[Quantity] = None
    text: Optional[str] = None


class Reference(FHIRBaseModel):
    reference: str
