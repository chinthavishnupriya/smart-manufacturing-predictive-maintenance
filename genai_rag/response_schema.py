"""Validated response contract for maintenance RAG answers."""

from pydantic import BaseModel, Field


class MaintenanceAnswer(BaseModel):
    summary: str = Field(description="Evidence-supported answer to the question")
    possible_causes: list[str] = Field(
        description="Supported possibilities only; do not present them as confirmed diagnoses"
    )
    recommended_checks: list[str] = Field(
        description="Checks explicitly supported by retrieved evidence"
    )
    evidence_sections: list[str] = Field(
        description="Section titles from the supplied retrieved passages"
    )
    limitations: list[str] = Field(
        description="Missing evidence, uncertainty, and relevant safety limitations"
    )
