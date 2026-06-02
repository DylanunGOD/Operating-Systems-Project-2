"""
Schemas Pydantic para validación de entrada/salida en la API.

Separados de los modelos SQLAlchemy (que viven en case.py y result.py)
porque la API expone una forma distinta a la persistencia.
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from src.models.case import CaseStatus
from src.models.result import IncidentCategory, IncidentSeverity, WorkerType


# ─────────────────────────────────────────────────────
# Inputs (Commands)
# ─────────────────────────────────────────────────────
class CreateCaseRequest(BaseModel):
    """Payload para crear un nuevo caso."""

    user_id: str = Field(..., min_length=1, max_length=64)
    title: str = Field(..., min_length=1, max_length=255)
    description: str | None = Field(None, max_length=2000)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ListCasesRequest(BaseModel):
    """Filtros para listar casos."""

    user_id: str | None = None
    status: CaseStatus | None = None
    limit: int = Field(50, ge=1, le=200)
    offset: int = Field(0, ge=0)


# ─────────────────────────────────────────────────────
# Outputs (Queries)
# ─────────────────────────────────────────────────────
class EvidenceResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    source_file: str
    location_data: dict[str, Any]
    snippet: str | None


class IncidentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    category: IncidentCategory
    severity: IncidentSeverity
    title: str
    description: str
    confidence_score: float
    detected_at: datetime
    evidences: list[EvidenceResponse] = []


class AnalysisResultResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    worker_type: WorkerType
    source_file: str
    source_index: int
    data: dict[str, Any]
    processing_time_ms: int
    confidence_score: float | None
    created_at: datetime


class CaseResponse(BaseModel):
    """Vista completa de un caso (con results e incidents)."""

    model_config = ConfigDict(from_attributes=True)

    id: str
    user_id: str
    title: str
    description: str | None
    status: CaseStatus
    error_message: str | None

    total_items: int
    processed_items: int
    progress_percentage: float

    case_metadata: dict[str, Any]

    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None

    results: list[AnalysisResultResponse] = []
    incidents: list[IncidentResponse] = []


class CaseSummary(BaseModel):
    """Vista resumida para listados (sin results/incidents)."""

    model_config = ConfigDict(from_attributes=True)

    id: str
    user_id: str
    title: str
    status: CaseStatus
    progress_percentage: float
    created_at: datetime
    completed_at: datetime | None
