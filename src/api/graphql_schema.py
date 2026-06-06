"""
Schema GraphQL con Strawberry.

Define los tipos públicos que la API expone + las queries y mutations
disponibles. Los resolvers solo despachan al `CommandBus` / `QueryBus`
(CQRS); la lógica real vive en `src/api/resolvers.py`.

Estructura:
- Enums: re-exposición type-safe de los enums del dominio
- Types: representación GraphQL de Case / Result / Incident / Evidence
- Inputs: payloads para mutations
- Query: GetCase, ListCases
- Mutation: CreateCase
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Optional

import strawberry

from src.models.case import CaseStatus
from src.models.result import (
    IncidentCategory,
    IncidentSeverity,
    WorkerType,
)
from src.patterns.cqrs import command_bus, query_bus


# ─────────────────────────────────────────────────────
# Enums (mapeo directo desde el dominio)
# ─────────────────────────────────────────────────────
@strawberry.enum
class CaseStatusGQL(Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    CONSOLIDATING = "consolidating"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"


@strawberry.enum
class WorkerTypeGQL(Enum):
    TEXT = "text"
    IMAGE = "image"
    AUDIO = "audio"


@strawberry.enum
class IncidentSeverityGQL(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@strawberry.enum
class IncidentCategoryGQL(Enum):
    VIOLENCE = "violence"
    WEAPONS = "weapons"
    HARASSMENT = "harassment"
    THREATS = "threats"
    EXPLICIT_CONTENT = "explicit_content"
    SUSPICIOUS_ACTIVITY = "suspicious_activity"
    OTHER = "other"


# ─────────────────────────────────────────────────────
# Helpers para metadata (JSON arbitrario)
# ─────────────────────────────────────────────────────
JSON = strawberry.scalar(
    Any,
    name="JSON",
    description="JSON arbitrario para metadata y payloads",
    serialize=lambda v: v,
    parse_value=lambda v: v,
)


# ─────────────────────────────────────────────────────
# Object Types
# ─────────────────────────────────────────────────────
@strawberry.type
class EvidenceGQL:
    id: str
    source_file: str
    snippet: Optional[str]
    location_data: JSON


@strawberry.type
class IncidentGQL:
    id: str
    category: IncidentCategoryGQL
    severity: IncidentSeverityGQL
    title: str
    description: str
    confidence_score: float
    detected_at: datetime
    evidences: list[EvidenceGQL]


@strawberry.type
class AnalysisResultGQL:
    id: str
    worker_type: WorkerTypeGQL
    source_file: str
    source_index: int
    processing_time_ms: int
    confidence_score: Optional[float]
    data: JSON
    created_at: datetime


@strawberry.type
class CaseGQL:
    """Vista completa del caso (con results e incidents anidados)."""

    id: str
    user_id: str
    title: str
    description: Optional[str]
    status: CaseStatusGQL
    error_message: Optional[str]

    total_items: int
    processed_items: int
    progress_percentage: float

    case_metadata: JSON

    created_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]

    results: list[AnalysisResultGQL]
    incidents: list[IncidentGQL]


@strawberry.type
class CaseSummaryGQL:
    """Vista resumida para listados."""

    id: str
    user_id: str
    title: str
    status: CaseStatusGQL
    progress_percentage: float
    created_at: datetime
    completed_at: Optional[datetime]


# ─────────────────────────────────────────────────────
# Input Types
# ─────────────────────────────────────────────────────
@strawberry.input
class CreateCaseInput:
    user_id: str
    title: str
    description: Optional[str] = None
    text_items_count: int = 0
    image_items_count: int = 0
    audio_items_count: int = 0
    metadata: Optional[JSON] = None


@strawberry.type
class CreateCaseResult:
    """Lo que devuelve la mutation: id y estado inicial del caso."""

    case_id: str
    status: CaseStatusGQL
    message: str = "Caso encolado correctamente"


# ─────────────────────────────────────────────────────
# Query
# ─────────────────────────────────────────────────────
@strawberry.type
class Query:
    @strawberry.field(description="Obtiene un caso por id.")
    async def case(self, id: str) -> Optional[CaseGQL]:
        from src.api.resolvers import GetCaseByIdQuery

        return await query_bus.dispatch(GetCaseByIdQuery(case_id=id))

    @strawberry.field(description="Lista casos con filtros opcionales.")
    async def cases(
        self,
        user_id: Optional[str] = None,
        status: Optional[CaseStatusGQL] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[CaseSummaryGQL]:
        from src.api.resolvers import ListCasesQuery

        domain_status = (
            CaseStatus(status.value) if status is not None else None
        )
        return await query_bus.dispatch(
            ListCasesQuery(
                user_id=user_id,
                status=domain_status,
                limit=limit,
                offset=offset,
            )
        )

    @strawberry.field(description="Health check trivial del schema GraphQL.")
    def ping(self) -> str:
        return "pong"


# ─────────────────────────────────────────────────────
# Mutation
# ─────────────────────────────────────────────────────
@strawberry.type
class Mutation:
    @strawberry.mutation(
        description="Crea un nuevo caso de análisis y dispara su procesamiento."
    )
    async def create_case(self, input: CreateCaseInput) -> CreateCaseResult:
        from src.api.resolvers import CreateCaseCommand

        return await command_bus.dispatch(
            CreateCaseCommand(
                user_id=input.user_id,
                title=input.title,
                description=input.description,
                text_items_count=input.text_items_count,
                image_items_count=input.image_items_count,
                audio_items_count=input.audio_items_count,
                metadata=input.metadata or {},
            )
        )


# ─────────────────────────────────────────────────────
# Schema final
# ─────────────────────────────────────────────────────
schema = strawberry.Schema(query=Query, mutation=Mutation)
