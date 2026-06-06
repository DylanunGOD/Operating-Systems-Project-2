"""
Resolvers — Commands, Queries y sus handlers.

ESTRUCTURA:
- Commands  (escrituras)    → mutaciones GraphQL / endpoints REST
- Queries   (lecturas)      → queries GraphQL
- Handlers                  → la lógica real
- register_handlers()       → wiring de los buses en el startup

Aquí vive el conocimiento del dominio: cómo se crea un caso, cómo se lee,
cómo se lista. El schema GraphQL solo orquesta; este módulo decide.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from src.api.graphql_schema import (
    AnalysisResultGQL,
    CaseGQL,
    CaseStatusGQL,
    CaseSummaryGQL,
    CreateCaseResult,
    EvidenceGQL,
    IncidentCategoryGQL,
    IncidentGQL,
    IncidentSeverityGQL,
    WorkerTypeGQL,
)
from src.database.connection import get_db
from src.models.case import AnalysisCase, CaseStatus
from src.models.result import AnalysisResult, Evidence, Incident
from src.patterns.builder import AnalysisCaseBuilder
from src.patterns.cqrs import (
    Command,
    CommandBus,
    CommandHandler,
    Query,
    QueryBus,
    QueryHandler,
    command_bus,
    query_bus,
)
from src.patterns.outbox import add_outbox_event
from src.patterns.splitter import MessageSplitter


# ═════════════════════════════════════════════════════
# COMMANDS
# ═════════════════════════════════════════════════════
@dataclass
class CreateCaseCommand(Command):
    """Crea un nuevo caso de análisis y dispara su procesamiento."""

    user_id: str
    title: str
    description: Optional[str] = None
    text_items_count: int = 0
    image_items_count: int = 0
    audio_items_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


# ═════════════════════════════════════════════════════
# QUERIES
# ═════════════════════════════════════════════════════
@dataclass
class GetCaseByIdQuery(Query):
    case_id: str


@dataclass
class ListCasesQuery(Query):
    user_id: Optional[str] = None
    status: Optional[CaseStatus] = None
    limit: int = 50
    offset: int = 0


# ═════════════════════════════════════════════════════
# HANDLERS
# ═════════════════════════════════════════════════════
class CreateCaseHandler(CommandHandler[CreateCaseCommand, CreateCaseResult]):
    """
    Crea el caso, lo persiste, genera sub-tareas y encola eventos a la outbox.

    Toda la operación es atómica (una sola transacción). Si algo falla, se
    revierte: ni el caso ni los eventos quedan a medias.
    """

    def __init__(self, splitter: MessageSplitter | None = None) -> None:
        self._splitter = splitter or MessageSplitter()

    async def handle(self, command: CreateCaseCommand) -> CreateCaseResult:
        db = get_db()

        # 1) Construir el caso con el Builder (valida invariantes)
        case = (
            AnalysisCaseBuilder()
            .for_user(command.user_id)
            .with_title(command.title)
            .with_description(command.description)
            .with_text_items(command.text_items_count)
            .with_image_items(command.image_items_count)
            .with_audio_items(command.audio_items_count)
            .with_metadata(command.metadata)
            .build()
        )

        # 2) Persistir caso + outbox events en la misma transacción
        async with db.write_session() as session:
            session.add(case)
            await session.flush()  # obtiene case.id sin commitear aún

            # 2a) Outbox: evento de "case.created" (para auditoría/notificación)
            add_outbox_event(
                session,
                aggregate_id=case.id,
                aggregate_type="case",
                event_type="case.created",
                topic="analysis.cases.events",
                payload={
                    "case_id": case.id,
                    "user_id": case.user_id,
                    "title": case.title,
                    "total_items": case.total_items,
                },
            )

            # 2b) Splitter: dividir en sub-tareas + outbox por cada una
            subtasks = self._splitter.split(
                case_id=case.id,
                text_items=_placeholder_items(
                    command.text_items_count, "text"
                ),
                image_items=_placeholder_items(
                    command.image_items_count, "image"
                ),
                audio_items=_placeholder_items(
                    command.audio_items_count, "audio"
                ),
            )

            for st in subtasks:
                add_outbox_event(
                    session,
                    aggregate_id=case.id,
                    aggregate_type="case",
                    event_type=f"analysis.{st.type}.requested",
                    topic=st.topic,
                    payload=st.payload,
                )

            await session.commit()
            await session.refresh(case)

        return CreateCaseResult(
            case_id=case.id,
            status=CaseStatusGQL(case.status.value),
            message=f"Caso encolado con {len(subtasks)} sub-tareas",
        )


class GetCaseByIdHandler(QueryHandler[GetCaseByIdQuery, Optional[CaseGQL]]):
    """Lee un caso por id desde una replica (en dev cae al primary)."""

    async def handle(self, query: GetCaseByIdQuery) -> Optional[CaseGQL]:
        db = get_db()
        stmt = (
            select(AnalysisCase)
            .where(AnalysisCase.id == query.case_id)
            .options(
                selectinload(AnalysisCase.results),
                selectinload(AnalysisCase.incidents).selectinload(
                    Incident.evidences
                ),
            )
        )

        # En dev solo hay primary; usamos write_session como fallback seguro
        async with db.write_session() as session:
            result = await session.execute(stmt)
            case = result.scalar_one_or_none()
            if case is None:
                return None
            return _to_case_gql(case)


class ListCasesHandler(QueryHandler[ListCasesQuery, list[CaseSummaryGQL]]):
    """Lista casos con filtros opcionales."""

    async def handle(self, query: ListCasesQuery) -> list[CaseSummaryGQL]:
        db = get_db()
        stmt = select(AnalysisCase)

        if query.user_id is not None:
            stmt = stmt.where(AnalysisCase.user_id == query.user_id)
        if query.status is not None:
            stmt = stmt.where(AnalysisCase.status == query.status)

        stmt = (
            stmt.order_by(AnalysisCase.created_at.desc())
            .limit(query.limit)
            .offset(query.offset)
        )

        async with db.write_session() as session:
            result = await session.execute(stmt)
            cases = result.scalars().all()
            return [_to_case_summary_gql(c) for c in cases]


# ═════════════════════════════════════════════════════
# Wiring
# ═════════════════════════════════════════════════════
def register_handlers(
    *,
    cb: CommandBus | None = None,
    qb: QueryBus | None = None,
    splitter: MessageSplitter | None = None,
) -> None:
    """
    Registra todos los handlers en los buses.
    Se llama una vez en el startup de la API (lifespan).

    Buses opcionales: facilita testing con buses dedicados.
    """
    cb = cb or command_bus
    qb = qb or query_bus

    if not cb.is_registered(CreateCaseCommand):
        cb.register(CreateCaseCommand, CreateCaseHandler(splitter=splitter))
    if not qb.is_registered(GetCaseByIdQuery):
        qb.register(GetCaseByIdQuery, GetCaseByIdHandler())
    if not qb.is_registered(ListCasesQuery):
        qb.register(ListCasesQuery, ListCasesHandler())


# ═════════════════════════════════════════════════════
# Helpers internos
# ═════════════════════════════════════════════════════
def _placeholder_items(count: int, type_: str) -> list[dict[str, Any]]:
    """
    Genera N items dummy (sin archivo real) para que el Splitter pueda
    encolar las sub-tareas iniciales.

    Cuando exista el endpoint REST de upload, esos archivos reales
    reemplazarán estos placeholders.
    """
    return [
        {"source_file": f"placeholder/{type_}/{i}", "type": type_}
        for i in range(count)
    ]


# ─────────────────────────────────────────
# Mappers domain → GraphQL
# ─────────────────────────────────────────
def _to_case_gql(case: AnalysisCase) -> CaseGQL:
    return CaseGQL(
        id=case.id,
        user_id=case.user_id,
        title=case.title,
        description=case.description,
        status=CaseStatusGQL(case.status.value),
        error_message=case.error_message,
        total_items=case.total_items,
        processed_items=case.processed_items,
        progress_percentage=case.progress_percentage,
        case_metadata=case.case_metadata or {},
        created_at=case.created_at,
        started_at=case.started_at,
        completed_at=case.completed_at,
        results=[_to_result_gql(r) for r in case.results],
        incidents=[_to_incident_gql(i) for i in case.incidents],
    )


def _to_case_summary_gql(case: AnalysisCase) -> CaseSummaryGQL:
    return CaseSummaryGQL(
        id=case.id,
        user_id=case.user_id,
        title=case.title,
        status=CaseStatusGQL(case.status.value),
        progress_percentage=case.progress_percentage,
        created_at=case.created_at,
        completed_at=case.completed_at,
    )


def _to_result_gql(r: AnalysisResult) -> AnalysisResultGQL:
    return AnalysisResultGQL(
        id=r.id,
        worker_type=WorkerTypeGQL(r.worker_type.value),
        source_file=r.source_file,
        source_index=r.source_index,
        processing_time_ms=r.processing_time_ms,
        confidence_score=r.confidence_score,
        data=r.data or {},
        created_at=r.created_at,
    )


def _to_incident_gql(i: Incident) -> IncidentGQL:
    return IncidentGQL(
        id=i.id,
        category=IncidentCategoryGQL(i.category.value),
        severity=IncidentSeverityGQL(i.severity.value),
        title=i.title,
        description=i.description,
        confidence_score=i.confidence_score,
        detected_at=i.detected_at,
        evidences=[_to_evidence_gql(e) for e in i.evidences],
    )


def _to_evidence_gql(e: Evidence) -> EvidenceGQL:
    return EvidenceGQL(
        id=e.id,
        source_file=e.source_file,
        snippet=e.snippet,
        location_data=e.location_data or {},
    )
