"""
Stores de persistencia para los workers.

Implementan los puertos definidos en `contracts.py`:
    - ResultStore     → guardar resultados + avanzar progreso del caso
    - ResultReader    → leer caso y resultados (consolidación)
    - IncidentWriter  → escribir incidentes + cerrar el caso (consolidación)

Hay dos backends:
    - DatabaseStore: PostgreSQL real. Respeta el Outbox Pattern de A: el
      resultado y el evento `analysis.<tipo>.completed` se escriben en la
      MISMA transacción → cero pérdida de eventos hacia la consolidación.
    - InMemoryStore: en memoria, para dev/tests sin BD ni Kafka. Puede
      alimentar un InMemoryConsumer para simular el salto outbox→Kafka.

Persona C, en FASE 3, sólo conecta el OutboxPublisher real y el Kafka real;
estos stores no cambian.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import select, update

from src.database.connection import get_db
from src.models.case import AnalysisCase, CaseStatus
from src.models.result import (
    AnalysisResult,
    Evidence,
    Incident,
    WorkerType,
)
from src.patterns.outbox import add_outbox_event
from src.workers.contracts import (
    CaseInfo,
    IncidentDraft,
    RecordedResult,
    ResultRecord,
    WorkerAnalysis,
    WorkerTask,
)
from src.workers.messaging import InMemoryConsumer

DEFAULT_RESULTS_TOPIC = "analysis.results"

# Columna de progreso por tipo de contenido.
_PROGRESS_COLUMN = {
    "text": "processed_text_items",
    "image": "processed_image_items",
    "audio": "processed_audio_items",
}


# ─────────────────────────────────────────────────────
# Serialización de un análisis a JSON persistible
# ─────────────────────────────────────────────────────
def serialize_analysis(analysis: WorkerAnalysis) -> dict[str, Any]:
    """Aplana un WorkerAnalysis al dict que va en AnalysisResult.data."""
    data = dict(analysis.data)
    data["summary"] = analysis.summary
    data["findings"] = [
        {
            "category": f.category,
            "severity": f.severity,
            "title": f.title,
            "description": f.description,
            "confidence": f.confidence,
            "source_file": f.source_file,
            "snippet": f.snippet,
            "location_data": f.location_data,
        }
        for f in analysis.findings
    ]
    return data


def _completion_payload(task: WorkerTask, result_id: str) -> dict[str, Any]:
    """Evento que dispara la consolidación cuando un resultado está listo."""
    return {
        "case_id": task.case_id,
        "part_id": task.part_id,
        "type": task.content_type,
        "index": task.index,
        "result_id": result_id,
    }


# ─────────────────────────────────────────────────────
# DatabaseStore (PostgreSQL)
# ─────────────────────────────────────────────────────
class DatabaseStore:
    """
    Gateway de persistencia contra PostgreSQL. Una sola instancia sirve a
    los workers de análisis (como ResultStore) y al ConsolidationWorker
    (como ResultReader + IncidentWriter).
    """

    def __init__(self, *, results_topic: str = DEFAULT_RESULTS_TOPIC) -> None:
        self._results_topic = results_topic

    # ── ResultStore ──────────────────────────────────
    async def record(
        self,
        task: WorkerTask,
        analysis: WorkerAnalysis,
        *,
        processing_time_ms: int,
    ) -> RecordedResult:
        db = get_db()
        result_id = str(uuid4())

        async with db.write_session() as session:
            # 1) Guardar el resultado del worker.
            session.add(
                AnalysisResult(
                    id=result_id,
                    case_id=task.case_id,
                    worker_type=WorkerType(task.content_type),
                    source_file=task.source_file,
                    source_index=task.index,
                    data=serialize_analysis(analysis),
                    processing_time_ms=processing_time_ms,
                    confidence_score=analysis.confidence_score,
                )
            )

            # 2) Avanzar el contador de progreso del tipo correspondiente.
            column = getattr(AnalysisCase, _PROGRESS_COLUMN[task.content_type])
            await session.execute(
                update(AnalysisCase)
                .where(AnalysisCase.id == task.case_id)
                .values({column: column + 1})
            )

            # 3) Transición QUEUED → PROCESSING en el primer resultado.
            await session.execute(
                update(AnalysisCase)
                .where(
                    AnalysisCase.id == task.case_id,
                    AnalysisCase.status == CaseStatus.QUEUED,
                )
                .values(status=CaseStatus.PROCESSING, started_at=datetime.utcnow())
            )

            # 4) Outbox: avisar a la consolidación (misma transacción).
            add_outbox_event(
                session,
                aggregate_id=task.case_id,
                aggregate_type="result",
                event_type=f"analysis.{task.content_type}.completed",
                topic=self._results_topic,
                payload=_completion_payload(task, result_id),
            )

            await session.commit()

        return RecordedResult(
            result_id=result_id, case_id=task.case_id, part_id=task.part_id
        )

    # ── ResultReader ─────────────────────────────────
    async def load_case(self, case_id: str) -> CaseInfo | None:
        db = get_db()
        async with db.write_session() as session:
            case = await session.get(AnalysisCase, case_id)
            if case is None:
                return None
            return CaseInfo(
                case_id=case.id,
                total_items=case.total_items,
                status=case.status,
                total_text_items=case.total_text_items,
                total_image_items=case.total_image_items,
                total_audio_items=case.total_audio_items,
            )

    async def load_results(self, case_id: str) -> list[ResultRecord]:
        db = get_db()
        stmt = select(AnalysisResult).where(AnalysisResult.case_id == case_id)
        async with db.write_session() as session:
            rows = (await session.execute(stmt)).scalars().all()
            return [
                ResultRecord(
                    case_id=r.case_id,
                    worker_type=r.worker_type.value,
                    source_file=r.source_file,
                    source_index=r.source_index,
                    data=r.data or {},
                    confidence_score=r.confidence_score,
                )
                for r in rows
            ]

    # ── IncidentWriter ───────────────────────────────
    async def save_incidents(
        self, case_id: str, incidents: list[IncidentDraft]
    ) -> int:
        if not incidents:
            return 0
        db = get_db()
        async with db.write_session() as session:
            for draft in incidents:
                incident = Incident(
                    case_id=case_id,
                    category=draft.category,
                    severity=draft.severity,
                    title=draft.title,
                    description=draft.description,
                    confidence_score=draft.confidence_score,
                )
                incident.evidences = [
                    Evidence(
                        source_file=ev.source_file,
                        snippet=ev.snippet,
                        location_data=ev.location_data,
                    )
                    for ev in draft.evidences
                ]
                session.add(incident)
            await session.commit()
        return len(incidents)

    async def finalize_case(
        self,
        case_id: str,
        status: CaseStatus,
        *,
        error_message: str | None = None,
    ) -> None:
        db = get_db()
        completed_at = (
            datetime.utcnow()
            if status
            in {CaseStatus.COMPLETED, CaseStatus.FAILED, CaseStatus.TIMEOUT}
            else None
        )
        async with db.write_session() as session:
            await session.execute(
                update(AnalysisCase)
                .where(AnalysisCase.id == case_id)
                .values(
                    status=status,
                    error_message=error_message,
                    completed_at=completed_at,
                )
            )
            await session.commit()


# ─────────────────────────────────────────────────────
# InMemoryStore (dev / tests)
# ─────────────────────────────────────────────────────
class InMemoryStore:
    """
    Backend en memoria. Guarda resultados e incidentes en diccionarios y,
    opcionalmente, alimenta un InMemoryConsumer con los eventos de
    completitud (simulando el salto outbox → Kafka hacia la consolidación).
    """

    def __init__(
        self,
        *,
        results_sink: InMemoryConsumer | None = None,
        results_topic: str = DEFAULT_RESULTS_TOPIC,
    ) -> None:
        self._results_sink = results_sink
        self._results_topic = results_topic
        self.cases: dict[str, dict[str, Any]] = {}
        self.results: dict[str, list[ResultRecord]] = {}
        self.incidents: dict[str, list[IncidentDraft]] = {}
        self.finalized: dict[str, tuple[CaseStatus, str | None]] = {}

    # ── Seeding (tests) ──
    def seed_case(
        self,
        case_id: str,
        *,
        text: int = 0,
        image: int = 0,
        audio: int = 0,
        status: CaseStatus = CaseStatus.QUEUED,
    ) -> None:
        self.cases[case_id] = {
            "total_text_items": text,
            "total_image_items": image,
            "total_audio_items": audio,
            "processed_text_items": 0,
            "processed_image_items": 0,
            "processed_audio_items": 0,
            "status": status,
        }
        self.results.setdefault(case_id, [])

    # ── ResultStore ──
    async def record(
        self,
        task: WorkerTask,
        analysis: WorkerAnalysis,
        *,
        processing_time_ms: int,
    ) -> RecordedResult:
        result_id = str(uuid4())
        record = ResultRecord(
            case_id=task.case_id,
            worker_type=task.content_type,
            source_file=task.source_file,
            source_index=task.index,
            data=serialize_analysis(analysis),
            confidence_score=analysis.confidence_score,
        )
        self.results.setdefault(task.case_id, []).append(record)

        case = self.cases.setdefault(
            task.case_id,
            {
                "status": CaseStatus.QUEUED,
                "total_text_items": 0,
                "total_image_items": 0,
                "total_audio_items": 0,
                "processed_text_items": 0,
                "processed_image_items": 0,
                "processed_audio_items": 0,
            },
        )
        case[_PROGRESS_COLUMN[task.content_type]] += 1
        if case["status"] == CaseStatus.QUEUED:
            case["status"] = CaseStatus.PROCESSING

        # Simular el evento de completitud hacia la consolidación.
        if self._results_sink is not None:
            self._results_sink.feed_nowait(
                self._results_topic, _completion_payload(task, result_id)
            )

        return RecordedResult(
            result_id=result_id, case_id=task.case_id, part_id=task.part_id
        )

    # ── ResultReader ──
    async def load_case(self, case_id: str) -> CaseInfo | None:
        case = self.cases.get(case_id)
        if case is None:
            return None
        total = (
            case["total_text_items"]
            + case["total_image_items"]
            + case["total_audio_items"]
        )
        return CaseInfo(
            case_id=case_id,
            total_items=total,
            status=case["status"],
            total_text_items=case["total_text_items"],
            total_image_items=case["total_image_items"],
            total_audio_items=case["total_audio_items"],
        )

    async def load_results(self, case_id: str) -> list[ResultRecord]:
        return list(self.results.get(case_id, []))

    # ── IncidentWriter ──
    async def save_incidents(
        self, case_id: str, incidents: list[IncidentDraft]
    ) -> int:
        self.incidents.setdefault(case_id, []).extend(incidents)
        return len(incidents)

    async def finalize_case(
        self,
        case_id: str,
        status: CaseStatus,
        *,
        error_message: str | None = None,
    ) -> None:
        self.finalized[case_id] = (status, error_message)
        if case_id in self.cases:
            self.cases[case_id]["status"] = status
