"""
Patrón Outbox — garantizar publicación de eventos a Kafka.

PROBLEMA:
    Si un handler escribe el cambio en BD y luego publica a Kafka, hay un
    instante en que el cambio quedó persistido pero el evento NO se publicó
    (si Kafka cae justo ahí). Resultado: evento perdido para siempre.

SOLUCIÓN:
    El handler escribe el cambio + el evento (en una tabla `outbox`) en la
    *misma transacción* BD. Un poller separado lee la outbox y publica a
    Kafka. Si la publicación falla, reintenta hasta que Kafka responda.

COMPONENTES:
    - add_outbox_event(): inserta un evento en la sesión actual (transaccional)
    - KafkaPublisher (Protocol): contrato que C implementará con Kafka real
    - OutboxPublisher: el poller async que recorre eventos pendientes

USO TÍPICO (en un Command Handler):

    async def handle(self, cmd: CreateCaseCommand) -> str:
        async with db.write_session() as session:
            case = AnalysisCaseBuilder.from_request(cmd.request).build()
            session.add(case)
            await session.flush()       # genera case.id

            add_outbox_event(
                session,
                aggregate_id=case.id,
                aggregate_type="case",
                event_type="case.created",
                topic="analysis.cases.events",
                payload={"case_id": case.id, "user_id": case.user_id},
            )
            await session.commit()      # ← cambio + evento atómicos
            return case.id
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, Protocol

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.connection import get_db
from src.models.outbox import OutboxEvent


# ─────────────────────────────────────────────────────
# Contrato del publicador de Kafka
# ─────────────────────────────────────────────────────
class KafkaPublisher(Protocol):
    """
    Cualquier objeto que sepa publicar a un topic Kafka.

    Persona C implementará la versión real con confluent-kafka.
    Persona A puede usar un fake que imprime en consola.
    """

    async def publish(self, topic: str, payload: dict[str, Any]) -> None: ...


# ─────────────────────────────────────────────────────
# Helper transaccional
# ─────────────────────────────────────────────────────
def add_outbox_event(
    session: AsyncSession,
    *,
    aggregate_id: str,
    aggregate_type: str,
    event_type: str,
    topic: str,
    payload: dict[str, Any],
) -> OutboxEvent:
    """
    Encola un evento dentro de la sesión actual (no hace commit).

    El handler de Command debe llamarlo *después* de hacer `session.add(entity)`
    y *antes* de `session.commit()`. Así el evento se persiste atómicamente
    con el cambio de dominio.
    """
    event = OutboxEvent(
        aggregate_id=aggregate_id,
        aggregate_type=aggregate_type,
        event_type=event_type,
        topic=topic,
        payload=payload,
    )
    session.add(event)
    return event


# ─────────────────────────────────────────────────────
# OutboxPublisher (poller)
# ─────────────────────────────────────────────────────
class OutboxPublisher:
    """
    Servicio que lee eventos pendientes y los publica a Kafka.

    Se ejecuta como tarea async en el lifespan de la API:
        publisher = OutboxPublisher(kafka_publisher=kafka_manager)
        task = asyncio.create_task(publisher.run_forever())

    Estrategia:
    - Cada `interval_seconds` consulta hasta `batch_size` eventos pendientes
    - Los publica a Kafka uno por uno
    - Si publish() lanza excepción → incrementa retry_count y guarda error
    - Si éxito → marca published_at = now()
    """

    def __init__(
        self,
        kafka_publisher: KafkaPublisher,
        *,
        interval_seconds: float = 1.0,
        batch_size: int = 50,
        max_retries: int = 10,
    ) -> None:
        self._kafka = kafka_publisher
        self._interval = interval_seconds
        self._batch_size = batch_size
        self._max_retries = max_retries
        self._stop = asyncio.Event()
        self._running = False

    # ─────────────────────────────────────────
    # Ciclo principal
    # ─────────────────────────────────────────
    async def run_forever(self) -> None:
        """Loop infinito hasta que se llame a stop()."""
        self._running = True
        try:
            while not self._stop.is_set():
                try:
                    await self.publish_pending()
                except Exception as e:  # nunca debe morir el poller
                    print(f"[outbox] error en loop: {type(e).__name__}: {e}")
                # Wait con interrupt por stop()
                try:
                    await asyncio.wait_for(
                        self._stop.wait(), timeout=self._interval
                    )
                except asyncio.TimeoutError:
                    pass
        finally:
            self._running = False

    def stop(self) -> None:
        """Señala al loop que termine en la próxima iteración."""
        self._stop.set()

    @property
    def is_running(self) -> bool:
        return self._running

    # ─────────────────────────────────────────
    # Una iteración
    # ─────────────────────────────────────────
    async def publish_pending(self) -> int:
        """
        Lee un batch de eventos pendientes y los publica.
        Retorna la cantidad publicada con éxito.
        """
        db = get_db()
        async with db.write_session() as session:
            events = await self._fetch_pending(session)
            if not events:
                return 0

            published_count = 0
            for event in events:
                try:
                    await self._kafka.publish(event.topic, event.payload)
                    event.published_at = datetime.utcnow()
                    event.error_message = None
                    published_count += 1
                except Exception as e:
                    event.retry_count += 1
                    event.error_message = f"{type(e).__name__}: {str(e)[:1900]}"

            await session.commit()
            return published_count

    async def _fetch_pending(
        self, session: AsyncSession
    ) -> list[OutboxEvent]:
        """
        Obtiene eventos sin publicar, ordenados por antigüedad.
        Excluye los que excedieron max_retries.
        """
        stmt = (
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None))
            .where(OutboxEvent.retry_count < self._max_retries)
            .order_by(OutboxEvent.created_at.asc())
            .limit(self._batch_size)
        )
        result = await session.execute(stmt)
        return list(result.scalars().all())

    # ─────────────────────────────────────────
    # Mantenimiento
    # ─────────────────────────────────────────
    async def reset_failed(self, aggregate_id: str | None = None) -> int:
        """
        Resetea retry_count de eventos fallidos para que el poller los vuelva
        a intentar. Útil después de arreglar un problema de Kafka.
        """
        db = get_db()
        async with db.write_session() as session:
            stmt = (
                update(OutboxEvent)
                .where(OutboxEvent.published_at.is_(None))
                .where(OutboxEvent.retry_count >= self._max_retries)
                .values(retry_count=0, error_message=None)
            )
            if aggregate_id is not None:
                stmt = stmt.where(OutboxEvent.aggregate_id == aggregate_id)

            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount or 0


# ─────────────────────────────────────────────────────
# Fake publisher para desarrollo local
# ─────────────────────────────────────────────────────
class ConsoleKafkaPublisher:
    """
    Implementación fake que imprime los eventos en consola.

    Persona A la usa durante FASE 1 mientras C no entrega kafka.py real.
    Persona C la reemplaza por KafkaManager real al iniciar FASE 3.
    """

    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, Any]]] = []

    async def publish(self, topic: str, payload: dict[str, Any]) -> None:
        self.published.append((topic, payload))
        print(f"[fake-kafka] -> {topic}: {payload}")
