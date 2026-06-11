"""
ConsolidationWorker — consolida los resultados de un caso en un reporte.

ROL EN EL SISTEMA:
    Es el último eslabón del procesamiento. Mientras los workers de
    texto/imagen/audio analizan sub-tareas en paralelo, este worker espera
    a que TODAS terminen (o a que venza un timeout) y produce el reporte:
    agrupa los hallazgos (`Finding`) en incidentes (`Incident` + `Evidence`)
    y cierra el caso.

CÓMO ESPERA (patrón Aggregator de A):
    Consume eventos `analysis.<tipo>.completed` (uno por resultado guardado,
    emitidos vía Outbox por los workers). Por cada caso abre una *ventana*
    en el `ResultAggregator` dimensionada por `total_items`. Cada evento
    suma una parte. Cuando la ventana se completa —o expira— consolida.

    El aggregator es en memoria y por proceso: este worker es el dueño de su
    ventana y la alimenta con lo que consume. (Persona C puede cambiarlo por
    uno respaldado en Redis sin tocar este worker: mismo contrato.)

DECISIÓN DE ESTADO FINAL:
    - ventana COMPLETE  → caso `completed`
    - ventana TIMEOUT   → caso `timeout` (se consolidan los resultados parciales)
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from src.models.case import CaseStatus
from src.models.result import IncidentCategory, IncidentSeverity
from src.patterns.aggregator import ResultAggregator
from src.workers.contracts import (
    EvidenceDraft,
    IncidentDraft,
    IncidentWriter,
    ResultReader,
    ResultRecord,
)
from src.workers.messaging import ConsumedMessage, MessageConsumer

logger = logging.getLogger("workers.consolidation")

# Orden de severidad para elegir la "peor" al agrupar.
_SEVERITY_ORDER = {
    IncidentSeverity.LOW: 0,
    IncidentSeverity.MEDIUM: 1,
    IncidentSeverity.HIGH: 2,
    IncidentSeverity.CRITICAL: 3,
}


# ─────────────────────────────────────────────────────
# Agregación de hallazgos → incidentes (función pura, testeable)
# ─────────────────────────────────────────────────────
def _coerce_category(value: Any) -> IncidentCategory:
    try:
        return IncidentCategory(value)
    except ValueError:
        return IncidentCategory.OTHER


def _coerce_severity(value: Any) -> IncidentSeverity:
    try:
        return IncidentSeverity(value)
    except ValueError:
        return IncidentSeverity.LOW


def build_incidents(records: list[ResultRecord]) -> list[IncidentDraft]:
    """
    Agrupa los `findings` de todos los resultados por categoría y produce un
    incidente por categoría: severidad = la más alta observada, confianza =
    la máxima, y cada finding aporta una evidencia. Ordena por severidad desc.
    """
    grouped: dict[IncidentCategory, list[dict[str, Any]]] = {}
    for record in records:
        for finding in record.findings:
            category = _coerce_category(finding.get("category"))
            grouped.setdefault(category, []).append(finding)

    incidents: list[IncidentDraft] = []
    for category, findings in grouped.items():
        severity = max(
            (_coerce_severity(f.get("severity")) for f in findings),
            key=lambda s: _SEVERITY_ORDER[s],
        )
        confidence = max(
            (float(f.get("confidence", 0.0)) for f in findings), default=0.0
        )
        evidences = [
            EvidenceDraft(
                source_file=str(f.get("source_file", "")),
                snippet=f.get("snippet"),
                location_data=f.get("location_data") or {},
            )
            for f in findings
        ]
        incidents.append(
            IncidentDraft(
                category=category,
                severity=severity,
                title=f"{len(findings)} indicio(s) de {category.value}",
                description=(
                    f"Se consolidaron {len(findings)} hallazgo(s) de tipo "
                    f"'{category.value}' a partir de los resultados del caso."
                ),
                confidence_score=round(confidence, 3),
                evidences=evidences,
            )
        )

    incidents.sort(key=lambda inc: _SEVERITY_ORDER[inc.severity], reverse=True)
    return incidents


# ─────────────────────────────────────────────────────
# ConsolidationWorker
# ─────────────────────────────────────────────────────
class ConsolidationWorker:
    """
    Consume eventos de resultados, espera la completitud de cada caso con el
    Aggregator y persiste el reporte consolidado.
    """

    def __init__(
        self,
        *,
        consumer: MessageConsumer,
        reader: ResultReader,
        writer: IncidentWriter,
        aggregator: ResultAggregator | None = None,
        timeout_seconds: float = 300.0,
        poll_timeout: float = 1.0,
        name: str = "consolidation-worker",
    ) -> None:
        self._consumer = consumer
        self._reader = reader
        self._writer = writer
        self._aggregator = aggregator or ResultAggregator(
            default_timeout_seconds=timeout_seconds
        )
        self._timeout = timeout_seconds
        self._poll_timeout = poll_timeout
        self._name = name

        self._stop = asyncio.Event()
        self._running = False
        self._ensure_lock = asyncio.Lock()
        self._waiters: dict[str, asyncio.Task[None]] = {}

        self._consumed = 0
        self._consolidated = 0

    # ─────────────────────────────────────────
    # Loop de consumo
    # ─────────────────────────────────────────
    async def run_forever(self) -> None:
        self._running = True
        logger.info("[%s] iniciado", self._name)
        try:
            while not self._stop.is_set():
                message = await self._consumer.poll(self._poll_timeout)
                if message is None:
                    continue
                self._consumed += 1
                try:
                    await self.process_message(message)
                    await self._consumer.commit(message)
                except Exception as e:
                    logger.error(
                        "[%s] error en evento: %s: %s",
                        self._name, type(e).__name__, e,
                    )
                    await self._consumer.commit(message)
        finally:
            self._running = False
            await self._drain_waiters()
            logger.info("[%s] detenido", self._name)

    def stop(self) -> None:
        self._stop.set()

    # ─────────────────────────────────────────
    # Manejo de eventos
    # ─────────────────────────────────────────
    async def process_message(self, message: ConsumedMessage) -> None:
        await self.process_event(message.payload)

    async def process_event(self, event: dict[str, Any]) -> None:
        """
        Procesa un evento `analysis.<tipo>.completed`: abre la ventana del
        caso si es nueva y suma esta parte.
        """
        case_id = event.get("case_id")
        part_id = event.get("part_id")
        if not case_id or not part_id:
            logger.warning("[%s] evento sin case_id/part_id: %s", self._name, event)
            return

        opened = await self._ensure_window(case_id)
        if not opened:
            return  # caso desconocido: no se pudo dimensionar la ventana

        await self._aggregator.add_part(
            case_id,
            part_id,
            {"result_id": event.get("result_id"), "type": event.get("type")},
        )

    async def _ensure_window(self, case_id: str) -> bool:
        """
        Garantiza que exista una ventana para el caso. Devuelve False si el
        caso no se encontró (no se puede saber cuántas partes esperar).
        """
        async with self._ensure_lock:
            if self._aggregator.has_window(case_id):
                return True

            case = await self._reader.load_case(case_id)
            if case is None or case.total_items <= 0:
                logger.warning(
                    "[%s] no se pudo dimensionar el caso '%s' (total=%s)",
                    self._name, case_id, getattr(case, "total_items", None),
                )
                return False

            await self._aggregator.start_window(
                case_id,
                expected_total=case.total_items,
                timeout_seconds=self._timeout,
            )
            # Lanzar el waiter que consolida cuando complete o expire.
            self._waiters[case_id] = asyncio.create_task(
                self._consolidate_when_ready(case_id)
            )
            return True

    # ─────────────────────────────────────────
    # Consolidación
    # ─────────────────────────────────────────
    async def _consolidate_when_ready(self, case_id: str) -> None:
        try:
            result = await self._aggregator.wait(case_id)
            await self._consolidate(case_id, complete=result.is_complete)
        except Exception as e:
            logger.error(
                "[%s] fallo consolidando '%s': %s: %s",
                self._name, case_id, type(e).__name__, e,
            )
            await self._safe_fail(case_id, str(e))
        finally:
            await self._aggregator.discard(case_id)
            self._waiters.pop(case_id, None)

    async def consolidate_case(self, case_id: str, *, complete: bool = True) -> int:
        """
        Consolida un caso ya sea por completitud o por timeout. Público para
        que la API/tests lo disparen sin pasar por el loop. Devuelve cuántos
        incidentes generó.
        """
        return await self._consolidate(case_id, complete=complete)

    async def _consolidate(self, case_id: str, *, complete: bool) -> int:
        records = await self._reader.load_results(case_id)
        incidents = build_incidents(records)
        await self._writer.save_incidents(case_id, incidents)

        status = CaseStatus.COMPLETED if complete else CaseStatus.TIMEOUT
        error = None if complete else "Timeout esperando resultados de workers"
        await self._writer.finalize_case(case_id, status, error_message=error)

        self._consolidated += 1
        logger.info(
            "[%s] caso '%s' %s con %d incidente(s) sobre %d resultado(s)",
            self._name, case_id, status.value, len(incidents), len(records),
        )
        return len(incidents)

    async def _safe_fail(self, case_id: str, reason: str) -> None:
        try:
            await self._writer.finalize_case(
                case_id, CaseStatus.FAILED, error_message=reason[:1900]
            )
        except Exception:
            pass

    async def _drain_waiters(self) -> None:
        """Al detener, espera a los waiters en vuelo (no los deja colgados)."""
        for task in list(self._waiters.values()):
            task.cancel()
        for task in list(self._waiters.values()):
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._waiters.clear()

    # ─────────────────────────────────────────
    # Inspección
    # ─────────────────────────────────────────
    @property
    def is_running(self) -> bool:
        return self._running

    def stats(self) -> dict[str, int]:
        return {
            "consumed": self._consumed,
            "consolidated": self._consolidated,
            "active_windows": self._aggregator.active_windows_count,
        }
