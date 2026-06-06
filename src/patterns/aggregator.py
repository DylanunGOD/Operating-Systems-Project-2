"""
Patrón Aggregator — consolidación de N resultados parciales en uno solo.

PROBLEMA:
    Un caso se divide en N sub-tareas (Splitter) y cada una termina en un
    worker distinto. Para generar el reporte final hay que **esperar a que
    todas terminen**. Si una se cuelga, no podemos esperar para siempre.

SOLUCIÓN:
    Una `AggregationWindow` que:
    - Sabe cuántas partes esperar (`expected_total`).
    - Recibe partes incrementalmente vía `add_part`.
    - Notifica completitud vía `asyncio.Event` (lo usa `wait`).
    - Tiene un timeout: si no llegan todas a tiempo, se cierra con
      AggregationStatus.TIMEOUT y los datos parciales.

ESCENARIO DE USO (Consolidación):

    aggregator = ResultAggregator(default_timeout_seconds=300)
    aggregator.start_window(case_id, expected_total=6)

    # Cada worker, al terminar, llama:
    await aggregator.add_part(case_id, part_id="text:0", data={...})

    # ConsolidationWorker espera:
    result = await aggregator.wait(case_id)
    if result.is_complete:
        # generar reporte con result.parts (todos los datos)
    elif result.is_timeout:
        # marcar caso como TIMEOUT, generar reporte parcial

EN ESTA FASE:
    Implementación en memoria. Persona C podrá reemplazarla por una
    versión Redis-backed sin tocar consumidores (mismo contrato público).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


# ─────────────────────────────────────────────────────
# Estado y resultados
# ─────────────────────────────────────────────────────
class AggregationStatus(str, Enum):
    OPEN = "open"           # esperando partes
    COMPLETE = "complete"   # llegaron todas
    TIMEOUT = "timeout"     # expiró antes de completarse


@dataclass
class AggregationResult:
    """
    Snapshot inmutable del estado de una ventana, devuelto por `wait`.
    """

    aggregate_id: str
    status: AggregationStatus
    parts: dict[str, dict[str, Any]]
    expected_total: int
    started_at: datetime
    completed_at: datetime | None

    @property
    def received_count(self) -> int:
        return len(self.parts)

    @property
    def missing_count(self) -> int:
        return max(0, self.expected_total - self.received_count)

    @property
    def is_complete(self) -> bool:
        return self.status == AggregationStatus.COMPLETE

    @property
    def is_timeout(self) -> bool:
        return self.status == AggregationStatus.TIMEOUT


# ─────────────────────────────────────────────────────
# Excepciones
# ─────────────────────────────────────────────────────
class WindowNotFoundError(Exception):
    """No hay ventana abierta para el aggregate_id solicitado."""

    def __init__(self, aggregate_id: str) -> None:
        super().__init__(
            f"No hay ventana abierta para aggregate_id='{aggregate_id}'. "
            "Llamá a start_window() primero."
        )


class WindowAlreadyExistsError(Exception):
    """Se intentó abrir dos ventanas para el mismo aggregate_id."""

    def __init__(self, aggregate_id: str) -> None:
        super().__init_(
            f"Ya hay una ventana abierta para '{aggregate_id}'."
        )


# ─────────────────────────────────────────────────────
# Ventana interna
# ─────────────────────────────────────────────────────
@dataclass
class _Window:
    aggregate_id: str
    expected_total: int
    timeout_seconds: float
    parts: dict[str, dict[str, Any]] = field(default_factory=dict)
    started_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    completed_at: datetime | None = None
    completion_event: asyncio.Event = field(default_factory=asyncio.Event)

    @property
    def is_complete(self) -> bool:
        return len(self.parts) >= self.expected_total

    def snapshot(self, status: AggregationStatus) -> AggregationResult:
        return AggregationResult(
            aggregate_id=self.aggregate_id,
            status=status,
            parts=dict(self.parts),
            expected_total=self.expected_total,
            started_at=self.started_at,
            completed_at=self.completed_at,
        )


# ─────────────────────────────────────────────────────
# Aggregator
# ─────────────────────────────────────────────────────
class ResultAggregator:
    """
    Coordinador en memoria de ventanas de agregación.

    Threadsafety: usa asyncio.Lock, válido para múltiples corrutinas
    en el mismo event loop. NO es seguro entre procesos.
    """

    DEFAULT_TIMEOUT_SECONDS = 300.0  # 5 minutos

    def __init__(
        self, default_timeout_seconds: float | None = None
    ) -> None:
        self._default_timeout = (
            default_timeout_seconds or self.DEFAULT_TIMEOUT_SECONDS
        )
        self._windows: dict[str, _Window] = {}
        self._lock = asyncio.Lock()

    # ─────────────────────────────────────────
    # API pública
    # ─────────────────────────────────────────
    async def start_window(
        self,
        aggregate_id: str,
        expected_total: int,
        timeout_seconds: float | None = None,
    ) -> None:
        """
        Abre una nueva ventana de agregación.
        Si `expected_total == 0`, queda completa inmediatamente.
        """
        if expected_total < 0:
            raise ValueError("expected_total no puede ser negativo")

        async with self._lock:
            if aggregate_id in self._windows:
                raise WindowAlreadyExistsError(aggregate_id)

            window = _Window(
                aggregate_id=aggregate_id,
                expected_total=expected_total,
                timeout_seconds=timeout_seconds or self._default_timeout,
            )
            if expected_total == 0:
                window.completed_at = datetime.now(timezone.utc)
                window.completion_event.set()
            self._windows[aggregate_id] = window

    async def add_part(
        self,
        aggregate_id: str,
        part_id: str,
        data: dict[str, Any],
    ) -> bool:
        """
        Registra el resultado de un part. Devuelve True si esta parte
        completó la ventana (señal para el waiter).
        Si la parte ya fue registrada, se pisa el valor anterior (idempotente).
        """
        async with self._lock:
            window = self._windows.get(aggregate_id)
            if window is None:
                raise WindowNotFoundError(aggregate_id)

            window.parts[part_id] = data

            if window.is_complete and not window.completion_event.is_set():
                window.completed_at = datetime.now(timezone.utc)
                window.completion_event.set()
                return True
            return False

    async def wait(
        self,
        aggregate_id: str,
        timeout_seconds: float | None = None,
    ) -> AggregationResult:
        """
        Espera a que la ventana se complete o expire.
        Devuelve el snapshot final (no destructivo: la ventana sigue abierta
        hasta que se llame a `discard`).
        """
        async with self._lock:
            window = self._windows.get(aggregate_id)
            if window is None:
                raise WindowNotFoundError(aggregate_id)

        effective_timeout = timeout_seconds or window.timeout_seconds

        try:
            await asyncio.wait_for(
                window.completion_event.wait(),
                timeout=effective_timeout,
            )
        except asyncio.TimeoutError:
            return window.snapshot(AggregationStatus.TIMEOUT)

        return window.snapshot(AggregationStatus.COMPLETE)

    async def discard(self, aggregate_id: str) -> None:
        """Elimina la ventana (libera memoria). Idempotente."""
        async with self._lock:
            self._windows.pop(aggregate_id, None)

    # ─────────────────────────────────────────
    # Inspección
    # ─────────────────────────────────────────
    def has_window(self, aggregate_id: str) -> bool:
        return aggregate_id in self._windows

    def snapshot(self, aggregate_id: str) -> AggregationResult | None:
        """Snapshot en caliente, sin esperar."""
        window = self._windows.get(aggregate_id)
        if window is None:
            return None
        status = (
            AggregationStatus.COMPLETE
            if window.is_complete
            else AggregationStatus.OPEN
        )
        return window.snapshot(status)

    @property
    def active_windows_count(self) -> int:
        return len(self._windows)


# ─────────────────────────────────────────────────────
# Singleton del proyecto
# ─────────────────────────────────────────────────────
# Una instancia compartida por la API. El ConsolidationWorker
# (FASE 2) usará la suya propia o esta misma según diseño.
result_aggregator = ResultAggregator()
