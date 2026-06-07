"""
Patrón Strategy — priorización pluggeable de tareas.

PROBLEMA:
    Cuando un worker tiene varias sub-tareas esperando, ¿cuál atiende
    primero? La respuesta cambia con el negocio: a veces FIFO es justo, a
    veces hay que adelantar lo "crítico", a veces lo que está por vencer
    su SLA. Hardcodear una sola política obliga a tocar (y redeployar) el
    worker cada vez que el criterio cambia.

SOLUCIÓN:
    Encapsular "cómo se ordena" en objetos intercambiables (estrategias)
    que comparten una interfaz. La cola de tareas recibe una estrategia y
    delega en ella la decisión. Cambiar de política es cambiar de objeto
    —incluso en runtime— sin tocar la cola ni el worker (habilita A/B
    testing de políticas de scheduling).

CONTRATO:
    Cada `PrioritizationStrategy` expone `sort_key(task, now)`. La cola
    atiende SIEMPRE la tarea con la clave más chica ("la más urgente").
    Todas las claves incluyen el número de secuencia como desempate, así
    el orden es estable y determinista (nunca dos tareas empatan).

USO TÍPICO:

    queue = PriorityTaskQueue(strategy=PriorityLevelStrategy())
    queue.push(ScheduledTask(payload={...}, priority="high", type="text"))
    queue.push(ScheduledTask(payload={...}, priority="low", type="text"))

    task = queue.pop()        # devuelve la 'high' primero

    queue.set_strategy(FIFOStrategy())   # cambia la política en caliente
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


# ─────────────────────────────────────────────────────
# Niveles de prioridad
# ─────────────────────────────────────────────────────
# Menor número = más urgente (para que la clave "más chica" gane).
PRIORITY_RANK: dict[str, int] = {
    "critical": 0,
    "high": 1,
    "normal": 2,
    "low": 3,
}
DEFAULT_PRIORITY = "normal"

# Orden por tipo de contenido: el texto es el más barato de procesar, así
# que por defecto se adelanta para liberar casos rápido. Configurable.
DEFAULT_TYPE_RANK: dict[str, int] = {"text": 0, "image": 1, "audio": 2}


def priority_rank(priority: str) -> int:
    """Rank numérico de una prioridad; valores desconocidos van al final."""
    return PRIORITY_RANK.get((priority or "").lower(), PRIORITY_RANK["low"] + 1)


# ─────────────────────────────────────────────────────
# Tarea programable
# ─────────────────────────────────────────────────────
def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class ScheduledTask:
    """
    Una sub-tarea esperando ser atendida por un worker.

    Envuelve el `payload` (el mensaje de Kafka, con el formato que define el
    Splitter de A) y agrega la metadata que las estrategias necesitan para
    ordenar.
    """

    payload: dict[str, Any]
    type: str = ""                       # text | image | audio
    priority: str = DEFAULT_PRIORITY     # critical|high|normal|low
    created_at: datetime = field(default_factory=_utcnow)
    deadline: datetime | None = None     # vencimiento de SLA (opcional)
    # Lo asigna la cola al hacer push; desempata de forma estable (FIFO).
    seq: int = field(default=0, compare=False)

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "ScheduledTask":
        """
        Construye una tarea a partir de un payload de Kafka, leyendo
        `type` y `priority` si vienen (campos opcionales del SubTask).
        """
        return cls(
            payload=payload,
            type=str(payload.get("type", "")),
            priority=str(payload.get("priority", DEFAULT_PRIORITY)),
        )

    def age_seconds(self, now: datetime) -> float:
        return max(0.0, (now - self.created_at).total_seconds())


# ─────────────────────────────────────────────────────
# Estrategia (interfaz)
# ─────────────────────────────────────────────────────
class PrioritizationStrategy(ABC):
    """
    Decide el orden de atención. La cola atiende la tarea cuya `sort_key`
    sea la más pequeña.
    """

    name: str = "base"

    @abstractmethod
    def sort_key(self, task: ScheduledTask, *, now: datetime) -> tuple:
        """Clave comparable; menor = se atiende antes."""

    def __repr__(self) -> str:
        return f"<{type(self).__name__} name={self.name!r}>"


# ─────────────────────────────────────────────────────
# Estrategias concretas
# ─────────────────────────────────────────────────────
class FIFOStrategy(PrioritizationStrategy):
    """First-In-First-Out: respeta el orden de llegada. Justo y predecible."""

    name = "fifo"

    def sort_key(self, task: ScheduledTask, *, now: datetime) -> tuple:
        return (task.seq,)


class LIFOStrategy(PrioritizationStrategy):
    """Last-In-First-Out: atiende lo más nuevo primero (pila)."""

    name = "lifo"

    def sort_key(self, task: ScheduledTask, *, now: datetime) -> tuple:
        return (-task.seq,)


class PriorityLevelStrategy(PrioritizationStrategy):
    """
    Por nivel de prioridad (critical > high > normal > low); a igualdad de
    nivel, FIFO. Es la política por defecto en producción.
    """

    name = "priority"

    def sort_key(self, task: ScheduledTask, *, now: datetime) -> tuple:
        return (priority_rank(task.priority), task.seq)


class ContentTypeStrategy(PrioritizationStrategy):
    """
    Por tipo de contenido (configurable); a igualdad, FIFO. Útil para
    adelantar el trabajo más barato (texto) y liberar casos antes.
    """

    name = "content_type"

    def __init__(self, type_rank: dict[str, int] | None = None) -> None:
        self._rank = dict(type_rank or DEFAULT_TYPE_RANK)
        self._fallback = max(self._rank.values(), default=0) + 1

    def sort_key(self, task: ScheduledTask, *, now: datetime) -> tuple:
        return (self._rank.get(task.type, self._fallback), task.seq)


class DeadlineStrategy(PrioritizationStrategy):
    """
    Earliest-Deadline-First: atiende primero lo que vence antes. Las tareas
    sin deadline van al final (orden FIFO entre ellas).
    """

    name = "deadline"

    def sort_key(self, task: ScheduledTask, *, now: datetime) -> tuple:
        if task.deadline is None:
            return (1, 0.0, task.seq)  # grupo "sin deadline", al final
        return (0, task.deadline.timestamp(), task.seq)


class WeightedAgingStrategy(PrioritizationStrategy):
    """
    Combina prioridad con *envejecimiento* para evitar inanición: una tarea
    de baja prioridad que espera mucho va ganando urgencia hasta atenderse.

    score = peso_prioridad - (antigüedad_segundos * aging_per_second)
    La cola usa -score como clave (mayor score = más urgente = clave menor).
    """

    name = "weighted_aging"

    def __init__(
        self,
        *,
        priority_weight: float = 100.0,
        aging_per_second: float = 1.0,
    ) -> None:
        self._priority_weight = priority_weight
        self._aging = aging_per_second

    def sort_key(self, task: ScheduledTask, *, now: datetime) -> tuple:
        # critical aporta más peso base que low.
        base = (PRIORITY_RANK["low"] + 1 - priority_rank(task.priority))
        score = base * self._priority_weight + task.age_seconds(now) * self._aging
        return (-score, task.seq)


# ─────────────────────────────────────────────────────
# Cola con estrategia intercambiable
# ─────────────────────────────────────────────────────
class PriorityTaskQueue:
    """
    Cola de `ScheduledTask` cuyo orden lo decide una estrategia inyectable
    e intercambiable en runtime.

    Implementación: selección O(n) en cada `pop`. Se elige así (en vez de un
    heap) a propósito: muchas estrategias dependen del tiempo actual
    (envejecimiento, deadline), por lo que la prioridad no es estática y un
    heap quedaría desordenado. Para los tamaños de buffer de un worker
    (cientos), O(n) es más que suficiente.
    """

    def __init__(self, strategy: PrioritizationStrategy | None = None) -> None:
        self._strategy: PrioritizationStrategy = strategy or FIFOStrategy()
        self._tasks: list[ScheduledTask] = []
        self._seq = 0

    # ── Mutación ──
    def push(self, task: ScheduledTask) -> ScheduledTask:
        task.seq = self._seq
        self._seq += 1
        self._tasks.append(task)
        return task

    def push_payload(self, payload: dict[str, Any]) -> ScheduledTask:
        """Atajo: arma la tarea desde un payload de Kafka y la encola."""
        return self.push(ScheduledTask.from_payload(payload))

    def pop(self) -> ScheduledTask | None:
        """Saca y devuelve la tarea más urgente según la estrategia."""
        if not self._tasks:
            return None
        idx = self._best_index()
        return self._tasks.pop(idx)

    def peek(self) -> ScheduledTask | None:
        """Mira la próxima tarea sin sacarla."""
        if not self._tasks:
            return None
        return self._tasks[self._best_index()]

    # ── Estrategia ──
    def set_strategy(self, strategy: PrioritizationStrategy) -> None:
        """Cambia la política de priorización en caliente."""
        self._strategy = strategy

    @property
    def strategy(self) -> PrioritizationStrategy:
        return self._strategy

    # ── Inspección ──
    def __len__(self) -> int:
        return len(self._tasks)

    @property
    def is_empty(self) -> bool:
        return not self._tasks

    def snapshot(self) -> list[ScheduledTask]:
        """Copia de las tareas en orden de atención (no destructiva)."""
        now = _utcnow()
        return sorted(
            self._tasks, key=lambda t: self._strategy.sort_key(t, now=now)
        )

    # ── Interno ──
    def _best_index(self) -> int:
        now = _utcnow()
        return min(
            range(len(self._tasks)),
            key=lambda i: self._strategy.sort_key(self._tasks[i], now=now),
        )


# ─────────────────────────────────────────────────────
# Factory por nombre (selección por configuración)
# ─────────────────────────────────────────────────────
_STRATEGIES: dict[str, type[PrioritizationStrategy]] = {
    FIFOStrategy.name: FIFOStrategy,
    LIFOStrategy.name: LIFOStrategy,
    PriorityLevelStrategy.name: PriorityLevelStrategy,
    ContentTypeStrategy.name: ContentTypeStrategy,
    DeadlineStrategy.name: DeadlineStrategy,
    WeightedAgingStrategy.name: WeightedAgingStrategy,
}


def build_strategy(name: str) -> PrioritizationStrategy:
    """
    Crea una estrategia por nombre (ej: leído de una env var), permitiendo
    cambiar la política sin tocar código.
    """
    key = (name or "").lower()
    try:
        return _STRATEGIES[key]()
    except KeyError:
        raise ValueError(
            f"Estrategia desconocida '{name}'. Disponibles: "
            f"{sorted(_STRATEGIES)}"
        ) from None


def available_strategies() -> list[str]:
    return sorted(_STRATEGIES)
