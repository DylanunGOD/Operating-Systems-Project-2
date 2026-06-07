"""
Contratos de datos del paquete de workers.

Concentra en un solo lugar los tipos que cruzan entre el BaseWorker, los
workers especializados, los stores de persistencia y el ConsolidationWorker.
Mantenerlos acá (sin lógica) evita imports circulares entre módulos.

- WorkerTask      → lo que un worker recibe (sub-tarea parseada del payload)
- Finding         → un hallazgo detectado por un worker (futuro Incident)
- WorkerAnalysis  → lo que un worker produce al analizar una tarea
- RecordedResult  → ack de que un resultado se persistió
- ResultRecord    → un resultado ya guardado (lo lee la consolidación)
- CaseInfo        → metadata del caso necesaria para consolidar
- IncidentDraft   → un incidente listo para persistir (lo genera la consolidación)
- ResultStore / ResultReader / IncidentWriter → puertos de persistencia
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from src.models.case import CaseStatus
from src.models.result import IncidentCategory, IncidentSeverity, WorkerType

# Tipos de contenido válidos (coinciden con WorkerType y con el Splitter de A).
CONTENT_TYPES = ("text", "image", "audio")


# ─────────────────────────────────────────────────────
# Entrada de un worker
# ─────────────────────────────────────────────────────
@dataclass(frozen=True)
class WorkerTask:
    """
    Una sub-tarea lista para procesar, parseada del payload de Kafka.

    El formato del payload lo definió A en el Splitter:
        {case_id, type, index, source_file, [priority], [extras...]}
    """

    case_id: str
    content_type: str          # "text" | "image" | "audio"
    index: int
    source_file: str
    priority: str = "normal"
    payload: dict[str, Any] = field(default_factory=dict)

    @property
    def part_id(self) -> str:
        """Identificador de la parte dentro del caso (ej: 'text:0')."""
        return f"{self.content_type}:{self.index}"

    @property
    def worker_type(self) -> WorkerType:
        return WorkerType(self.content_type)

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "WorkerTask":
        case_id = payload.get("case_id")
        content_type = payload.get("type")
        if not case_id:
            raise ValueError("payload sin 'case_id'")
        if content_type not in CONTENT_TYPES:
            raise ValueError(
                f"payload con type inválido: {content_type!r} "
                f"(esperado uno de {CONTENT_TYPES})"
            )
        return cls(
            case_id=str(case_id),
            content_type=str(content_type),
            index=int(payload.get("index", 0)),
            source_file=str(payload.get("source_file", "")),
            priority=str(payload.get("priority", "normal")),
            payload=payload,
        )


# ─────────────────────────────────────────────────────
# Salida de un worker
# ─────────────────────────────────────────────────────
@dataclass
class Finding:
    """
    Un hallazgo relevante detectado al analizar un archivo. La consolidación
    lo convierte en un `Incident` (+ `Evidence`).

    `category` y `severity` se llevan como str (los valores de los enums) para
    no acoplar a los workers con SQLAlchemy; la consolidación los valida.
    """

    category: str               # uno de IncidentCategory
    severity: str               # uno de IncidentSeverity
    title: str
    description: str
    confidence: float = 0.0
    source_file: str = ""
    snippet: str | None = None
    location_data: dict[str, Any] = field(default_factory=dict)

    def normalized_category(self) -> IncidentCategory:
        try:
            return IncidentCategory(self.category)
        except ValueError:
            return IncidentCategory.OTHER

    def normalized_severity(self) -> IncidentSeverity:
        try:
            return IncidentSeverity(self.severity)
        except ValueError:
            return IncidentSeverity.LOW


@dataclass
class WorkerAnalysis:
    """
    Resultado de analizar una sub-tarea. `data` es el JSON flexible que se
    guarda en AnalysisResult.data; `findings` alimenta la consolidación.
    """

    data: dict[str, Any] = field(default_factory=dict)
    findings: list[Finding] = field(default_factory=list)
    confidence_score: float | None = None
    summary: str = ""


# ─────────────────────────────────────────────────────
# Persistencia: acks y lecturas
# ─────────────────────────────────────────────────────
@dataclass(frozen=True)
class RecordedResult:
    """Ack de que un resultado se guardó (lo devuelve el ResultStore)."""

    result_id: str
    case_id: str
    part_id: str


@dataclass(frozen=True)
class ResultRecord:
    """Un resultado ya persistido, tal como lo lee la consolidación."""

    case_id: str
    worker_type: str
    source_file: str
    source_index: int
    data: dict[str, Any]
    confidence_score: float | None = None

    @property
    def findings(self) -> list[dict[str, Any]]:
        raw = self.data.get("findings", [])
        return raw if isinstance(raw, list) else []


@dataclass(frozen=True)
class CaseInfo:
    """Metadata mínima del caso para consolidar."""

    case_id: str
    total_items: int
    status: CaseStatus
    total_text_items: int = 0
    total_image_items: int = 0
    total_audio_items: int = 0


# ─────────────────────────────────────────────────────
# Borradores de la consolidación
# ─────────────────────────────────────────────────────
@dataclass
class EvidenceDraft:
    source_file: str
    snippet: str | None = None
    location_data: dict[str, Any] = field(default_factory=dict)


@dataclass
class IncidentDraft:
    category: IncidentCategory
    severity: IncidentSeverity
    title: str
    description: str
    confidence_score: float = 0.0
    evidences: list[EvidenceDraft] = field(default_factory=list)


# ─────────────────────────────────────────────────────
# Puertos de persistencia (los implementan los stores)
# ─────────────────────────────────────────────────────
@runtime_checkable
class ResultStore(Protocol):
    """Persiste el resultado de un worker y actualiza el progreso del caso."""

    async def record(
        self,
        task: WorkerTask,
        analysis: WorkerAnalysis,
        *,
        processing_time_ms: int,
    ) -> RecordedResult: ...


@runtime_checkable
class ResultReader(Protocol):
    """Lee el caso y sus resultados (lo usa la consolidación)."""

    async def load_case(self, case_id: str) -> CaseInfo | None: ...
    async def load_results(self, case_id: str) -> list[ResultRecord]: ...


@runtime_checkable
class IncidentWriter(Protocol):
    """Escribe los incidentes y cierra el caso (lo usa la consolidación)."""

    async def save_incidents(
        self, case_id: str, incidents: list[IncidentDraft]
    ) -> int: ...

    async def finalize_case(
        self,
        case_id: str,
        status: CaseStatus,
        *,
        error_message: str | None = None,
    ) -> None: ...
