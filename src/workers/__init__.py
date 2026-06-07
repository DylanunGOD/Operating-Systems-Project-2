"""
Paquete de workers (FASE 2 — Persona B).

Capa de procesamiento distribuido: consume sub-tareas de Kafka, las analiza
en paralelo (texto/imagen/audio) y consolida los resultados en reportes.

Patrones aplicados aquí: Factory (worker_factory), Bulkhead, Object Pool,
Strategy y Decorator (en src/patterns), más el Aggregator de Persona A.

API pública de conveniencia (también se puede importar por módulo):
"""

from src.workers.audio_worker import AudioWorker
from src.workers.base_worker import BaseWorker, ProcessOutcome
from src.workers.consolidation_worker import ConsolidationWorker, build_incidents
from src.workers.contracts import (
    Finding,
    RecordedResult,
    ResultRecord,
    WorkerAnalysis,
    WorkerTask,
)
from src.workers.image_worker import ImageWorker
from src.workers.messaging import (
    ConsumedMessage,
    InMemoryConsumer,
    MessageConsumer,
)
from src.workers.stores import DatabaseStore, InMemoryStore
from src.workers.text_worker import TextWorker
from src.workers.worker_factory import (
    WorkerFactory,
    build_default_factory,
    create_consolidation_worker,
    create_worker,
)

__all__ = [
    # Workers
    "BaseWorker",
    "TextWorker",
    "ImageWorker",
    "AudioWorker",
    "ConsolidationWorker",
    "ProcessOutcome",
    "build_incidents",
    # Factory
    "WorkerFactory",
    "build_default_factory",
    "create_worker",
    "create_consolidation_worker",
    # Contratos
    "WorkerTask",
    "WorkerAnalysis",
    "Finding",
    "RecordedResult",
    "ResultRecord",
    # Mensajería
    "MessageConsumer",
    "ConsumedMessage",
    "InMemoryConsumer",
    # Stores
    "DatabaseStore",
    "InMemoryStore",
]
