"""
Patrón Factory — creación de workers.

PROBLEMA:
    Hay varios tipos de worker (text, image, audio) y mañana puede haber
    más (video, pdf...). Si el código que arranca los workers tuviera un
    `if tipo == "text": TextWorker(...) elif ...`, agregar un tipo obligaría
    a tocar ese código (y a recordar todos los lugares donde se elige tipo).

SOLUCIÓN:
    Un `WorkerFactory` con un registro `tipo -> constructor`. Crear un worker
    es `factory.create(tipo, ...)`. Agregar un tipo nuevo es
    `factory.register("video", VideoWorker)` —sin tocar el código existente
    (principio Abierto/Cerrado).

EXTRA:
    - Integra el patrón Bulkhead: cada worker recibe su compartimento aislado
      desde un `BulkheadRegistry` compartido.
    - `main()` es el entrypoint ejecutable (lo usan los Dockerfiles de los
      workers: `python -m src.workers <tipo>`).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
from typing import Any, Callable

from src.patterns.bulkhead import Bulkhead, BulkheadRegistry, build_default_registry
from src.workers.audio_worker import AudioWorker
from src.workers.base_worker import BaseWorker
from src.workers.consolidation_worker import ConsolidationWorker
from src.workers.image_worker import ImageWorker
from src.workers.messaging import InMemoryConsumer, MessageConsumer
from src.workers.stores import DatabaseStore
from src.workers.text_worker import TextWorker

logger = logging.getLogger("workers.factory")

# Constructor de un worker de análisis: recibe kwargs y devuelve un BaseWorker.
WorkerBuilder = Callable[..., BaseWorker]


# ─────────────────────────────────────────────────────
# Excepciones
# ─────────────────────────────────────────────────────
class UnknownWorkerTypeError(Exception):
    def __init__(self, content_type: str, available: list[str]) -> None:
        super().__init__(
            f"Tipo de worker desconocido: '{content_type}'. "
            f"Registrados: {available}"
        )


# ─────────────────────────────────────────────────────
# Factory
# ─────────────────────────────────────────────────────
class WorkerFactory:
    """
    Registro de constructores de workers de análisis (text/image/audio/…).

    El ConsolidationWorker tiene otra forma (lee resultados y escribe
    incidentes, no produce AnalysisResult) y se construye con su propio
    helper `create_consolidation_worker`.
    """

    def __init__(self) -> None:
        self._builders: dict[str, WorkerBuilder] = {}

    def register(self, content_type: str, builder: WorkerBuilder) -> "WorkerFactory":
        """Asocia un tipo con su constructor. Sobre-escribible a propósito."""
        self._builders[content_type] = builder
        return self

    def unregister(self, content_type: str) -> None:
        self._builders.pop(content_type, None)

    def is_registered(self, content_type: str) -> bool:
        return content_type in self._builders

    def available_types(self) -> list[str]:
        return sorted(self._builders)

    def create(self, content_type: str, **kwargs: Any) -> BaseWorker:
        try:
            builder = self._builders[content_type]
        except KeyError:
            raise UnknownWorkerTypeError(
                content_type, self.available_types()
            ) from None
        return builder(**kwargs)


def build_default_factory() -> WorkerFactory:
    """Factory con los tres workers estándar del sistema."""
    factory = WorkerFactory()
    factory.register("text", TextWorker)
    factory.register("image", ImageWorker)
    factory.register("audio", AudioWorker)
    return factory


# ─────────────────────────────────────────────────────
# Helpers de alto nivel (cablean Bulkhead + store)
# ─────────────────────────────────────────────────────
def create_worker(
    content_type: str,
    *,
    consumer: MessageConsumer,
    store: Any,
    factory: WorkerFactory | None = None,
    bulkhead_registry: BulkheadRegistry | None = None,
    **kwargs: Any,
) -> BaseWorker:
    """
    Crea un worker de análisis ya cableado con su compartimento Bulkhead.
    """
    factory = factory or build_default_factory()
    registry = bulkhead_registry or build_default_registry()
    bulkhead: Bulkhead = registry.get_or_create(content_type)
    return factory.create(
        content_type, consumer=consumer, store=store, bulkhead=bulkhead, **kwargs
    )


def create_consolidation_worker(
    *,
    consumer: MessageConsumer,
    reader: Any,
    writer: Any,
    **kwargs: Any,
) -> ConsolidationWorker:
    """Crea el ConsolidationWorker (reader+writer suelen ser el mismo store)."""
    return ConsolidationWorker(
        consumer=consumer, reader=reader, writer=writer, **kwargs
    )


# ─────────────────────────────────────────────────────
# Construcción del consumer (con/sin Kafka real de C)
# ─────────────────────────────────────────────────────
def build_consumer(topics: list[str]) -> MessageConsumer:
    """
    Devuelve un consumer real de Kafka si Persona C ya entregó la fábrica en
    `infrastructure/kafka.py`; si no, un InMemoryConsumer (modo dev) con una
    advertencia. Así el worker arranca igual y se conecta a Kafka sin cambios
    cuando la infra esté lista.
    """
    try:
        from src.infrastructure import kafka as kafka_infra  # type: ignore

        builder = getattr(kafka_infra, "build_consumer", None)
        if callable(builder):
            return builder(topics)
    except Exception as e:  # pragma: no cover - depende de infra de C
        logger.warning("No se pudo construir el consumer real de Kafka: %s", e)

    logger.warning(
        "Kafka real pendiente (Persona C). Usando InMemoryConsumer (modo dev) "
        "para topics=%s; no recibirá mensajes reales.",
        topics,
    )
    return InMemoryConsumer(name=f"dev:{','.join(topics)}")


# ─────────────────────────────────────────────────────
# Entrypoint ejecutable
# ─────────────────────────────────────────────────────
def _topics_for(content_type: str) -> list[str]:
    from src.infrastructure.config import get_settings

    settings = get_settings()
    return {
        "text": [settings.kafka_topic_text],
        "image": [settings.kafka_topic_image],
        "audio": [settings.kafka_topic_audio],
        "consolidation": ["analysis.results"],
    }.get(content_type, [])


async def run_worker(content_type: str) -> None:
    """
    Construye y corre un worker del tipo indicado hasta recibir SIGTERM/SIGINT.
    Usado por el CLI / los Dockerfiles.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    topics = _topics_for(content_type)
    consumer = build_consumer(topics)
    store = DatabaseStore()

    if content_type == "consolidation":
        worker: Any = create_consolidation_worker(
            consumer=consumer, reader=store, writer=store
        )
    else:
        worker = create_worker(content_type, consumer=consumer, store=store)

    # Apagado limpio ante señales.
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, worker.stop)
        except NotImplementedError:  # Windows no soporta add_signal_handler
            pass

    logger.info("Arrancando worker '%s' sobre topics %s", content_type, topics)
    try:
        await worker.run_forever()
    finally:
        await consumer.close()


def main(argv: list[str] | None = None) -> None:
    """CLI: `python -m src.workers <tipo>`."""
    parser = argparse.ArgumentParser(description="Lanza un worker de análisis.")
    parser.add_argument(
        "content_type",
        nargs="?",
        default=os.getenv("WORKER_TYPE", "text"),
        choices=["text", "image", "audio", "consolidation"],
        help="Tipo de worker a ejecutar.",
    )
    args = parser.parse_args(argv)
    try:
        asyncio.run(run_worker(args.content_type))
    except KeyboardInterrupt:
        pass
