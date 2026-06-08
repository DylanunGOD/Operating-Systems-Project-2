"""
BaseWorker — esqueleto común de todos los workers de análisis.

RESPONSABILIDADES (lo que comparten Text/Image/Audio):
    1. Consumir mensajes de un topic (vía MessageConsumer).
    2. Parsear el payload a un WorkerTask (formato del Splitter de A).
    3. Ejecutar el análisis especializado DENTRO de un Bulkhead, de modo que
       una saturación de un tipo de worker no tumbe a los demás, y que el
       trabajo CPU-bound (modelos ML) no bloquee el event loop.
    4. Envolver ese análisis en el pipeline de Decorators (logging, métricas,
       retry, timing) sin duplicar ese andamiaje en cada worker.
    5. Persistir el resultado y avanzar el progreso del caso (ResultStore),
       lo que de paso emite —vía Outbox— el evento que despierta a la
       consolidación.

Lo ÚNICO que cada worker concreto implementa es `analyze(task)`: la lógica
de su dominio (NER, detección de objetos, transcripción). Puede ser síncrona
(recomendado para modelos ML: el Bulkhead la descarga a un hilo) o async.

USO:

    worker = TextWorker(consumer=consumer, store=store)
    await worker.run_forever()      # loop hasta worker.stop()
    # o, para una sola tarea (tests):
    outcome = await worker.process_message(message)
"""

from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import ClassVar

from src.models.result import WorkerType
from src.patterns.bulkhead import Bulkhead
from src.patterns.decorators import (
    FunctionProcessor,
    MetricsRecorder,
    NoopMetrics,
    PipelineOptions,
    Processor,
    build_pipeline,
)
from src.workers.contracts import (
    RecordedResult,
    ResultStore,
    WorkerAnalysis,
    WorkerTask,
)
from src.workers.messaging import ConsumedMessage, MessageConsumer

logger = logging.getLogger("workers")


# ─────────────────────────────────────────────────────
# Resultado de procesar un mensaje
# ─────────────────────────────────────────────────────
@dataclass(frozen=True)
class ProcessOutcome:
    status: str                      # "ok" | "failed" | "skipped"
    task: WorkerTask | None = None
    result: RecordedResult | None = None
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.status == "ok"


# ─────────────────────────────────────────────────────
# BaseWorker
# ─────────────────────────────────────────────────────
class BaseWorker(ABC):
    """
    Base abstracta. Las subclases definen `content_type` y `analyze`.
    """

    #: "text" | "image" | "audio" — lo fija cada subclase.
    content_type: ClassVar[str] = ""

    def __init__(
        self,
        *,
        consumer: MessageConsumer,
        store: ResultStore,
        bulkhead: Bulkhead | None = None,
        metrics: MetricsRecorder | None = None,
        poll_timeout: float = 1.0,
        name: str | None = None,
        pipeline_options: PipelineOptions | None = None,
    ) -> None:
        if not self.content_type:
            raise TypeError(
                f"{type(self).__name__} debe definir content_type "
                "('text'|'image'|'audio')."
            )

        self._consumer = consumer
        self._store = store
        self._bulkhead = bulkhead or Bulkhead(
            self.content_type, max_concurrency=4
        )
        self._metrics = metrics or NoopMetrics()
        self._poll_timeout = poll_timeout
        self._name = name or f"{self.content_type}-worker"

        # Pipeline de decoradores alrededor del análisis especializado.
        options = pipeline_options or PipelineOptions(
            metrics=self._metrics,
            describe=self._describe_task,
            tags={"worker": self.content_type},
        )
        self._pipeline: Processor[WorkerTask, WorkerAnalysis] = build_pipeline(
            FunctionProcessor(self._run_core, name=self._name), options
        )

        self._stop = asyncio.Event()
        self._running = False

        # Contadores propios (livianos, no crecen sin límite).
        self._consumed = 0
        self._ok = 0
        self._failed = 0

    # ─────────────────────────────────────────
    # A implementar por cada worker
    # ─────────────────────────────────────────
    @abstractmethod
    def analyze(self, task: WorkerTask) -> WorkerAnalysis:
        """
        Analiza la sub-tarea y devuelve un WorkerAnalysis.

        Puede ser `def` (síncrona, ideal para modelos ML: el Bulkhead la
        corre en un hilo) o `async def`. El BaseWorker se adapta a ambas.
        """

    async def on_start(self) -> None:
        """Hook opcional: cargar modelos, calentar pools, etc."""

    async def on_stop(self) -> None:
        """Hook opcional: liberar recursos."""

    # ─────────────────────────────────────────
    # Núcleo: análisis bajo el Bulkhead
    # ─────────────────────────────────────────
    async def _run_core(self, task: WorkerTask) -> WorkerAnalysis:
        # El Bulkhead aísla la concurrencia y descarga a hilo si analyze es
        # síncrona (CPU-bound). El pipeline (logging/retry/métricas) lo envuelve.
        return await self._bulkhead.run(self.analyze, task)

    # ─────────────────────────────────────────
    # Procesamiento de un mensaje
    # ─────────────────────────────────────────
    def parse_task(self, message: ConsumedMessage) -> WorkerTask:
        return WorkerTask.from_payload(message.payload)

    async def process_message(self, message: ConsumedMessage) -> ProcessOutcome:
        """
        Procesa un mensaje de punta a punta: parsea, analiza y persiste.
        No hace commit (eso lo decide el loop). Lanza si algo falla, para que
        el caller distinga éxito de error.
        """
        task = self.parse_task(message)

        start = time.perf_counter()
        analysis = await self._pipeline.process(task)
        processing_ms = int((time.perf_counter() - start) * 1000)

        recorded = await self._store.record(
            task, analysis, processing_time_ms=processing_ms
        )

        self._metrics.increment("worker.results", worker=self.content_type)
        return ProcessOutcome(status="ok", task=task, result=recorded)

    # ─────────────────────────────────────────
    # Loop de consumo
    # ─────────────────────────────────────────
    async def run_forever(self) -> None:
        """Consume y procesa mensajes hasta que se llame a `stop()`."""
        self._running = True
        await self.on_start()
        logger.info("[%s] iniciado", self._name)
        try:
            while not self._stop.is_set():
                message = await self._consumer.poll(self._poll_timeout)
                if message is None:
                    continue
                self._consumed += 1
                await self._handle(message)
        finally:
            self._running = False
            await self.on_stop()
            logger.info(
                "[%s] detenido (consumidos=%d ok=%d fallidos=%d)",
                self._name,
                self._consumed,
                self._ok,
                self._failed,
            )

    async def _handle(self, message: ConsumedMessage) -> None:
        """Procesa un mensaje del loop, aislando errores para no morir."""
        try:
            await self.process_message(message)
            self._ok += 1
        except Exception as e:
            self._failed += 1
            self._metrics.increment("worker.failures", worker=self.content_type)
            logger.error(
                "[%s] error procesando mensaje (offset=%s): %s: %s",
                self._name,
                message.offset,
                type(e).__name__,
                e,
            )
        finally:
            # At-least-once con salto de "poison pill": confirmamos siempre
            # para no quedar atascados en un mensaje envenenado. Un DLQ real
            # es trabajo de FASE 3 (Persona C).
            try:
                await self._consumer.commit(message)
            except Exception as e:
                logger.error("[%s] error en commit: %s", self._name, e)

    def stop(self) -> None:
        """Pide al loop que termine en la próxima iteración."""
        self._stop.set()

    # ─────────────────────────────────────────
    # Inspección
    # ─────────────────────────────────────────
    @property
    def name(self) -> str:
        return self._name

    @property
    def worker_type(self) -> WorkerType:
        return WorkerType(self.content_type)

    @property
    def is_running(self) -> bool:
        return self._running

    def stats(self) -> dict[str, int]:
        return {
            "consumed": self._consumed,
            "ok": self._ok,
            "failed": self._failed,
        }

    # ─────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────
    def _describe_task(self, task: WorkerTask) -> str:
        return f"{task.part_id} case={task.case_id[:8]}"
