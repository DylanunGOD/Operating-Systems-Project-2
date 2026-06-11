"""
monitoring/worker_runner.py — arranca un worker CON servidor de métricas.

Entregable de Persona C (FASE 3). Envuelve el entrypoint de B
(``worker_factory``) **sin modificarlo**, usando solo su API pública:

1. Levanta el servidor HTTP de métricas Prometheus del worker.
2. Inyecta ``PrometheusMetrics`` en el worker (B lo acepta vía el parámetro
   ``metrics`` de ``BaseWorker``), de modo que los contadores ``worker.results``
   / ``worker.failures`` / latencias del pipeline aparezcan en Prometheus.
3. Corre el worker hasta SIGTERM/SIGINT.

Lo usan los Dockerfiles de los workers:
    python -m src.monitoring.worker_runner <tipo>
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
from typing import Any

logger = logging.getLogger("monitoring.worker_runner")


async def _run(content_type: str) -> None:
    from src.infrastructure.config import get_settings
    from src.monitoring.metrics import get_metrics
    from src.workers.stores import DatabaseStore
    from src.workers.worker_factory import (
        build_consumer,
        create_consolidation_worker,
        create_worker,
    )

    settings = get_settings()
    topics = {
        "text": [settings.kafka_topic_text],
        "image": [settings.kafka_topic_image],
        "audio": [settings.kafka_topic_audio],
        "consolidation": [settings.kafka_topic_results],
    }.get(content_type, [])

    consumer = build_consumer(topics)
    store = DatabaseStore()

    if content_type == "consolidation":
        worker: Any = create_consolidation_worker(
            consumer=consumer, reader=store, writer=store
        )
    else:
        try:
            worker = create_worker(
                content_type, consumer=consumer, store=store, metrics=get_metrics()
            )
        except TypeError:
            # Versión de BaseWorker sin parámetro metrics: corre igual sin métricas.
            worker = create_worker(content_type, consumer=consumer, store=store)

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, worker.stop)
        except NotImplementedError:  # Windows
            pass

    logger.info("Worker '%s' arrancando sobre topics %s", content_type, topics)
    try:
        await worker.run_forever()
    finally:
        await consumer.close()


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="Lanza un worker de análisis con servidor de métricas."
    )
    parser.add_argument(
        "content_type",
        nargs="?",
        default=os.getenv("WORKER_TYPE", "text"),
        choices=["text", "image", "audio", "consolidation"],
    )
    args = parser.parse_args(argv)

    # Servidor de métricas (best-effort: si falla, el worker corre igual).
    try:
        from src.infrastructure.config import get_settings
        from src.monitoring.metrics import start_metrics_server

        start_metrics_server(get_settings().worker_metrics_port)
    except Exception as e:  # pragma: no cover - efecto de red / lib ausente
        logger.warning("Servidor de métricas no disponible: %s", e)

    try:
        asyncio.run(_run(args.content_type))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
