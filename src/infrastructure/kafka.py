"""
infrastructure/kafka.py — Mensajería real con Apache Kafka (confluent-kafka).

Entregable de Persona C (FASE 3). Reemplaza los *fakes* que A y B usaron:

- ``KafkaManager``       → lado PRODUCTOR. Cumple el Protocol ``KafkaPublisher``
  de A (``async publish(topic, payload)``). Lo consume el ``OutboxPublisher``
  para drenar la tabla ``outbox`` hacia Kafka con cero pérdida de eventos.
- ``KafkaWorkerConsumer``→ lado CONSUMIDOR. Cumple el Protocol
  ``MessageConsumer`` de B (``poll`` / ``commit`` / ``close``). Lo usan los
  workers para sacar sub-tareas de sus topics.
- ``build_consumer(topics)`` → fábrica que el ``WorkerFactory`` de B invoca.
  Construye un ``KafkaWorkerConsumer`` real y **verifica conectividad**; si no
  hay broker (o ``confluent-kafka`` no está instalado) lanza
  ``KafkaUnavailableError``, y el factory cae elegantemente al
  ``InMemoryConsumer`` (modo dev). Por eso el contrato "el worker arranca igual
  y se conecta a Kafka sin cambios cuando la infra esté lista" se cumple solo.

``confluent-kafka`` se importa de forma PEREZOSA: este módulo se puede importar
(y testear con fakes inyectados) sin tener la librería ni un broker corriendo.
"""

from __future__ import annotations

import asyncio
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from typing import Any

from src.infrastructure.config import Settings, get_settings

# El contrato del mensaje consumido lo definió B; lo reutilizamos para que los
# workers no distingan entre el consumer real y el InMemoryConsumer.
from src.workers.messaging import ConsumedMessage

logger = logging.getLogger("infrastructure.kafka")


# ─────────────────────────────────────────────────────
# Errores
# ─────────────────────────────────────────────────────
class KafkaUnavailableError(RuntimeError):
    """No se pudo construir/conectar un cliente Kafka real."""


class KafkaPublishError(RuntimeError):
    """Falló la entrega de un mensaje al broker."""


class KafkaConsumeError(RuntimeError):
    """Error al consumir un mensaje del broker."""


# ─────────────────────────────────────────────────────
# Import perezoso de confluent-kafka
# ─────────────────────────────────────────────────────
def _load_confluent() -> Any:
    """
    Importa ``confluent_kafka`` o lanza ``KafkaUnavailableError`` si no está.
    Mantenerlo perezoso permite importar y testear este módulo sin la lib.
    """
    try:
        import confluent_kafka  # type: ignore
    except ImportError as e:
        raise KafkaUnavailableError(
            "confluent-kafka no está instalado; instala requirements.txt "
            "para habilitar el Kafka real."
        ) from e
    return confluent_kafka


# ─────────────────────────────────────────────────────
# (De)serialización JSON
# ─────────────────────────────────────────────────────
def serialize(payload: dict[str, Any]) -> bytes:
    """dict → bytes JSON (UTF-8). ``default=str`` cubre UUID/datetime."""
    return json.dumps(payload, default=str, separators=(",", ":")).encode("utf-8")


def deserialize(value: Any) -> dict[str, Any]:
    """bytes/str/dict → dict. ``None`` → ``{}``."""
    if value is None:
        return {}
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str):
        return json.loads(value)
    if isinstance(value, dict):
        return value
    raise TypeError(f"No sé deserializar un value de tipo {type(value).__name__}")


def _encode_key(key: str | None) -> bytes | None:
    return key.encode("utf-8") if key is not None else None


def _decode_key(key: Any) -> str | None:
    if key is None:
        return None
    if isinstance(key, (bytes, bytearray)):
        return key.decode("utf-8", errors="replace")
    return str(key)


def _resolve(fut: "asyncio.Future[Any]", value: Any) -> None:
    if not fut.done():
        fut.set_result(value)


def _reject(fut: "asyncio.Future[Any]", exc: BaseException) -> None:
    if not fut.done():
        fut.set_exception(exc)


# ─────────────────────────────────────────────────────
# Configuración de clientes (desde Settings)
# ─────────────────────────────────────────────────────
def _producer_config(settings: Settings) -> dict[str, Any]:
    # Idempotencia + acks=all → un evento de la outbox nunca se duplica ni se
    # pierde aunque haya reintentos internos de la librería.
    return {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "client.id": f"{settings.kafka_client_id}-producer",
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 5,
        "retries": 5,
    }


def _consumer_config(settings: Settings, group_id: str | None) -> dict[str, Any]:
    # enable.auto.commit=False → el BaseWorker confirma manualmente tras
    # procesar (at-least-once); el commit lo hace KafkaWorkerConsumer.commit.
    return {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": group_id or settings.kafka_group_id,
        "client.id": f"{settings.kafka_client_id}-consumer",
        "auto.offset.reset": settings.kafka_auto_offset_reset,
        "enable.auto.commit": False,
    }


# ─────────────────────────────────────────────────────
# KafkaManager — productor (cumple KafkaPublisher de A)
# ─────────────────────────────────────────────────────
class KafkaManager:
    """
    Publicador a Kafka. Implementa el Protocol ``KafkaPublisher`` de
    ``patterns/outbox.py`` (``async publish(topic, payload)``), así que el
    ``OutboxPublisher`` lo acepta tal cual sustituyendo al ``ConsoleKafkaPublisher``.

    La librería ``confluent_kafka.Producer`` es BLOQUEANTE; sus llamadas se
    descargan a un hilo dedicado para no congelar el event loop. ``publish``
    espera la confirmación de entrega (delivery callback) antes de retornar,
    de modo que si el broker rechaza el mensaje, ``publish`` lanza y el poller
    de la outbox lo reintenta.
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str | None = None,
        producer: Any = None,
        flush_timeout: float = 10.0,
        settings: Settings | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        if bootstrap_servers is not None:
            # No mutamos el singleton de Settings: trabajamos sobre una copia.
            self._settings = self._settings.model_copy()
            self._settings.kafka_bootstrap_servers = bootstrap_servers
        self._producer = producer  # inyectable (tests); si None se crea lazy
        self._flush_timeout = flush_timeout
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="kafka-producer"
        )
        self._closed = False

    # ── Ciclo de vida ────────────────────────────────
    def _ensure_producer(self) -> Any:
        if self._producer is None:
            producer_cls = _load_confluent().Producer  # pragma: no cover - real broker
            self._producer = producer_cls(  # pragma: no cover
                _producer_config(self._settings)
            )
        return self._producer

    # ── Contrato KafkaPublisher ──────────────────────
    async def publish(
        self, topic: str, payload: dict[str, Any], *, key: str | None = None
    ) -> None:
        """
        Publica ``payload`` (dict JSON-serializable) en ``topic`` y espera la
        confirmación de entrega. Lanza ``KafkaPublishError`` si falla.
        """
        if self._closed:
            raise KafkaPublishError("KafkaManager ya está cerrado")

        producer = self._ensure_producer()
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[Any] = loop.create_future()

        def _on_delivery(err: Any, msg: Any) -> None:
            # Corre en el hilo del executor; reprograma en el loop de forma segura.
            if err is not None:
                _err = KafkaPublishError(f"entrega fallida en {topic}: {err}")
                loop.call_soon_threadsafe(_reject, fut, _err)
            else:
                loop.call_soon_threadsafe(_resolve, fut, msg)

        def _produce_and_flush() -> None:
            producer.produce(
                topic,
                value=serialize(payload),
                key=_encode_key(key),
                on_delivery=_on_delivery,
            )
            # flush bloquea hasta drenar la cola → dispara on_delivery.
            remaining = producer.flush(self._flush_timeout)
            if remaining:  # pragma: no cover - timeout de broker real
                raise KafkaPublishError(
                    f"timeout publicando a {topic}: quedan {remaining} en cola"
                )

        await loop.run_in_executor(self._executor, _produce_and_flush)
        await fut  # propaga el resultado/excepción de la entrega

    # ── Mantenimiento ────────────────────────────────
    async def flush(self, timeout: float | None = None) -> None:
        if self._producer is None:
            return
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            self._executor, self._producer.flush, timeout or self._flush_timeout
        )

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            await self.flush()
        finally:
            self._executor.shutdown(wait=True)

    def ensure_topics(
        self,
        topics: list[str],
        *,
        num_partitions: int = 3,
        replication_factor: int = 1,
    ) -> None:  # pragma: no cover - requiere broker real
        """
        Crea los topics indicados si no existen (idempotente). Útil para
        bootstrap; en docker-compose ``KAFKA_AUTO_CREATE_TOPICS_ENABLE=true``
        ya los crea al primer uso, así que esto es opcional.
        """
        _load_confluent()
        from confluent_kafka.admin import AdminClient, NewTopic

        admin = AdminClient(
            {"bootstrap.servers": self._settings.kafka_bootstrap_servers}
        )
        existing = set(admin.list_topics(timeout=10).topics)
        nuevos = [
            NewTopic(t, num_partitions, replication_factor)
            for t in topics
            if t not in existing
        ]
        if not nuevos:
            return
        for topic, future in admin.create_topics(nuevos).items():
            try:
                future.result()
                logger.info("Topic creado: %s", topic)
            except Exception as e:
                msg = str(e).lower()
                if "already exists" in msg:
                    continue
                raise


# ─────────────────────────────────────────────────────
# KafkaWorkerConsumer — consumidor (cumple MessageConsumer de B)
# ─────────────────────────────────────────────────────
def _is_partition_eof(err: Any) -> bool:
    """¿El 'error' es realmente fin-de-partición (no es un fallo)?"""
    try:  # pragma: no cover - depende de confluent instalado
        from confluent_kafka import KafkaError

        return err.code() == KafkaError._PARTITION_EOF
    except Exception:
        return False


class KafkaWorkerConsumer:
    """
    Adapta un ``confluent_kafka.Consumer`` al Protocol ``MessageConsumer``.
    Las llamadas bloqueantes de la librería se descargan a un hilo dedicado.

    Semántica del contrato:
    - ``poll(timeout)``  → próximo ``ConsumedMessage`` o ``None`` (no bloquea
      para siempre; fin-de-partición se trata como "nada nuevo").
    - ``commit(message)``→ commit síncrono del offset (at-least-once).
    - ``close()``        → cierra el consumer y libera el hilo.
    """

    def __init__(
        self,
        topics: list[str],
        *,
        consumer: Any,
        poll_executor: ThreadPoolExecutor | None = None,
    ) -> None:
        self._topics = list(topics)
        self._consumer = consumer
        self._executor = poll_executor or ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="kafka-consumer"
        )
        self._closed = False

    @property
    def topics(self) -> list[str]:
        return list(self._topics)

    async def poll(self, timeout: float) -> ConsumedMessage | None:
        loop = asyncio.get_running_loop()
        raw = await loop.run_in_executor(self._executor, self._consumer.poll, timeout)
        if raw is None:
            return None
        err = raw.error()
        if err is not None:
            if _is_partition_eof(err):
                return None
            raise KafkaConsumeError(f"error consumiendo de {self._topics}: {err}")
        return ConsumedMessage(
            topic=raw.topic(),
            payload=deserialize(raw.value()),
            key=_decode_key(raw.key()),
            offset=raw.offset(),
        )

    async def commit(self, message: ConsumedMessage) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            self._executor, lambda: self._consumer.commit(asynchronous=False)
        )

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(self._executor, self._consumer.close)
        finally:
            self._executor.shutdown(wait=False)

    @property
    def is_closed(self) -> bool:
        return self._closed


# ─────────────────────────────────────────────────────
# build_consumer — fábrica que invoca el WorkerFactory de B
# ─────────────────────────────────────────────────────
def build_consumer(
    topics: list[str], *, group_id: str | None = None
) -> KafkaWorkerConsumer:
    """
    Construye un ``KafkaWorkerConsumer`` real suscrito a ``topics``.

    Verifica conectividad con un sondeo de metadata; si el broker no responde
    (o ``confluent-kafka`` no está instalado) lanza ``KafkaUnavailableError``.
    El ``WorkerFactory`` de B atrapa esa excepción y cae al ``InMemoryConsumer``,
    de modo que los workers arrancan igual en dev y se conectan a Kafka real
    en cuanto el broker está disponible —sin cambiar una línea de B.
    """
    settings = get_settings()
    if not settings.kafka_enabled:
        # Camino rápido para dev/tests: ni siquiera intentamos confluent ni el
        # sondeo; el WorkerFactory caerá al InMemoryConsumer.
        raise KafkaUnavailableError("KAFKA_ENABLED=false (modo dev/in-memory)")
    confluent = _load_confluent()  # KafkaUnavailableError si no está instalado

    consumer = confluent.Consumer(  # pragma: no cover - requiere confluent real
        _consumer_config(settings, group_id)
    )
    try:  # pragma: no cover - requiere broker real
        consumer.list_topics(timeout=settings.kafka_probe_timeout_seconds)
    except Exception as e:  # pragma: no cover
        try:
            consumer.close()
        except Exception:
            pass
        raise KafkaUnavailableError(
            f"no hay broker en {settings.kafka_bootstrap_servers}: {e}"
        ) from e

    consumer.subscribe(list(topics))  # pragma: no cover - requiere broker real
    logger.info("KafkaWorkerConsumer suscrito a %s", topics)  # pragma: no cover
    return KafkaWorkerConsumer(topics, consumer=consumer)  # pragma: no cover


# ─────────────────────────────────────────────────────
# Singleton del productor (para el OutboxPublisher / API)
# ─────────────────────────────────────────────────────
@lru_cache
def get_kafka_manager() -> KafkaManager:
    """Singleton del ``KafkaManager`` por proceso."""
    return KafkaManager()


__all__ = [
    "KafkaManager",
    "KafkaWorkerConsumer",
    "build_consumer",
    "get_kafka_manager",
    "KafkaUnavailableError",
    "KafkaPublishError",
    "KafkaConsumeError",
    "serialize",
    "deserialize",
]
