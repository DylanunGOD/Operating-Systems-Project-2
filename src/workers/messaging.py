"""
Abstracción de consumo de mensajes para los workers.

CONTEXTO:
    Persona A definió el lado *publicador* (`KafkaPublisher` Protocol en
    patterns/outbox.py). Los workers de FASE 2 necesitan el lado
    *consumidor*: algo de lo que sacar mensajes de un topic.

    Como Persona C todavía no entrega el Kafka real (infrastructure/kafka.py
    está pendiente), aquí se define el *contrato* (`MessageConsumer`) más
    una implementación en memoria (`InMemoryConsumer`) para desarrollo y
    tests —exactamente el mismo enfoque que A usó con `ConsoleKafkaPublisher`.

    Cuando C entregue el `KafkaManager`, sólo tiene que cumplir el Protocol
    `MessageConsumer` (o envolver su consumer con `KafkaConsumerAdapter`) y
    los workers no cambian una línea.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable


# ─────────────────────────────────────────────────────
# Mensaje consumido
# ─────────────────────────────────────────────────────
@dataclass(frozen=True)
class ConsumedMessage:
    """
    Un mensaje sacado de un topic. `payload` es el dict que el productor
    publicó (en este sistema, el `payload` de un SubTask del Splitter).
    """

    topic: str
    payload: dict[str, Any]
    key: str | None = None
    offset: int | None = None


# ─────────────────────────────────────────────────────
# Contrato del consumidor
# ─────────────────────────────────────────────────────
@runtime_checkable
class MessageConsumer(Protocol):
    """
    Cualquier objeto del que un worker pueda consumir mensajes.

    Semántica:
    - `poll(timeout)` devuelve el próximo mensaje o None si no hay nada en
      `timeout` segundos (no bloquea para siempre: deja respirar al loop).
    - `commit(message)` confirma el procesamiento (avanza el offset).
    - `close()` libera recursos.
    """

    async def poll(self, timeout: float) -> ConsumedMessage | None: ...
    async def commit(self, message: ConsumedMessage) -> None: ...
    async def close(self) -> None: ...


# ─────────────────────────────────────────────────────
# Implementación en memoria (dev / tests)
# ─────────────────────────────────────────────────────
class InMemoryConsumer:
    """
    Consumidor respaldado por una `asyncio.Queue`. Permite alimentar
    mensajes a mano (`feed`) y verificar cuáles se confirmaron
    (`committed`). Es la contraparte de `ConsoleKafkaPublisher`.
    """

    def __init__(self, *, name: str = "in-memory") -> None:
        self.name = name
        self._queue: asyncio.Queue[ConsumedMessage] = asyncio.Queue()
        self._seq = 0
        self._closed = False
        self.committed: list[ConsumedMessage] = []

    # ── Alimentación (productor de prueba) ──
    async def feed(
        self, topic: str, payload: dict[str, Any], key: str | None = None
    ) -> ConsumedMessage:
        msg = ConsumedMessage(topic, payload, key, self._seq)
        self._seq += 1
        await self._queue.put(msg)
        return msg

    def feed_nowait(
        self, topic: str, payload: dict[str, Any], key: str | None = None
    ) -> ConsumedMessage:
        msg = ConsumedMessage(topic, payload, key, self._seq)
        self._seq += 1
        self._queue.put_nowait(msg)
        return msg

    # ── Contrato MessageConsumer ──
    async def poll(self, timeout: float) -> ConsumedMessage | None:
        try:
            return await asyncio.wait_for(self._queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    async def commit(self, message: ConsumedMessage) -> None:
        self.committed.append(message)

    async def close(self) -> None:
        self._closed = True

    # ── Inspección ──
    @property
    def pending(self) -> int:
        return self._queue.qsize()

    @property
    def is_closed(self) -> bool:
        return self._closed

    async def join(self) -> None:
        """Espera a que se hayan sacado todos los mensajes encolados."""
        while self._queue.qsize() > 0:
            await asyncio.sleep(0)


# ─────────────────────────────────────────────────────
# Adapter para el Kafka real de Persona C
# ─────────────────────────────────────────────────────
class KafkaConsumerAdapter:
    """
    Envuelve un `confluent_kafka.Consumer` (o equivalente) y lo adapta al
    Protocol `MessageConsumer`. No importa confluent-kafka a nivel de módulo
    para que el resto del sistema funcione sin esa dependencia instalada;
    Persona C inyecta el consumer ya construido.

    Se espera que `raw_consumer` exponga:
        - poll(timeout) -> mensaje con .topic(), .value(), .key(), .error()
        - commit(message)
        - close()
    y que `deserialize(bytes) -> dict` convierta el value a payload.
    """

    def __init__(
        self,
        raw_consumer: Any,
        *,
        deserialize: Any = None,
    ) -> None:
        self._raw = raw_consumer
        self._deserialize = deserialize or _default_json_deserialize

    async def poll(self, timeout: float) -> ConsumedMessage | None:
        loop = asyncio.get_event_loop()
        # confluent-kafka es bloqueante: a un hilo para no congelar el loop.
        raw = await loop.run_in_executor(None, self._raw.poll, timeout)
        if raw is None:
            return None
        if raw.error():
            raise RuntimeError(f"Kafka consumer error: {raw.error()}")
        return ConsumedMessage(
            topic=raw.topic(),
            payload=self._deserialize(raw.value()),
            key=raw.key().decode() if raw.key() else None,
            offset=raw.offset(),
        )

    async def commit(self, message: ConsumedMessage) -> None:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._raw.commit)

    async def close(self) -> None:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._raw.close)


def _default_json_deserialize(value: Any) -> dict[str, Any]:
    import json

    if value is None:
        return {}
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str):
        return json.loads(value)
    if isinstance(value, dict):
        return value
    raise TypeError(f"No sé deserializar un value de tipo {type(value).__name__}")
