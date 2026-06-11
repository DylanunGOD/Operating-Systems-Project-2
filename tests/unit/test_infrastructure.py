"""
Tests de la capa de infraestructura de Persona C (FASE 3):

- ``infrastructure/kafka.py``  → KafkaManager (productor), KafkaWorkerConsumer
  (consumidor) y build_consumer, ejercitados con productores/consumidores FAKE
  inyectados (sin broker ni confluent-kafka instalado).
- ``infrastructure/redis_client.py`` → RedisCache (con un cliente redis fake) e
  InMemoryRedisCache (sin Redis): get/set, cache-aside, lock distribuido y
  pub/sub.

Todo corre en memoria, igual que el resto de la suite (sin Kafka/Postgres/Redis
reales).
"""

from __future__ import annotations

import asyncio

import pytest

from src.infrastructure import kafka as kafka_infra
from src.infrastructure.kafka import (
    KafkaConsumeError,
    KafkaManager,
    KafkaPublishError,
    KafkaUnavailableError,
    KafkaWorkerConsumer,
    build_consumer,
    deserialize,
    serialize,
)
from src.infrastructure.redis_client import (
    InMemoryRedisCache,
    LockNotAcquiredError,
    RedisCache,
    _loads,
    get_cache,
)
from src.workers.messaging import ConsumedMessage


# ═════════════════════════════════════════════════════
# Fakes de confluent-kafka
# ═════════════════════════════════════════════════════
class _FakeMsg:
    def __init__(self, topic: str, value: bytes, offset: int = 0) -> None:
        self._topic, self._value, self._offset = topic, value, offset

    def topic(self) -> str:
        return self._topic

    def value(self) -> bytes:
        return self._value

    def offset(self) -> int:
        return self._offset


class _FakeProducerOK:
    """Confirma la entrega con éxito al hacer flush."""

    def __init__(self) -> None:
        self.produced: list[dict] = []
        self._pending = None

    def produce(self, topic, value=None, key=None, on_delivery=None) -> None:
        self.produced.append({"topic": topic, "value": value, "key": key})
        self._pending = (on_delivery, topic, value)

    def flush(self, timeout=None) -> int:
        if self._pending:
            cb, topic, value = self._pending
            self._pending = None
            if cb:
                cb(None, _FakeMsg(topic, value, offset=len(self.produced) - 1))
        return 0


class _FakeProducerFail:
    """Reporta error de entrega al hacer flush."""

    def __init__(self) -> None:
        self._cb = None

    def produce(self, topic, value=None, key=None, on_delivery=None) -> None:
        self._cb = on_delivery

    def flush(self, timeout=None) -> int:
        if self._cb:
            self._cb("broker caído", None)
        return 0


class _FakeErr:
    def __init__(self, msg: str = "boom") -> None:
        self._m = msg

    def __str__(self) -> str:
        return self._m

    def code(self) -> int:
        return -1  # no es _PARTITION_EOF


class _FakeRawOK:
    def __init__(self, topic, value, key=None, offset=0) -> None:
        self._t, self._v, self._k, self._o = topic, value, key, offset

    def error(self):
        return None

    def topic(self):
        return self._t

    def value(self):
        return self._v

    def key(self):
        return self._k

    def offset(self):
        return self._o


class _FakeRawErr:
    def error(self):
        return _FakeErr()


class _FakeConsumer:
    def __init__(self, script) -> None:
        self._script = list(script)
        self.committed = 0
        self.closed = False

    def poll(self, timeout):
        return self._script.pop(0) if self._script else None

    def commit(self, asynchronous=False):
        self.committed += 1

    def close(self):
        self.closed = True


# ═════════════════════════════════════════════════════
# (De)serialización
# ═════════════════════════════════════════════════════
class TestSerialization:
    def test_round_trip(self) -> None:
        payload = {"case_id": "c1", "type": "text", "index": 0}
        assert deserialize(serialize(payload)) == payload

    def test_deserialize_none_is_empty(self) -> None:
        assert deserialize(None) == {}

    def test_deserialize_str_and_dict(self) -> None:
        assert deserialize('{"a":1}') == {"a": 1}
        assert deserialize({"a": 1}) == {"a": 1}

    def test_deserialize_bad_type(self) -> None:
        with pytest.raises(TypeError):
            deserialize(12345)


# ═════════════════════════════════════════════════════
# KafkaManager (productor)
# ═════════════════════════════════════════════════════
class TestKafkaManager:
    async def test_publish_ok(self) -> None:
        prod = _FakeProducerOK()
        mgr = KafkaManager(producer=prod)
        await mgr.publish("analysis.text.tasks", {"case_id": "c1", "n": 1}, key="c1")
        assert len(prod.produced) == 1
        rec = prod.produced[0]
        assert rec["topic"] == "analysis.text.tasks"
        assert deserialize(rec["value"]) == {"case_id": "c1", "n": 1}
        assert rec["key"] == b"c1"
        await mgr.close()

    async def test_publish_delivery_error_raises(self) -> None:
        mgr = KafkaManager(producer=_FakeProducerFail())
        with pytest.raises(KafkaPublishError):
            await mgr.publish("t", {"a": 1})
        await mgr.close()

    async def test_publish_after_close_raises(self) -> None:
        mgr = KafkaManager(producer=_FakeProducerOK())
        await mgr.close()
        with pytest.raises(KafkaPublishError):
            await mgr.publish("t", {})

    async def test_satisfies_kafka_publisher_contract(self) -> None:
        # El OutboxPublisher de A solo exige un objeto con async publish(...).
        from src.patterns.outbox import OutboxPublisher

        mgr = KafkaManager(producer=_FakeProducerOK())
        publisher = OutboxPublisher(kafka_publisher=mgr)  # no debe explotar
        assert publisher.is_running is False
        await mgr.close()


# ═════════════════════════════════════════════════════
# KafkaWorkerConsumer (consumidor)
# ═════════════════════════════════════════════════════
class TestKafkaWorkerConsumer:
    async def test_poll_returns_consumed_message(self) -> None:
        raw = _FakeRawOK(
            "analysis.text.tasks",
            serialize({"case_id": "c1", "type": "text"}),
            key=b"c1",
            offset=7,
        )
        consumer = KafkaWorkerConsumer(
            ["analysis.text.tasks"], consumer=_FakeConsumer([raw])
        )
        msg = await consumer.poll(0.1)
        assert isinstance(msg, ConsumedMessage)
        assert msg.topic == "analysis.text.tasks"
        assert msg.payload == {"case_id": "c1", "type": "text"}
        assert msg.key == "c1"
        assert msg.offset == 7
        await consumer.close()

    async def test_poll_none_when_idle(self) -> None:
        consumer = KafkaWorkerConsumer(["t"], consumer=_FakeConsumer([None]))
        assert await consumer.poll(0.01) is None
        await consumer.close()

    async def test_poll_error_raises(self) -> None:
        consumer = KafkaWorkerConsumer(["t"], consumer=_FakeConsumer([_FakeRawErr()]))
        with pytest.raises(KafkaConsumeError):
            await consumer.poll(0.01)
        await consumer.close()

    async def test_commit_and_close(self) -> None:
        fake = _FakeConsumer([])
        consumer = KafkaWorkerConsumer(["t"], consumer=fake)
        await consumer.commit(ConsumedMessage("t", {}, None, 0))
        assert fake.committed == 1
        await consumer.close()
        assert fake.closed is True
        assert consumer.is_closed is True
        # close idempotente
        await consumer.close()


# ═════════════════════════════════════════════════════
# build_consumer (fábrica que invoca el WorkerFactory de B)
# ═════════════════════════════════════════════════════
class TestBuildConsumer:
    def test_raises_when_kafka_disabled(self) -> None:
        # KAFKA_ENABLED=false (default dev) → camino rápido sin tocar confluent.
        with pytest.raises(KafkaUnavailableError):
            build_consumer(["analysis.text.tasks"])

    def test_raises_when_confluent_missing(self, monkeypatch) -> None:
        # Con Kafka habilitado pero sin confluent/broker → KafkaUnavailableError,
        # que el WorkerFactory atrapa para caer al InMemoryConsumer.
        from src.infrastructure.config import get_settings

        enabled = get_settings().model_copy()
        enabled.kafka_enabled = True
        monkeypatch.setattr(kafka_infra, "get_settings", lambda: enabled)

        def _boom():
            raise KafkaUnavailableError("sin confluent")

        monkeypatch.setattr(kafka_infra, "_load_confluent", _boom)
        with pytest.raises(KafkaUnavailableError):
            build_consumer(["analysis.text.tasks"])

    def test_worker_factory_falls_back_to_inmemory(self) -> None:
        # Integración del contrato con B: el factory cae al fake si no hay Kafka.
        from src.workers.messaging import InMemoryConsumer
        from src.workers.worker_factory import build_consumer as factory_build

        consumer = factory_build(["analysis.text.tasks"])
        assert isinstance(consumer, InMemoryConsumer)


# ═════════════════════════════════════════════════════
# InMemoryRedisCache
# ═════════════════════════════════════════════════════
class TestInMemoryRedisCache:
    async def test_get_set_round_trip(self) -> None:
        cache = InMemoryRedisCache()
        assert await cache.get("k") is None
        await cache.set("k", {"a": 1})
        assert await cache.get("k") == {"a": 1}

    async def test_ttl_expiry(self, monkeypatch) -> None:
        import src.infrastructure.redis_client as rc

        clock = {"now": 1000.0}
        monkeypatch.setattr(rc.time, "monotonic", lambda: clock["now"])
        cache = rc.InMemoryRedisCache()
        await cache.set("k", "v", ttl=5)
        assert await cache.get("k") == "v"
        clock["now"] += 6
        assert await cache.get("k") is None

    async def test_delete_and_exists(self) -> None:
        cache = InMemoryRedisCache()
        await cache.set("k", 1)
        assert await cache.exists("k") is True
        assert await cache.delete("k") is True
        assert await cache.exists("k") is False
        assert await cache.delete("k") is False

    async def test_get_or_set_async_factory_caches(self) -> None:
        cache = InMemoryRedisCache()
        calls = {"n": 0}

        async def factory():
            calls["n"] += 1
            return {"v": 42}

        assert await cache.get_or_set("k", factory) == {"v": 42}
        assert await cache.get_or_set("k", factory) == {"v": 42}
        assert calls["n"] == 1  # solo se calculó una vez

    async def test_get_or_set_sync_factory(self) -> None:
        cache = InMemoryRedisCache()
        assert await cache.get_or_set("k", lambda: [1, 2, 3]) == [1, 2, 3]

    async def test_lock_is_mutually_exclusive(self) -> None:
        cache = InMemoryRedisCache()
        order: list[str] = []

        async def worker(tag: str, hold: float) -> None:
            async with cache.lock("res"):
                order.append(f"{tag}-in")
                await asyncio.sleep(hold)
                order.append(f"{tag}-out")

        await asyncio.gather(worker("a", 0.02), worker("b", 0.0))
        assert order in (
            ["a-in", "a-out", "b-in", "b-out"],
            ["b-in", "b-out", "a-in", "a-out"],
        )

    async def test_lock_timeout(self) -> None:
        cache = InMemoryRedisCache()
        async with cache.lock("res", blocking_timeout=0.05):
            with pytest.raises(LockNotAcquiredError):
                async with cache.lock("res", blocking_timeout=0.05):
                    pass

    async def test_pubsub(self) -> None:
        cache = InMemoryRedisCache()
        async with cache.subscribe("ch") as ps:
            n = await cache.publish("ch", {"hello": "world"})
            assert n == 1
            msg = await ps.get_message(timeout=0.5)
            assert msg is not None
            assert _loads(msg["data"]) == {"hello": "world"}

    async def test_pubsub_get_message_timeout(self) -> None:
        cache = InMemoryRedisCache()
        async with cache.subscribe("ch") as ps:
            assert await ps.get_message(timeout=0.01) is None


# ═════════════════════════════════════════════════════
# RedisCache (con cliente redis fake)
# ═════════════════════════════════════════════════════
class _FakeRedis:
    """Mínimo cliente redis.asyncio para probar la lógica de RedisCache."""

    def __init__(self) -> None:
        self.store: dict[str, tuple[str, int | None]] = {}
        self.published: list[tuple[str, str]] = []

    async def get(self, key):
        entry = self.store.get(key)
        return None if entry is None else entry[0]

    async def set(self, key, value, ex=None, nx=False):
        if nx and key in self.store:
            return None
        self.store[key] = (value, ex)
        return True

    async def delete(self, key):
        return 1 if self.store.pop(key, None) is not None else 0

    async def exists(self, key):
        return 1 if key in self.store else 0

    async def publish(self, channel, message):
        self.published.append((channel, message))
        return 0

    async def eval(self, script, numkeys, *args):
        key, token = args[0], args[1]
        entry = self.store.get(key)
        if entry is not None and entry[0] == token:
            del self.store[key]
            return 1
        return 0

    async def ping(self):
        return True

    async def aclose(self):
        self.store.clear()


class TestRedisCache:
    async def test_get_set_namespaced(self) -> None:
        client = _FakeRedis()
        cache = RedisCache(client=client, namespace="t")
        await cache.set("k", {"a": 1}, ttl=30)
        assert await cache.get("k") == {"a": 1}
        assert "t:k" in client.store  # prefijo de namespace aplicado

    async def test_delete_and_exists(self) -> None:
        cache = RedisCache(client=_FakeRedis())
        await cache.set("k", 1)
        assert await cache.exists("k") is True
        assert await cache.delete("k") is True
        assert await cache.exists("k") is False

    async def test_get_or_set(self) -> None:
        cache = RedisCache(client=_FakeRedis())
        calls = {"n": 0}

        async def factory():
            calls["n"] += 1
            return "computed"

        assert await cache.get_or_set("k", factory, ttl=10) == "computed"
        assert await cache.get_or_set("k", factory, ttl=10) == "computed"
        assert calls["n"] == 1

    async def test_lock_acquire_and_release(self) -> None:
        client = _FakeRedis()
        cache = RedisCache(client=client)
        async with cache.lock("res", timeout=10) as token:
            assert "analysis:lock:res" in client.store
            assert token
        # liberado vía Lua tras salir del context
        assert "analysis:lock:res" not in client.store

    async def test_lock_contended_raises(self) -> None:
        cache = RedisCache(client=_FakeRedis())
        async with cache.lock("res", timeout=10, blocking_timeout=0.1):
            with pytest.raises(LockNotAcquiredError):
                async with cache.lock("res", timeout=10, blocking_timeout=0.1):
                    pass

    async def test_publish(self) -> None:
        client = _FakeRedis()
        cache = RedisCache(client=client)
        await cache.publish("ch", {"x": 1})
        assert client.published[0][0] == "ch"
        assert _loads(client.published[0][1]) == {"x": 1}

    async def test_ping_and_close(self) -> None:
        cache = RedisCache(client=_FakeRedis())
        assert await cache.ping() is True
        await cache.close()


# ═════════════════════════════════════════════════════
# Config de clientes + singletons
# ═════════════════════════════════════════════════════
class TestKafkaConfigAndSingletons:
    def test_producer_config_has_idempotence(self) -> None:
        from src.infrastructure.config import get_settings
        from src.infrastructure.kafka import _producer_config

        cfg = _producer_config(get_settings())
        assert cfg["enable.idempotence"] is True
        assert cfg["acks"] == "all"
        assert "bootstrap.servers" in cfg

    def test_consumer_config_disables_autocommit(self) -> None:
        from src.infrastructure.config import get_settings
        from src.infrastructure.kafka import _consumer_config

        cfg = _consumer_config(get_settings(), group_id="g1")
        assert cfg["enable.auto.commit"] is False
        assert cfg["group.id"] == "g1"

    def test_get_kafka_manager_is_singleton(self) -> None:
        from src.infrastructure.kafka import get_kafka_manager

        assert get_kafka_manager() is get_kafka_manager()

    def test_kafka_manager_bootstrap_override_does_not_mutate_singleton(self) -> None:
        from src.infrastructure.config import get_settings

        original = get_settings().kafka_bootstrap_servers
        KafkaManager(bootstrap_servers="other-broker:9092", producer=_FakeProducerOK())
        assert get_settings().kafka_bootstrap_servers == original


# ═════════════════════════════════════════════════════
# Singleton get_cache
# ═════════════════════════════════════════════════════
def test_get_cache_is_cache_like() -> None:
    cache = get_cache()
    assert hasattr(cache, "get") and hasattr(cache, "set")
