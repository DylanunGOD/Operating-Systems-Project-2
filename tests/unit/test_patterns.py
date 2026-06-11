"""
Tests unitarios de los patrones de FASE 2 (Persona B):
Object Pool, Bulkhead, Strategy y Decorator.

asyncio_mode=auto (ver pyproject) → los `async def test_*` corren solos.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from src.patterns.bulkhead import (
    Bulkhead,
    BulkheadFullError,
    BulkheadRegistry,
    build_default_registry,
)
from src.patterns.decorators import (
    CachingProcessor,
    FunctionProcessor,
    InMemoryCache,
    InMemoryMetrics,
    MetricsProcessor,
    PipelineOptions,
    RetryProcessor,
    build_pipeline,
)
from src.patterns.object_pool import (
    ObjectPool,
    PoolClosedError,
    PoolExhaustedError,
)
from src.patterns.strategy import (
    ContentTypeStrategy,
    DeadlineStrategy,
    FIFOStrategy,
    PriorityLevelStrategy,
    PriorityTaskQueue,
    ScheduledTask,
    WeightedAgingStrategy,
    available_strategies,
    build_strategy,
)


# ═════════════════════════════════════════════════════
# Object Pool
# ═════════════════════════════════════════════════════
class TestObjectPool:
    async def test_reuses_instances(self) -> None:
        created = []

        def factory() -> object:
            obj = object()
            created.append(obj)
            return obj

        pool = ObjectPool(factory, max_size=2)
        a = await pool.acquire()
        await pool.release(a)
        b = await pool.acquire()
        assert a is b  # se reutilizó, no se creó otro
        assert len(created) == 1

    async def test_lease_returns_on_exception(self) -> None:
        pool = ObjectPool(lambda: object(), max_size=1)
        with pytest.raises(RuntimeError):
            async with pool.lease():
                raise RuntimeError("boom")
        assert pool.stats().available == 1  # se devolvió pese al error

    async def test_exhaustion_raises_timeout(self) -> None:
        pool = ObjectPool(lambda: object(), max_size=1, acquire_timeout=0.05)
        await pool.acquire()  # no se devuelve
        with pytest.raises(PoolExhaustedError):
            await pool.acquire()

    async def test_blocked_acquire_unblocks_on_release(self) -> None:
        pool = ObjectPool(lambda: object(), max_size=1, acquire_timeout=2.0)
        a = await pool.acquire()
        waiter = asyncio.create_task(pool.acquire())
        await asyncio.sleep(0.05)
        assert not waiter.done()
        await pool.release(a)
        got = await asyncio.wait_for(waiter, timeout=1.0)
        assert got is a

    async def test_validator_discards_invalid(self) -> None:
        class Conn:
            def __init__(self) -> None:
                self.alive = True

        pool = ObjectPool(Conn, max_size=2, validator=lambda c: c.alive)
        c = await pool.acquire()
        c.alive = False
        await pool.release(c)
        c2 = await pool.acquire()
        assert c2 is not c  # el inválido se descartó y se creó otro

    async def test_warmup_and_stats(self) -> None:
        pool = ObjectPool(lambda: object(), max_size=4, min_size=2)
        created = await pool.warmup()
        assert created == 2
        assert pool.stats().available == 2

    async def test_close_blocks_acquire(self) -> None:
        pool = ObjectPool(lambda: object(), max_size=1)
        await pool.close()
        with pytest.raises(PoolClosedError):
            await pool.acquire()

    async def test_async_factory(self) -> None:
        async def make() -> str:
            await asyncio.sleep(0)
            return "x"

        pool = ObjectPool(make, max_size=1)
        async with pool.lease() as v:
            assert v == "x"


# ═════════════════════════════════════════════════════
# Bulkhead
# ═════════════════════════════════════════════════════
class TestBulkhead:
    async def test_runs_sync_in_thread(self) -> None:
        import threading

        bh = Bulkhead("t", max_concurrency=2)
        main_thread = threading.current_thread().ident
        seen = {}

        def work(x: int) -> int:
            seen["thread"] = threading.current_thread().ident
            return x * 2

        result = await bh.run(work, 21)
        assert result == 42
        assert seen["thread"] != main_thread  # se descargó a un hilo

    async def test_runs_async_in_loop(self) -> None:
        bh = Bulkhead("t", max_concurrency=2)

        async def work(x: int) -> int:
            return x + 1

        assert await bh.run(work, 9) == 10
        assert bh.stats().executed == 1

    async def test_rejects_when_full(self) -> None:
        bh = Bulkhead("t", max_concurrency=1, max_queue=0)
        gate = asyncio.Event()

        async def hold() -> str:
            await gate.wait()
            return "done"

        running = asyncio.create_task(bh.run(hold))
        await asyncio.sleep(0.05)  # que tome el único slot
        with pytest.raises(BulkheadFullError):
            await bh.run(lambda: 1)
        gate.set()
        assert await running == "done"
        assert bh.stats().rejected == 1

    async def test_concurrency_is_capped(self) -> None:
        bh = Bulkhead("t", max_concurrency=2)
        active = 0
        peak = 0
        lock = asyncio.Lock()

        async def work() -> None:
            nonlocal active, peak
            async with lock:
                active += 1
                peak = max(peak, active)
            await asyncio.sleep(0.05)
            async with lock:
                active -= 1

        await asyncio.gather(*[bh.run(work) for _ in range(6)])
        assert peak <= 2

    async def test_failure_counts(self) -> None:
        bh = Bulkhead("t", max_concurrency=1)

        def boom() -> None:
            raise ValueError("x")

        with pytest.raises(ValueError):
            await bh.run(boom)
        assert bh.stats().failed == 1

    def test_registry(self) -> None:
        reg = BulkheadRegistry()
        reg.get_or_create("text", max_concurrency=3)
        assert reg.has("text")
        assert reg.get("text").stats().max_concurrency == 3
        with pytest.raises(KeyError):
            reg.get("nope")

    def test_default_registry_isolated(self) -> None:
        reg = build_default_registry()
        assert {"text", "image", "audio", "consolidation"} <= set(
            reg.all_stats()
        )
        reg.close_all()


# ═════════════════════════════════════════════════════
# Strategy
# ═════════════════════════════════════════════════════
class TestStrategy:
    def test_fifo_order(self) -> None:
        q = PriorityTaskQueue(FIFOStrategy())
        for i in range(3):
            q.push(ScheduledTask(payload={"i": i}))
        assert [q.pop().payload["i"] for _ in range(3)] == [0, 1, 2]

    def test_priority_level_order(self) -> None:
        q = PriorityTaskQueue(PriorityLevelStrategy())
        q.push(ScheduledTask(payload={"id": "low"}, priority="low"))
        q.push(ScheduledTask(payload={"id": "crit"}, priority="critical"))
        q.push(ScheduledTask(payload={"id": "norm"}, priority="normal"))
        assert [q.pop().payload["id"] for _ in range(3)] == ["crit", "norm", "low"]

    def test_content_type_order(self) -> None:
        q = PriorityTaskQueue(ContentTypeStrategy())
        q.push(ScheduledTask(payload={}, type="audio"))
        q.push(ScheduledTask(payload={}, type="text"))
        q.push(ScheduledTask(payload={}, type="image"))
        assert [q.pop().type for _ in range(3)] == ["text", "image", "audio"]

    def test_deadline_order(self) -> None:
        now = datetime.now(timezone.utc)
        q = PriorityTaskQueue(DeadlineStrategy())
        q.push(ScheduledTask(
            payload={"id": "later"}, deadline=now + timedelta(hours=2)
        ))
        q.push(ScheduledTask(
            payload={"id": "soon"}, deadline=now + timedelta(minutes=5)
        ))
        q.push(ScheduledTask(payload={"id": "none"}))  # sin deadline → al final
        assert [q.pop().payload["id"] for _ in range(3)] == ["soon", "later", "none"]

    def test_weighted_aging_prevents_starvation(self) -> None:
        old = datetime.now(timezone.utc) - timedelta(seconds=10_000)
        strat = WeightedAgingStrategy(priority_weight=100.0, aging_per_second=1.0)
        q = PriorityTaskQueue(strat)
        q.push(ScheduledTask(payload={"id": "old-low"}, priority="low", created_at=old))
        q.push(ScheduledTask(payload={"id": "new-high"}, priority="high"))
        # la vieja de baja prioridad envejeció lo suficiente para adelantarse
        assert q.pop().payload["id"] == "old-low"

    def test_swap_strategy_at_runtime(self) -> None:
        q = PriorityTaskQueue(FIFOStrategy())
        q.push(ScheduledTask(payload={"id": "a"}, priority="low"))
        q.push(ScheduledTask(payload={"id": "b"}, priority="critical"))
        q.set_strategy(PriorityLevelStrategy())
        assert q.pop().payload["id"] == "b"  # ahora gana la prioridad

    def test_empty_queue(self) -> None:
        q = PriorityTaskQueue()
        assert q.is_empty
        assert q.pop() is None

    def test_build_strategy_factory(self) -> None:
        assert isinstance(build_strategy("priority"), PriorityLevelStrategy)
        assert "fifo" in available_strategies()
        with pytest.raises(ValueError):
            build_strategy("inexistente")


# ═════════════════════════════════════════════════════
# Decorator (pipeline)
# ═════════════════════════════════════════════════════
class TestDecorators:
    async def test_caching_skips_core_on_hit(self) -> None:
        calls = {"n": 0}

        async def core(item: dict) -> dict:
            calls["n"] += 1
            return {"r": item["v"]}

        cache = InMemoryCache()
        proc = CachingProcessor(
            FunctionProcessor(core), key_fn=lambda i: i["v"], cache=cache
        )
        assert (await proc.process({"v": "a"}))["r"] == "a"
        await proc.process({"v": "a"})  # hit
        assert calls["n"] == 1

    async def test_retry_then_succeed(self) -> None:
        attempts = {"n": 0}

        async def flaky(item: int) -> int:
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise ValueError("transitorio")
            return item

        proc = RetryProcessor(
            FunctionProcessor(flaky), max_attempts=3, base_delay=0.0
        )
        assert await proc.process(7) == 7
        assert attempts["n"] == 3

    async def test_retry_exhausts(self) -> None:
        async def always_fail(item: int) -> int:
            raise ValueError("nope")

        proc = RetryProcessor(
            FunctionProcessor(always_fail), max_attempts=2, base_delay=0.0
        )
        with pytest.raises(ValueError):
            await proc.process(1)

    async def test_metrics_count_processed_and_errors(self) -> None:
        metrics = InMemoryMetrics()

        async def core(item: int) -> int:
            if item < 0:
                raise ValueError("neg")
            return item

        proc = MetricsProcessor(FunctionProcessor(core), metrics=metrics, worker="x")
        await proc.process(1)
        with pytest.raises(ValueError):
            await proc.process(-1)
        assert metrics.counters["processor.processed{worker=x}"] == 1
        assert metrics.counters["processor.errors{worker=x}"] == 1

    async def test_build_pipeline_composition(self) -> None:
        metrics = InMemoryMetrics()
        calls = {"n": 0}

        async def core(item: dict) -> dict:
            calls["n"] += 1
            return {"echo": item["v"]}

        pipeline = build_pipeline(
            core,
            PipelineOptions(
                with_caching=True,
                cache_key_fn=lambda i: i["v"],
                metrics=metrics,
                max_attempts=2,
            ),
        )
        assert (await pipeline.process({"v": "z"}))["echo"] == "z"
        await pipeline.process({"v": "z"})  # cache hit → no recomputa
        assert calls["n"] == 1
        assert metrics.counters.get("cache.hits") == 1

    async def test_inmemory_cache_ttl_and_lru(self) -> None:
        cache = InMemoryCache(max_size=2)
        await cache.set("a", 1, ttl=None)
        await cache.set("b", 2)
        await cache.set("c", 3)  # desaloja 'a' (LRU)
        assert await cache.get("a") is None
        assert await cache.get("c") == 3

        await cache.set("k", 9, ttl=0.05)
        await asyncio.sleep(0.08)
        assert await cache.get("k") is None  # expiró por TTL
