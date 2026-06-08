"""
Tests complementarios de FASE 2: cubren ramas de utilidad (mensajería,
factory, metadata de archivos, contratos, estrategias y decoradores
auxiliares) que no entran en los flujos principales de test_workers /
test_patterns.
"""

from __future__ import annotations

import asyncio
import logging

import pytest

from src.models.case import CaseStatus
from src.models.result import IncidentCategory, IncidentSeverity, WorkerType
from src.patterns.decorators import (
    FunctionProcessor,
    LoggingProcessor,
    TimingProcessor,
)
from src.patterns.strategy import (
    LIFOStrategy,
    PriorityTaskQueue,
    ScheduledTask,
)
from src.workers.audio_worker import (
    HeuristicAudioAnalyzer,
    _estimate_duration,
    build_audio_analyzer,
)
from src.workers.contracts import CaseInfo, Finding, WorkerTask
from src.workers.image_worker import (
    HeuristicImageAnalyzer,
    _read_image_meta,
    build_image_analyzer,
)
from src.workers.messaging import ConsumedMessage, InMemoryConsumer
from src.workers.worker_factory import (
    build_consumer,
    create_consolidation_worker,
)


# ─────────────────────────────────────────────────────
# Mensajería
# ─────────────────────────────────────────────────────
class TestMessaging:
    async def test_poll_timeout_returns_none(self) -> None:
        consumer = InMemoryConsumer()
        assert await consumer.poll(0.01) is None

    async def test_feed_nowait_and_join(self) -> None:
        consumer = InMemoryConsumer()
        consumer.feed_nowait("t", {"a": 1})
        assert consumer.pending == 1
        msg = await consumer.poll(0.1)
        assert isinstance(msg, ConsumedMessage)
        assert msg.payload == {"a": 1}
        await consumer.join()
        assert consumer.pending == 0

    async def test_commit_and_close(self) -> None:
        consumer = InMemoryConsumer()
        msg = await consumer.feed("t", {})
        await consumer.commit(msg)
        assert consumer.committed == [msg]
        await consumer.close()
        assert consumer.is_closed


# ─────────────────────────────────────────────────────
# Contratos
# ─────────────────────────────────────────────────────
class TestContracts:
    def test_finding_normalization_fallback(self) -> None:
        f = Finding("inexistente", "rarísima", "t", "d")
        assert f.normalized_category() == IncidentCategory.OTHER
        assert f.normalized_severity() == IncidentSeverity.LOW

    def test_finding_normalization_valid(self) -> None:
        f = Finding("weapons", "critical", "t", "d")
        assert f.normalized_category() == IncidentCategory.WEAPONS
        assert f.normalized_severity() == IncidentSeverity.CRITICAL

    def test_worker_task_worker_type(self) -> None:
        task = WorkerTask.from_payload(
            {"case_id": "c1", "type": "audio", "index": 0, "source_file": "a"}
        )
        assert task.worker_type == WorkerType.AUDIO

    def test_case_info(self) -> None:
        info = CaseInfo("c1", 3, CaseStatus.QUEUED, total_text_items=3)
        assert info.total_items == 3


# ─────────────────────────────────────────────────────
# Metadata de imagen / audio (sin libs pesadas)
# ─────────────────────────────────────────────────────
class TestMediaMeta:
    def test_image_meta_reads_size(self, tmp_path) -> None:
        f = tmp_path / "x.png"
        f.write_bytes(b"\x89PNG\r\n\x1a\n" + b"0" * 100)
        meta = _read_image_meta(str(f))
        assert meta["size_bytes"] == 108

    def test_image_meta_missing_file(self) -> None:
        meta = _read_image_meta("no/existe.png")
        assert meta["size_bytes"] is None

    def test_image_heuristic_analyzer(self) -> None:
        task = WorkerTask.from_payload({
            "case_id": "c1", "type": "image", "index": 0,
            "source_file": "foto.jpg", "caption": "una pistola sobre la mesa",
        })
        analysis = HeuristicImageAnalyzer().run(task)
        assert analysis.data["engine"] == "heuristic"
        assert any(f.category == "weapons" for f in analysis.findings)

    def test_build_image_analyzer_defaults_heuristic(self) -> None:
        # Sin ultralytics instalado, debe caer al heurístico.
        assert isinstance(build_image_analyzer(), HeuristicImageAnalyzer)

    def test_estimate_duration(self, tmp_path) -> None:
        f = tmp_path / "a.mp3"
        f.write_bytes(b"0" * 32_000)
        assert _estimate_duration(str(f)) == 2.0

    def test_estimate_duration_missing(self) -> None:
        assert _estimate_duration("no/existe.mp3") is None

    def test_audio_heuristic_without_transcript(self) -> None:
        task = WorkerTask.from_payload({
            "case_id": "c1", "type": "audio", "index": 0, "source_file": "a.mp3",
        })
        analysis = HeuristicAudioAnalyzer().run(task)
        assert analysis.data["transcript_length"] == 0
        assert analysis.findings == []

    def test_build_audio_analyzer_defaults_heuristic(self) -> None:
        assert isinstance(build_audio_analyzer(), HeuristicAudioAnalyzer)


# ─────────────────────────────────────────────────────
# Estrategia LIFO + snapshot
# ─────────────────────────────────────────────────────
class TestStrategyExtra:
    def test_lifo(self) -> None:
        q = PriorityTaskQueue(LIFOStrategy())
        for i in range(3):
            q.push(ScheduledTask(payload={"i": i}))
        assert [q.pop().payload["i"] for _ in range(3)] == [2, 1, 0]

    def test_snapshot_is_nondestructive(self) -> None:
        q = PriorityTaskQueue()
        q.push(ScheduledTask(payload={"i": 0}))
        q.push(ScheduledTask(payload={"i": 1}))
        ordered = q.snapshot()
        assert [t.payload["i"] for t in ordered] == [0, 1]
        assert len(q) == 2  # no se consumió

    def test_peek(self) -> None:
        q = PriorityTaskQueue()
        assert q.peek() is None
        q.push(ScheduledTask(payload={"i": 7}))
        assert q.peek().payload["i"] == 7
        assert len(q) == 1


# ─────────────────────────────────────────────────────
# Decoradores auxiliares
# ─────────────────────────────────────────────────────
class TestDecoratorsExtra:
    async def test_timing_records_last_ms(self) -> None:
        seen = {}

        async def core(x: int) -> int:
            await asyncio.sleep(0.01)
            return x

        proc = TimingProcessor(
            FunctionProcessor(core), on_timing=lambda ms: seen.update(ms=ms)
        )
        assert await proc.process(5) == 5
        assert proc.last_ms is not None and proc.last_ms > 0
        assert "ms" in seen

    async def test_logging_passthrough_and_error(self, caplog) -> None:
        async def core(x: int) -> int:
            if x < 0:
                raise ValueError("neg")
            return x

        proc = LoggingProcessor(
            FunctionProcessor(core), describe=lambda i: f"item-{i}"
        )
        with caplog.at_level(logging.INFO, logger="workers.pipeline"):
            assert await proc.process(3) == 3
        with pytest.raises(ValueError):
            await proc.process(-1)


# ─────────────────────────────────────────────────────
# Factory: helpers de construcción
# ─────────────────────────────────────────────────────
class TestFactoryHelpers:
    def test_build_consumer_dev_fallback(self) -> None:
        # Sin Kafka real (Persona C), devuelve un InMemoryConsumer.
        consumer = build_consumer(["analysis.text.tasks"])
        assert isinstance(consumer, InMemoryConsumer)

    def test_create_consolidation_worker(self) -> None:
        from src.workers.stores import InMemoryStore

        store = InMemoryStore()
        cw = create_consolidation_worker(
            consumer=InMemoryConsumer(), reader=store, writer=store
        )
        assert cw.is_running is False
        assert cw.stats()["consolidated"] == 0
