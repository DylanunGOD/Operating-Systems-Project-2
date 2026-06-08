"""
Tests unitarios de los workers de FASE 2 (Persona B):
BaseWorker, TextWorker, ImageWorker, AudioWorker, ConsolidationWorker,
WorkerFactory y los stores en memoria.

Todo corre sin Kafka ni PostgreSQL reales (fakes de InMemory*).
asyncio_mode=auto (ver pyproject) → los `async def test_*` corren solos.
"""

from __future__ import annotations

import asyncio

import pytest

from src.models.case import CaseStatus
from src.workers.base_worker import BaseWorker
from src.workers.consolidation_worker import ConsolidationWorker, build_incidents
from src.workers.contracts import (
    Finding,
    ResultRecord,
    WorkerAnalysis,
    WorkerTask,
)
from src.workers.messaging import InMemoryConsumer
from src.workers.stores import InMemoryStore, serialize_analysis
from src.workers.audio_worker import AudioWorker
from src.workers.image_worker import ImageWorker
from src.workers.text_worker import TextWorker
from src.workers.worker_factory import (
    UnknownWorkerTypeError,
    WorkerFactory,
    build_default_factory,
    create_worker,
)


# ─────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────
def _payload(case_id: str, ctype: str, index: int = 0, **extra) -> dict:
    return {
        "case_id": case_id,
        "type": ctype,
        "index": index,
        "source_file": extra.pop("source_file", f"{ctype}_{index}.dat"),
        **extra,
    }


async def _process(worker: BaseWorker, consumer: InMemoryConsumer, payload: dict):
    msg = await consumer.feed(f"analysis.{payload['type']}.tasks", payload)
    return await worker.process_message(msg)


# ═════════════════════════════════════════════════════
# WorkerTask (parseo del payload del Splitter de A)
# ═════════════════════════════════════════════════════
class TestWorkerTask:
    def test_from_payload_ok(self) -> None:
        task = WorkerTask.from_payload(_payload("c1", "text", 2, priority="high"))
        assert task.case_id == "c1"
        assert task.content_type == "text"
        assert task.index == 2
        assert task.part_id == "text:2"
        assert task.priority == "high"

    def test_missing_case_id(self) -> None:
        with pytest.raises(ValueError):
            WorkerTask.from_payload({"type": "text"})

    def test_invalid_type(self) -> None:
        with pytest.raises(ValueError):
            WorkerTask.from_payload({"case_id": "c1", "type": "video"})


# ═════════════════════════════════════════════════════
# BaseWorker (flujo común)
# ═════════════════════════════════════════════════════
class _DummyWorker(BaseWorker):
    content_type = "text"

    def __init__(self, *, fail: bool = False, **kwargs) -> None:
        super().__init__(**kwargs)
        self._fail = fail

    def analyze(self, task: WorkerTask) -> WorkerAnalysis:
        if self._fail:
            raise RuntimeError("analyze reventó")
        return WorkerAnalysis(data={"ok": True}, summary="dummy", confidence_score=0.5)


class TestBaseWorker:
    async def test_process_persists_and_advances_progress(self) -> None:
        consumer = InMemoryConsumer()
        store = InMemoryStore()
        store.seed_case("c1", text=2)
        worker = _DummyWorker(consumer=consumer, store=store)

        outcome = await _process(worker, consumer, _payload("c1", "text", 0))
        assert outcome.ok
        assert len(store.results["c1"]) == 1
        assert store.cases["c1"]["processed_text_items"] == 1
        # QUEUED → PROCESSING en el primer resultado
        assert store.cases["c1"]["status"] == CaseStatus.PROCESSING

    async def test_run_forever_consumes_and_commits(self) -> None:
        consumer = InMemoryConsumer()
        store = InMemoryStore()
        store.seed_case("c1", text=1)
        worker = _DummyWorker(consumer=consumer, store=store, poll_timeout=0.05)

        await consumer.feed("analysis.text.tasks", _payload("c1", "text", 0))
        task = asyncio.create_task(worker.run_forever())
        await asyncio.sleep(0.2)
        worker.stop()
        await asyncio.wait_for(task, timeout=2)

        assert worker.stats()["ok"] == 1
        assert len(consumer.committed) == 1

    async def test_error_is_isolated(self) -> None:
        consumer = InMemoryConsumer()
        store = InMemoryStore()
        store.seed_case("c1", text=1)
        worker = _DummyWorker(consumer=consumer, store=store, fail=True, poll_timeout=0.05)

        await consumer.feed("analysis.text.tasks", _payload("c1", "text", 0))
        task = asyncio.create_task(worker.run_forever())
        await asyncio.sleep(0.2)
        worker.stop()
        await asyncio.wait_for(task, timeout=2)

        assert worker.stats()["failed"] == 1
        assert len(consumer.committed) == 1  # se confirma igual (skip poison)

    async def test_requires_content_type(self) -> None:
        class Bad(BaseWorker):
            def analyze(self, task):  # type: ignore[override]
                return WorkerAnalysis()

        with pytest.raises(TypeError):
            Bad(consumer=InMemoryConsumer(), store=InMemoryStore())


# ═════════════════════════════════════════════════════
# Workers especializados (camino heurístico)
# ═════════════════════════════════════════════════════
class TestSpecializedWorkers:
    async def test_text_detects_threats_and_weapons(self) -> None:
        consumer = InMemoryConsumer()
        store = InMemoryStore()
        store.seed_case("c1", text=1)
        worker = TextWorker(consumer=consumer, store=store)
        await worker.on_start()
        await _process(worker, consumer, _payload(
            "c1", "text", 0, source_file="x.txt", content="te voy a matar con un cuchillo"
        ))
        await worker.on_stop()

        data = store.results["c1"][0].data
        cats = {f["category"] for f in data["findings"]}
        assert "threats" in cats
        assert "weapons" in cats
        assert data["engine"] == "heuristic"

    async def test_text_reads_file(self, tmp_path) -> None:
        f = tmp_path / "msg.txt"
        f.write_text("mensaje con amenaza de bomba", encoding="utf-8")
        consumer = InMemoryConsumer()
        store = InMemoryStore()
        store.seed_case("c1", text=1)
        worker = TextWorker(consumer=consumer, store=store)
        await worker.on_start()
        await _process(worker, consumer, _payload("c1", "text", 0, source_file=str(f)))
        await worker.on_stop()
        cats = {f["category"] for f in store.results["c1"][0].data["findings"]}
        assert "weapons" in cats  # 'bomba'

    async def test_image_filename_heuristic(self) -> None:
        consumer = InMemoryConsumer()
        store = InMemoryStore()
        store.seed_case("c1", image=1)
        worker = ImageWorker(consumer=consumer, store=store)
        await worker.on_start()
        await _process(worker, consumer, _payload(
            "c1", "image", 0, source_file="pistola.jpg", original_name="pistola.jpg"
        ))
        await worker.on_stop()
        cats = {f["category"] for f in store.results["c1"][0].data["findings"]}
        assert "weapons" in cats

    async def test_audio_analyzes_transcript(self) -> None:
        consumer = InMemoryConsumer()
        store = InMemoryStore()
        store.seed_case("c1", audio=1)
        worker = AudioWorker(consumer=consumer, store=store)
        await worker.on_start()
        await _process(worker, consumer, _payload(
            "c1", "audio", 0, source_file="a.mp3",
            transcript="voy a vender droga", language="es",
        ))
        await worker.on_stop()
        data = store.results["c1"][0].data
        assert data["transcript_length"] > 0
        cats = {f["category"] for f in data["findings"]}
        assert "suspicious_activity" in cats


# ═════════════════════════════════════════════════════
# Stores
# ═════════════════════════════════════════════════════
class TestStores:
    def test_serialize_analysis(self) -> None:
        analysis = WorkerAnalysis(
            data={"sentiment": "negative"},
            findings=[Finding("threats", "high", "t", "d", 0.9, "x.txt")],
            summary="s",
        )
        out = serialize_analysis(analysis)
        assert out["sentiment"] == "negative"
        assert out["summary"] == "s"
        assert out["findings"][0]["category"] == "threats"

    async def test_inmemory_store_feeds_sink(self) -> None:
        sink = InMemoryConsumer()
        store = InMemoryStore(results_sink=sink)
        store.seed_case("c1", text=1)
        task = WorkerTask.from_payload(_payload("c1", "text", 0))
        await store.record(task, WorkerAnalysis(summary="x"), processing_time_ms=5)
        assert sink.pending == 1  # emitió el evento de completitud


# ═════════════════════════════════════════════════════
# WorkerFactory
# ═════════════════════════════════════════════════════
class TestWorkerFactory:
    def test_create_known_types(self) -> None:
        factory = build_default_factory()
        consumer = InMemoryConsumer()
        store = InMemoryStore()
        for ctype, cls in (("text", TextWorker), ("image", ImageWorker), ("audio", AudioWorker)):
            worker = factory.create(ctype, consumer=consumer, store=store)
            assert isinstance(worker, cls)

    def test_unknown_type_raises(self) -> None:
        factory = build_default_factory()
        with pytest.raises(UnknownWorkerTypeError):
            factory.create("video", consumer=InMemoryConsumer(), store=InMemoryStore())

    def test_register_new_type_open_closed(self) -> None:
        factory = WorkerFactory()
        factory.register("text", TextWorker)
        assert factory.available_types() == ["text"]
        factory.register("image", ImageWorker)
        assert "image" in factory.available_types()

    def test_create_worker_wires_bulkhead(self) -> None:
        worker = create_worker(
            "text", consumer=InMemoryConsumer(), store=InMemoryStore()
        )
        assert worker.content_type == "text"


# ═════════════════════════════════════════════════════
# ConsolidationWorker
# ═════════════════════════════════════════════════════
class TestConsolidation:
    def test_build_incidents_groups_by_category(self) -> None:
        records = [
            ResultRecord(
                "c1", "text", "a.txt", 0,
                {"findings": [
                    {"category": "threats", "severity": "high", "confidence": 0.8,
                     "source_file": "a.txt", "snippet": "..."},
                    {"category": "weapons", "severity": "medium", "confidence": 0.6,
                     "source_file": "a.txt"},
                ]},
            ),
            ResultRecord(
                "c1", "image", "b.jpg", 0,
                {"findings": [
                    {"category": "weapons", "severity": "critical", "confidence": 0.95,
                     "source_file": "b.jpg"},
                ]},
            ),
        ]
        incidents = build_incidents(records)
        by_cat = {i.category.value: i for i in incidents}
        assert set(by_cat) == {"threats", "weapons"}
        # weapons toma la severidad más alta observada (critical) y 2 evidencias
        assert by_cat["weapons"].severity.value == "critical"
        assert len(by_cat["weapons"].evidences) == 2
        # ordenado por severidad desc → weapons (critical) antes que threats (high)
        assert incidents[0].category.value == "weapons"

    async def test_e2e_complete(self) -> None:
        results = InMemoryConsumer()
        store = InMemoryStore(results_sink=results)
        store.seed_case("c1", text=2)

        tcons = InMemoryConsumer()
        tw = TextWorker(consumer=tcons, store=store)
        await tw.on_start()
        await _process(tw, tcons, _payload("c1", "text", 0, content="te voy a matar"))
        await _process(tw, tcons, _payload("c1", "text", 1, content="tengo una pistola"))
        await tw.on_stop()

        cw = ConsolidationWorker(
            consumer=results, reader=store, writer=store,
            timeout_seconds=3, poll_timeout=0.05,
        )
        task = asyncio.create_task(cw.run_forever())
        for _ in range(60):
            await asyncio.sleep(0.05)
            if "c1" in store.finalized:
                break
        cw.stop()
        await asyncio.wait_for(task, timeout=3)

        status, _ = store.finalized["c1"]
        assert status == CaseStatus.COMPLETED
        assert len(store.incidents["c1"]) >= 1

    async def test_e2e_timeout_partial(self) -> None:
        # El caso espera 3 partes pero sólo llega 1 → la ventana expira.
        results = InMemoryConsumer()
        store = InMemoryStore(results_sink=results)
        store.seed_case("c1", text=3)

        tcons = InMemoryConsumer()
        tw = TextWorker(consumer=tcons, store=store)
        await tw.on_start()
        await _process(tw, tcons, _payload("c1", "text", 0, content="hola mundo"))
        await tw.on_stop()

        cw = ConsolidationWorker(
            consumer=results, reader=store, writer=store,
            timeout_seconds=0.3, poll_timeout=0.05,
        )
        task = asyncio.create_task(cw.run_forever())
        for _ in range(60):
            await asyncio.sleep(0.05)
            if "c1" in store.finalized:
                break
        cw.stop()
        await asyncio.wait_for(task, timeout=3)

        status, error = store.finalized["c1"]
        assert status == CaseStatus.TIMEOUT
        assert error is not None
