"""
Tests de ``monitoring/metrics.py`` (Persona C).

El mapeo nombre-con-puntos → métrica Prometheus se prueba con un registry
aislado. Si ``prometheus_client`` no está instalado, los tests que lo necesitan
se saltan; ``_sanitize`` y ``get_metrics`` se prueban siempre.
"""

from __future__ import annotations

import pytest

from src.monitoring.metrics import _sanitize, get_metrics


class TestSanitize:
    def test_dots_and_dashes_become_underscores(self) -> None:
        assert _sanitize("worker.results") == "worker_results"
        assert _sanitize("processor.latency-ms") == "processor_latency_ms"

    def test_leading_digit_prefixed(self) -> None:
        assert _sanitize("5xx").startswith("_")

    def test_empty_is_named(self) -> None:
        assert _sanitize("...") == "unnamed"


class TestPrometheusMetrics:
    def test_increment_and_observe(self) -> None:
        prom = pytest.importorskip("prometheus_client")
        from src.monitoring.metrics import PrometheusMetrics

        registry = prom.CollectorRegistry()
        metrics = PrometheusMetrics(registry=registry, namespace="test")

        metrics.increment("worker.results", worker="text")
        metrics.increment("worker.results", worker="text")
        assert (
            registry.get_sample_value(
                "test_worker_results_total", {"worker": "text"}
            )
            == 2
        )

        metrics.observe("processor.latency_ms", 12.0, worker="text")
        count = registry.get_sample_value(
            "test_processor_latency_ms_count", {"worker": "text"}
        )
        assert count == 1

    def test_satisfies_metrics_recorder_contract(self) -> None:
        # Debe servir como drop-in del MetricsRecorder de los workers.
        prom = pytest.importorskip("prometheus_client")
        from src.monitoring.metrics import PrometheusMetrics

        metrics = PrometheusMetrics(registry=prom.CollectorRegistry())
        assert hasattr(metrics, "increment") and hasattr(metrics, "observe")
        # Sin etiquetas tampoco debe romper.
        metrics.increment("cache.hits")
        metrics.set_gauge("queue.depth", 7)


def test_get_metrics_is_metrics_like() -> None:
    metrics = get_metrics()
    assert hasattr(metrics, "increment") and hasattr(metrics, "observe")
