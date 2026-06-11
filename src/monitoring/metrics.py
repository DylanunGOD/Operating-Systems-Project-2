"""
monitoring/metrics.py — Métricas Prometheus.

Entregable de Persona C (FASE 3). ``PrometheusMetrics`` implementa el Protocol
``MetricsRecorder`` de los decoradores de B (``increment`` / ``observe``), así
que se inyecta directo en los workers y en el pipeline de procesamiento:

    worker = TextWorker(consumer=..., store=..., metrics=get_metrics())

Mapea los nombres con puntos del código (``worker.results``, ``processor.latency_ms``)
a métricas Prometheus válidas (Counter / Histogram), creándolas perezosamente.
``prometheus_client`` se importa de forma PEREZOSA: sin la lib, ``get_metrics``
devuelve un ``NoopMetrics`` (degradación elegante).

Exposición:
- API:     monta ``metrics_asgi_app()`` en ``/metrics``.
- Workers: llaman ``start_metrics_server(port)`` (servidor HTTP dedicado).
"""

from __future__ import annotations

import logging
from typing import Any

from src.patterns.decorators import NoopMetrics

logger = logging.getLogger("monitoring.metrics")

# Buckets de latencia en ms pensados para análisis ML (de 5ms a ~30s).
_LATENCY_BUCKETS_MS = (
    5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000,
)


def _load_prometheus() -> Any:
    import prometheus_client  # type: ignore

    return prometheus_client


def _sanitize(name: str) -> str:
    """``worker.results`` → ``worker_results`` (nombre Prometheus válido)."""
    out = []
    for ch in name:
        out.append(ch if (ch.isalnum() or ch == "_") else "_")
    safe = "".join(out).strip("_")
    # Un nombre no puede empezar por dígito.
    if safe and safe[0].isdigit():
        safe = f"_{safe}"
    return safe or "unnamed"


class PrometheusMetrics:
    """
    Sink de métricas respaldado por ``prometheus_client``. Cumple el contrato
    ``MetricsRecorder`` (increment/observe). Crea Counters/Histograms bajo
    demanda; asume que un mismo nombre de métrica usa siempre el mismo conjunto
    de etiquetas (cierto en este sistema: ``worker=...``).
    """

    def __init__(self, *, registry: Any = None, namespace: str = "analysis") -> None:
        self._prom = _load_prometheus()
        self._registry = registry if registry is not None else self._prom.REGISTRY
        self._namespace = namespace
        self._counters: dict[str, Any] = {}
        self._histograms: dict[str, Any] = {}

    # ── MetricsRecorder ──────────────────────────────
    def increment(self, name: str, value: int = 1, **tags: str) -> None:
        counter = self._get_counter(name, tuple(sorted(tags)))
        (counter.labels(**tags) if tags else counter).inc(value)

    def observe(self, name: str, value: float, **tags: str) -> None:
        hist = self._get_histogram(name, tuple(sorted(tags)))
        (hist.labels(**tags) if tags else hist).observe(value)

    def set_gauge(self, name: str, value: float, **tags: str) -> None:
        gauge = self._get_gauge(name, tuple(sorted(tags)))
        (gauge.labels(**tags) if tags else gauge).set(value)

    # ── Creación perezosa ────────────────────────────
    def _get_counter(self, name: str, label_keys: tuple[str, ...]) -> Any:
        metric = self._counters.get(name)
        if metric is None:
            metric = self._prom.Counter(
                _sanitize(name),
                name,
                labelnames=label_keys,
                namespace=self._namespace,
                registry=self._registry,
            )
            self._counters[name] = metric
        return metric

    def _get_histogram(self, name: str, label_keys: tuple[str, ...]) -> Any:
        metric = self._histograms.get(name)
        if metric is None:
            buckets = (
                _LATENCY_BUCKETS_MS
                if "ms" in name or "latency" in name
                else self._prom.Histogram.DEFAULT_BUCKETS
            )
            metric = self._prom.Histogram(
                _sanitize(name),
                name,
                labelnames=label_keys,
                namespace=self._namespace,
                registry=self._registry,
                buckets=buckets,
            )
            self._histograms[name] = metric
        return metric

    def _get_gauge(self, name: str, label_keys: tuple[str, ...]) -> Any:
        # Los gauges se guardan junto a los counters (mismo dict-namespace lógico).
        metric = self._counters.get(f"gauge:{name}")
        if metric is None:
            metric = self._prom.Gauge(
                _sanitize(name),
                name,
                labelnames=label_keys,
                namespace=self._namespace,
                registry=self._registry,
            )
            self._counters[f"gauge:{name}"] = metric
        return metric


# ─────────────────────────────────────────────────────
# Exposición HTTP
# ─────────────────────────────────────────────────────
def metrics_asgi_app() -> Any:
    """App ASGI para montar en ``/metrics`` (la usa la API FastAPI)."""
    return _load_prometheus().make_asgi_app()


def start_metrics_server(port: int) -> None:  # pragma: no cover - efecto de red
    """Arranca un servidor HTTP de métricas (lo usan los workers)."""
    _load_prometheus().start_http_server(port)
    logger.info("Servidor de métricas Prometheus escuchando en :%d/metrics", port)


# ─────────────────────────────────────────────────────
# Singleton
# ─────────────────────────────────────────────────────
_metrics: Any = None


def get_metrics() -> Any:
    """
    Singleton del sink de métricas. ``PrometheusMetrics`` si la lib está
    instalada; si no, ``NoopMetrics`` (no rompe en dev/tests sin Prometheus).
    """
    global _metrics
    if _metrics is None:
        try:
            _metrics = PrometheusMetrics()
        except ImportError:
            logger.warning("prometheus_client no instalado; uso NoopMetrics")
            _metrics = NoopMetrics()
    return _metrics


__all__ = [
    "PrometheusMetrics",
    "get_metrics",
    "metrics_asgi_app",
    "start_metrics_server",
]
