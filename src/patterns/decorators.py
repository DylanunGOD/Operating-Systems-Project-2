"""
Patrón Decorator — añadir capacidades al pipeline de procesamiento sin
duplicar código ni tocar la lógica de análisis.

PROBLEMA:
    Todo worker quiere, alrededor de su análisis "puro", lo mismo: medir
    cuánto tardó, loguear inicio/fin/errores, exponer métricas, reintentar
    fallos transitorios, cachear resultados repetidos. Si cada worker lo
    implementa por su cuenta, se duplica y se desincroniza.

SOLUCIÓN:
    Modelar el análisis como un `Processor` (algo con `process(item)`) y
    *envolverlo* en decoradores que comparten esa misma interfaz. Cada
    decorador agrega UNA capacidad y delega el resto en el envuelto. Como
    todos cumplen el mismo contrato, se componen como cebollas:

        pipeline = CachingProcessor(
            LoggingProcessor(
                MetricsProcessor(
                    RetryProcessor(
                        TimingProcessor(core)))))

    `core` no sabe que está decorado; los decoradores no saben qué hace
    `core`. Total desacoplamiento.

EN ESTE PROYECTO:
    Los workers de FASE 2 envuelven su `analyze` con este pipeline vía el
    helper `build_pipeline(...)`. El `Cache` por defecto es en memoria;
    Persona C puede inyectar uno respaldado por Redis sin cambiar nada más.
"""

from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import (
    Any,
    Awaitable,
    Callable,
    Generic,
    Protocol,
    TypeVar,
)

I = TypeVar("I")
O = TypeVar("O")

logger = logging.getLogger("workers.pipeline")


# ─────────────────────────────────────────────────────
# Contrato base
# ─────────────────────────────────────────────────────
class Processor(ABC, Generic[I, O]):
    """Algo que transforma un item de entrada en un resultado, async."""

    @abstractmethod
    async def process(self, item: I) -> O: ...


class FunctionProcessor(Processor[I, O]):
    """
    Adapta una función `async def f(item) -> result` a un Processor, para no
    tener que crear una subclase cuando el core ya es una función.
    """

    def __init__(self, fn: Callable[[I], Awaitable[O]], *, name: str = "core") -> None:
        self._fn = fn
        self.name = name

    async def process(self, item: I) -> O:
        return await self._fn(item)


class ProcessorDecorator(Processor[I, O]):
    """
    Base de todos los decoradores: envuelve un `inner` Processor y por
    defecto delega. Las subclases sobrescriben `process` para agregar su
    capacidad antes/después/alrededor de `self._inner.process(item)`.
    """

    def __init__(self, inner: Processor[I, O]) -> None:
        self._inner = inner

    @property
    def inner(self) -> Processor[I, O]:
        return self._inner

    async def process(self, item: I) -> O:
        return await self._inner.process(item)


# ─────────────────────────────────────────────────────
# Puertos (contratos que C puede reimplementar)
# ─────────────────────────────────────────────────────
class MetricsRecorder(Protocol):
    """Sink de métricas. Por defecto, una implementación en memoria."""

    def increment(self, name: str, value: int = 1, **tags: str) -> None: ...
    def observe(self, name: str, value: float, **tags: str) -> None: ...


class Cache(Protocol):
    """Caché async. Por defecto, una implementación en memoria con TTL."""

    async def get(self, key: str) -> Any | None: ...
    async def set(self, key: str, value: Any, ttl: float | None = None) -> None: ...


# ─────────────────────────────────────────────────────
# Implementaciones por defecto de los puertos
# ─────────────────────────────────────────────────────
class InMemoryMetrics:
    """Registra contadores y observaciones en diccionarios. Útil en tests."""

    def __init__(self) -> None:
        self.counters: dict[str, int] = {}
        self.observations: dict[str, list[float]] = {}

    def increment(self, name: str, value: int = 1, **tags: str) -> None:
        key = _metric_key(name, tags)
        self.counters[key] = self.counters.get(key, 0) + value

    def observe(self, name: str, value: float, **tags: str) -> None:
        key = _metric_key(name, tags)
        self.observations.setdefault(key, []).append(value)


class NoopMetrics:
    """Descarta todo. Default cuando no interesa medir."""

    def increment(self, name: str, value: int = 1, **tags: str) -> None:
        return None

    def observe(self, name: str, value: float, **tags: str) -> None:
        return None


_CACHE_MISS = object()


class InMemoryCache:
    """
    Caché LRU en memoria con TTL opcional. Pensada para dev/tests y como
    contrato de referencia; Persona C la sustituye por Redis en FASE 3.
    """

    def __init__(self, *, max_size: int = 1024) -> None:
        self._max = max_size
        self._data: OrderedDict[str, tuple[Any, float | None]] = OrderedDict()
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Any | None:
        async with self._lock:
            entry = self._data.get(key, _CACHE_MISS)
            if entry is _CACHE_MISS:
                return None
            value, expires_at = entry  # type: ignore[misc]
            if expires_at is not None and time.monotonic() >= expires_at:
                self._data.pop(key, None)
                return None
            self._data.move_to_end(key)  # marca como recientemente usado
            return value

    async def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        async with self._lock:
            expires_at = time.monotonic() + ttl if ttl else None
            self._data[key] = (value, expires_at)
            self._data.move_to_end(key)
            while len(self._data) > self._max:
                self._data.popitem(last=False)  # desaloja el menos usado


# ─────────────────────────────────────────────────────
# Decoradores concretos
# ─────────────────────────────────────────────────────
class TimingProcessor(ProcessorDecorator[I, O]):
    """Mide el tiempo de `process` y lo reporta vía callback / `last_ms`."""

    def __init__(
        self,
        inner: Processor[I, O],
        *,
        on_timing: Callable[[float], None] | None = None,
    ) -> None:
        super().__init__(inner)
        self._on_timing = on_timing
        self.last_ms: float | None = None

    async def process(self, item: I) -> O:
        start = time.perf_counter()
        try:
            return await self._inner.process(item)
        finally:
            self.last_ms = (time.perf_counter() - start) * 1000.0
            if self._on_timing is not None:
                try:
                    self._on_timing(self.last_ms)
                except Exception:
                    pass


class LoggingProcessor(ProcessorDecorator[I, O]):
    """Loguea inicio, fin y errores, con un descriptor opcional del item."""

    def __init__(
        self,
        inner: Processor[I, O],
        *,
        log: logging.Logger | None = None,
        describe: Callable[[I], str] | None = None,
        level: int = logging.INFO,
    ) -> None:
        super().__init__(inner)
        self._log = log or logger
        self._describe = describe or (lambda _i: "item")
        self._level = level

    async def process(self, item: I) -> O:
        label = self._safe_describe(item)
        self._log.log(self._level, "procesando %s", label)
        try:
            result = await self._inner.process(item)
        except Exception as e:
            self._log.error("falló %s: %s: %s", label, type(e).__name__, e)
            raise
        self._log.log(self._level, "completado %s", label)
        return result

    def _safe_describe(self, item: I) -> str:
        try:
            return self._describe(item)
        except Exception:
            return "item"


class MetricsProcessor(ProcessorDecorator[I, O]):
    """Cuenta procesados/errores y observa la latencia."""

    def __init__(
        self,
        inner: Processor[I, O],
        *,
        metrics: MetricsRecorder | None = None,
        prefix: str = "processor",
        **tags: str,
    ) -> None:
        super().__init__(inner)
        self._metrics = metrics or NoopMetrics()
        self._prefix = prefix
        self._tags = tags

    async def process(self, item: I) -> O:
        start = time.perf_counter()
        try:
            result = await self._inner.process(item)
        except Exception:
            self._metrics.increment(f"{self._prefix}.errors", **self._tags)
            raise
        finally:
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            self._metrics.observe(
                f"{self._prefix}.latency_ms", elapsed_ms, **self._tags
            )
        self._metrics.increment(f"{self._prefix}.processed", **self._tags)
        return result


class RetryProcessor(ProcessorDecorator[I, O]):
    """
    Reintenta ante fallos transitorios con backoff exponencial + jitter
    opcional. Sólo reintenta las excepciones indicadas en `retry_on`.
    """

    def __init__(
        self,
        inner: Processor[I, O],
        *,
        max_attempts: int = 3,
        base_delay: float = 0.1,
        max_delay: float = 5.0,
        retry_on: tuple[type[BaseException], ...] = (Exception,),
        on_retry: Callable[[int, BaseException], None] | None = None,
    ) -> None:
        super().__init__(inner)
        if max_attempts < 1:
            raise ValueError("max_attempts debe ser >= 1")
        self._max_attempts = max_attempts
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._retry_on = retry_on
        self._on_retry = on_retry

    async def process(self, item: I) -> O:
        attempt = 0
        while True:
            attempt += 1
            try:
                return await self._inner.process(item)
            except self._retry_on as e:
                if attempt >= self._max_attempts:
                    raise
                if self._on_retry is not None:
                    try:
                        self._on_retry(attempt, e)
                    except Exception:
                        pass
                delay = min(
                    self._max_delay, self._base_delay * (2 ** (attempt - 1))
                )
                await asyncio.sleep(delay)


class CachingProcessor(ProcessorDecorator[I, O]):
    """
    Memoiza resultados por una clave derivada del item. En un cache hit no
    se llama al inner (se ahorra todo el trabajo aguas abajo).
    """

    def __init__(
        self,
        inner: Processor[I, O],
        *,
        key_fn: Callable[[I], str],
        cache: Cache | None = None,
        ttl: float | None = 300.0,
        metrics: MetricsRecorder | None = None,
    ) -> None:
        super().__init__(inner)
        self._key_fn = key_fn
        self._cache = cache or InMemoryCache()
        self._ttl = ttl
        self._metrics = metrics or NoopMetrics()

    async def process(self, item: I) -> O:
        try:
            key = self._key_fn(item)
        except Exception:
            # Si no se puede derivar la clave, se saltea el cache.
            return await self._inner.process(item)

        cached = await self._cache.get(key)
        if cached is not None:
            self._metrics.increment("cache.hits")
            return cached

        self._metrics.increment("cache.misses")
        result = await self._inner.process(item)
        if result is not None:
            await self._cache.set(key, result, ttl=self._ttl)
        return result


# ─────────────────────────────────────────────────────
# Builder del pipeline
# ─────────────────────────────────────────────────────
@dataclass
class PipelineOptions:
    """Qué capas activar y con qué parámetros."""

    with_caching: bool = False
    with_logging: bool = True
    with_metrics: bool = True
    with_retry: bool = True
    with_timing: bool = True

    # Retry
    max_attempts: int = 3
    base_delay: float = 0.1

    # Caching
    cache_key_fn: Callable[[Any], str] | None = None
    cache_ttl: float | None = 300.0

    # Logging
    describe: Callable[[Any], str] | None = None

    # Dependencias inyectables
    metrics: MetricsRecorder | None = None
    cache: Cache | None = None
    on_timing: Callable[[float], None] | None = None
    tags: dict[str, str] = field(default_factory=dict)


def build_pipeline(
    core: Processor[I, O] | Callable[[I], Awaitable[O]],
    options: PipelineOptions | None = None,
) -> Processor[I, O]:
    """
    Compone el pipeline alrededor de `core` según `options`.

    Orden (de afuera hacia adentro):
        Caching → Logging → Metrics → Retry → Timing → core

    Así un cache hit evita loguear/medir/reintentar, y el Timing mide sólo
    el trabajo real del core en cada intento.
    """
    opts = options or PipelineOptions()

    processor: Processor[I, O] = (
        core if isinstance(core, Processor) else FunctionProcessor(core)
    )

    if opts.with_timing:
        processor = TimingProcessor(processor, on_timing=opts.on_timing)

    if opts.with_retry:
        processor = RetryProcessor(
            processor,
            max_attempts=opts.max_attempts,
            base_delay=opts.base_delay,
        )

    if opts.with_metrics:
        processor = MetricsProcessor(
            processor, metrics=opts.metrics, **opts.tags
        )

    if opts.with_logging:
        processor = LoggingProcessor(processor, describe=opts.describe)

    if opts.with_caching:
        if opts.cache_key_fn is None:
            raise ValueError(
                "with_caching=True requiere cache_key_fn en PipelineOptions"
            )
        processor = CachingProcessor(
            processor,
            key_fn=opts.cache_key_fn,
            cache=opts.cache,
            ttl=opts.cache_ttl,
            metrics=opts.metrics,
        )

    return processor


# ─────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────
def _metric_key(name: str, tags: dict[str, str]) -> str:
    if not tags:
        return name
    suffix = ",".join(f"{k}={v}" for k, v in sorted(tags.items()))
    return f"{name}{{{suffix}}}"
