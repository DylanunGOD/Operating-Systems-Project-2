"""
Patrón Bulkhead — aislamiento de fallos por compartimentos.

NOMBRE:
    Viene de los mamparos (bulkheads) de un barco: el casco se divide en
    compartimentos estancos para que una vía de agua en uno no hunda todo
    el barco.

PROBLEMA:
    Si los workers de texto, imagen y audio comparten el mismo "pozo" de
    recursos (hilos, conexiones) y el ImageWorker se satura procesando
    imágenes pesadas, agotaría los recursos y bloquearía también a Text y
    Audio. Un fallo localizado se convierte en una caída en cascada.

SOLUCIÓN:
    Darle a cada tipo de trabajo su propio compartimento con un límite de
    concurrencia y una cola de espera acotada. Si un compartimento se
    llena, rechaza trabajo (fail-fast con BulkheadFullError) en lugar de
    consumir recursos de los demás. Así un fallo queda aislado.

DETALLES IMPORTANTES:
    - El trabajo CPU-bound (inferencia de modelos ML) es *bloqueante*. Si
      se corriera directo en el event loop, congelaría toda la API/worker.
      Por eso el Bulkhead descarga las funciones síncronas a un
      ThreadPoolExecutor propio (otro nivel de aislamiento).
    - Las corrutinas (funciones async) se ejecutan en el loop, sólo
      limitadas por el semáforo de concurrencia.

USO TÍPICO (dentro de un worker):

    bulkhead = Bulkhead("text", max_concurrency=4, max_queue=100)
    result = await bulkhead.run(modelo_pesado_sync, texto)   # va a un hilo

    # o con varios compartimentos:
    registry = build_default_registry()
    await registry.get("image").run(analizar_imagen, path)
"""

from __future__ import annotations

import asyncio
import functools
import inspect
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, TypeVar

R = TypeVar("R")


# ─────────────────────────────────────────────────────
# Excepciones
# ─────────────────────────────────────────────────────
class BulkheadFullError(Exception):
    """
    El compartimento está saturado: todos los slots ocupados y la cola de
    espera llena. Se rechaza el trabajo para no propagar la saturación.
    """

    def __init__(self, name: str, max_concurrency: int, max_queue: int) -> None:
        self.name = name
        super().__init__(
            f"Bulkhead '{name}' lleno: {max_concurrency} en ejecución y "
            f"{max_queue} en cola. Trabajo rechazado (fail-fast)."
        )


class BulkheadClosedError(Exception):
    """Se intentó usar un compartimento ya cerrado."""

    def __init__(self, name: str) -> None:
        super().__init__(f"Bulkhead '{name}' está cerrado.")


# ─────────────────────────────────────────────────────
# Estadísticas
# ─────────────────────────────────────────────────────
@dataclass(frozen=True)
class BulkheadStats:
    name: str
    active: int             # ejecutándose ahora
    waiting: int            # esperando un slot
    max_concurrency: int
    max_queue: int | None
    executed: int           # completadas (éxito o error)
    failed: int             # terminaron con excepción
    rejected: int           # rechazadas por estar lleno

    @property
    def saturation(self) -> float:
        """Fracción [0,1] de slots de ejecución ocupados."""
        if self.max_concurrency == 0:
            return 0.0
        return round(self.active / self.max_concurrency, 4)


# ─────────────────────────────────────────────────────
# Bulkhead
# ─────────────────────────────────────────────────────
class Bulkhead:
    """
    Un compartimento aislado con concurrencia limitada y cola acotada.

    Args:
        name: identificador (text/image/audio/...). Usado en logs y métricas.
        max_concurrency: ejecuciones simultáneas permitidas.
        max_queue: cuántas pueden *esperar* un slot. None = espera ilimitada.
                   0 = sin espera (rechaza apenas no hay slot libre).
        thread_name_prefix: prefijo de los hilos del executor.
    """

    def __init__(
        self,
        name: str,
        *,
        max_concurrency: int = 4,
        max_queue: int | None = None,
        thread_name_prefix: str | None = None,
    ) -> None:
        if max_concurrency <= 0:
            raise ValueError("max_concurrency debe ser >= 1")
        if max_queue is not None and max_queue < 0:
            raise ValueError("max_queue debe ser None o >= 0")

        self._name = name
        self._max_concurrency = max_concurrency
        self._max_queue = max_queue
        self._sem = asyncio.Semaphore(max_concurrency)
        self._executor = ThreadPoolExecutor(
            max_workers=max_concurrency,
            thread_name_prefix=thread_name_prefix or f"bulkhead-{name}",
        )

        self._active = 0
        self._waiting = 0
        self._executed = 0
        self._failed = 0
        self._rejected = 0
        self._closed = False

    @property
    def name(self) -> str:
        return self._name

    # ─────────────────────────────────────────
    # Ejecución
    # ─────────────────────────────────────────
    async def run(
        self,
        fn: Callable[..., R | Awaitable[R]],
        *args: Any,
        timeout: float | None = None,
        **kwargs: Any,
    ) -> R:
        """
        Ejecuta `fn` dentro del compartimento, respetando el límite de
        concurrencia. Funciones síncronas se descargan a un hilo; corrutinas
        se await-ean en el loop.

        Lanza:
            BulkheadFullError  si la cola de espera está llena.
            BulkheadClosedError si el compartimento fue cerrado.
            asyncio.TimeoutError si se excede `timeout` (sólo aplica a la
                espera + ejecución de corrutinas; ver caveat en el código).
        """
        if self._closed:
            raise BulkheadClosedError(self._name)

        # ── Control de admisión (atómico: sin await intermedio) ──
        if self._active >= self._max_concurrency:
            if self._max_queue is not None and self._waiting >= self._max_queue:
                self._rejected += 1
                raise BulkheadFullError(
                    self._name, self._max_concurrency, self._max_queue
                )

        self._waiting += 1
        try:
            await self._sem.acquire()
        finally:
            self._waiting -= 1

        self._active += 1
        try:
            coro = self._invoke(fn, *args, **kwargs)
            if timeout is not None:
                return await asyncio.wait_for(coro, timeout=timeout)
            return await coro
        except Exception:
            self._failed += 1
            raise
        finally:
            self._active -= 1
            self._executed += 1
            self._sem.release()

    async def _invoke(
        self, fn: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> Any:
        """Despacha al loop (async) o a un hilo (sync)."""
        if _is_async_callable(fn):
            return await fn(*args, **kwargs)

        # Sync y potencialmente CPU-bound: a un hilo del executor para no
        # bloquear el event loop.
        loop = asyncio.get_event_loop()
        call = functools.partial(fn, *args, **kwargs)
        return await loop.run_in_executor(self._executor, call)

    # ─────────────────────────────────────────
    # Ciclo de vida e inspección
    # ─────────────────────────────────────────
    def close(self) -> None:
        """Cierra el executor. No espera a los hilos en vuelo."""
        self._closed = True
        self._executor.shutdown(wait=False)

    def stats(self) -> BulkheadStats:
        return BulkheadStats(
            name=self._name,
            active=self._active,
            waiting=self._waiting,
            max_concurrency=self._max_concurrency,
            max_queue=self._max_queue,
            executed=self._executed,
            failed=self._failed,
            rejected=self._rejected,
        )

    @property
    def is_closed(self) -> bool:
        return self._closed


# ─────────────────────────────────────────────────────
# Registry de compartimentos
# ─────────────────────────────────────────────────────
class BulkheadRegistry:
    """
    Colección nombrada de Bulkheads. Da a cada tipo de worker su propio
    compartimento y permite consultar las métricas de todos de una vez.
    """

    def __init__(self) -> None:
        self._bulkheads: dict[str, Bulkhead] = {}

    def register(self, bulkhead: Bulkhead) -> Bulkhead:
        if bulkhead.name in self._bulkheads:
            raise ValueError(f"Ya existe un bulkhead '{bulkhead.name}'")
        self._bulkheads[bulkhead.name] = bulkhead
        return bulkhead

    def get_or_create(
        self,
        name: str,
        *,
        max_concurrency: int = 4,
        max_queue: int | None = None,
    ) -> Bulkhead:
        """Devuelve el bulkhead `name`, creándolo si no existe."""
        existing = self._bulkheads.get(name)
        if existing is not None:
            return existing
        return self.register(
            Bulkhead(
                name, max_concurrency=max_concurrency, max_queue=max_queue
            )
        )

    def get(self, name: str) -> Bulkhead:
        try:
            return self._bulkheads[name]
        except KeyError:
            raise KeyError(
                f"No hay bulkhead '{name}'. Registrados: "
                f"{sorted(self._bulkheads)}"
            ) from None

    def has(self, name: str) -> bool:
        return name in self._bulkheads

    def all_stats(self) -> dict[str, BulkheadStats]:
        return {name: bh.stats() for name, bh in self._bulkheads.items()}

    def close_all(self) -> None:
        for bh in self._bulkheads.values():
            bh.close()


# ─────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────
def _is_async_callable(fn: Callable[..., Any]) -> bool:
    """
    True si llamar a `fn` produce una corrutina. Cubre funciones async,
    partials sobre funciones async y objetos con `__call__` async.
    """
    if inspect.iscoroutinefunction(fn):
        return True
    # functools.partial envuelve el target en .func
    target = getattr(fn, "func", None)
    if target is not None and inspect.iscoroutinefunction(target):
        return True
    call = getattr(fn, "__call__", None)
    return bool(call and inspect.iscoroutinefunction(call))


# ─────────────────────────────────────────────────────
# Registry por defecto del proyecto
# ─────────────────────────────────────────────────────
def build_default_registry() -> BulkheadRegistry:
    """
    Crea un compartimento por tipo de worker, aislados entre sí.

    Los límites son conservadores y pensados para 1 proceso; en producción
    el escalado horizontal (réplicas/HPA) lo decide Kubernetes, mientras que
    el bulkhead protege a cada proceso de saturarse internamente.
    """
    registry = BulkheadRegistry()
    registry.register(Bulkhead("text", max_concurrency=8, max_queue=200))
    registry.register(Bulkhead("image", max_concurrency=4, max_queue=100))
    registry.register(Bulkhead("audio", max_concurrency=2, max_queue=50))
    registry.register(
        Bulkhead("consolidation", max_concurrency=4, max_queue=100)
    )
    return registry
