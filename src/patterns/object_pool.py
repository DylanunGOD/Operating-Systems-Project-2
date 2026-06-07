"""
Patrón Object Pool — reutilización de objetos costosos de crear.

PROBLEMA:
    Algunos objetos son caros de construir: un modelo de spaCy tarda
    segundos en cargarse en memoria, una conexión a Redis hace un
    handshake TCP, un cliente de YOLO reserva memoria de GPU. Crear uno
    por cada tarea que llega a un worker desperdiciaría CPU/RAM y
    dispararía la latencia.

SOLUCIÓN:
    Mantener un *pool* de instancias ya creadas. Cuando un consumidor
    necesita una, la pide prestada (`acquire`); cuando termina, la
    devuelve (`release`) para que otro la reuse. El pool limita el total
    de instancias vivas (`max_size`) para acotar el consumo de recursos.

EN ESTE PROYECTO:
    - Los workers de FASE 2 lo usan para reutilizar modelos de análisis
      (un único modelo de NER compartido por todas las tareas de texto).
    - Persona C puede usarlo para poolear conexiones a Redis.
    - La capa de BD ya usa un Object Pool implícito (el pool interno de
      SQLAlchemy); este módulo lo hace explícito para el resto del sistema.

USO TÍPICO:

    pool = ObjectPool(factory=lambda: load_spacy_model(), max_size=4)
    await pool.warmup()                 # opcional: pre-carga min_size

    async with pool.lease() as model:   # toma prestado, devuelve solo
        doc = model("texto a analizar")

CONTRATO:
    - `factory` puede ser síncrona o async (se await-ea si hace falta).
    - `validator(obj) -> bool` (opcional): descarta objetos rotos al
      tomarlos prestados (ej: conexión caída).
    - `on_release(obj)` (opcional): resetea estado antes de devolver.
    - `on_close(obj)` (opcional): libera el objeto al cerrar el pool.
"""

from __future__ import annotations

import asyncio
import inspect
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Generic,
    TypeVar,
)

T = TypeVar("T")

# Una factory/callback puede ser sync o async.
Factory = Callable[[], "T | Awaitable[T]"]
Validator = Callable[[T], "bool | Awaitable[bool]"]
Hook = Callable[[T], "None | Awaitable[None]"]


# ─────────────────────────────────────────────────────
# Excepciones
# ─────────────────────────────────────────────────────
class PoolExhaustedError(Exception):
    """No hubo objeto disponible dentro del timeout configurado."""

    def __init__(self, max_size: int, timeout: float) -> None:
        super().__init__(
            f"Pool agotado: las {max_size} instancias estan en uso y no se "
            f"liberó ninguna en {timeout:.1f}s."
        )


class PoolClosedError(Exception):
    """Se intentó usar el pool después de cerrarlo."""

    def __init__(self) -> None:
        super().__init__("El pool está cerrado; no admite nuevos acquire().")


# ─────────────────────────────────────────────────────
# Estadísticas
# ─────────────────────────────────────────────────────
@dataclass(frozen=True)
class PoolStats:
    """Foto del estado del pool (para métricas/health)."""

    size: int          # total de instancias vivas (en uso + disponibles)
    in_use: int        # prestadas ahora mismo
    available: int     # listas para prestar
    max_size: int
    created_total: int  # cuántas se crearon en toda la vida del pool

    @property
    def utilization(self) -> float:
        """Fracción [0,1] de la capacidad que está en uso."""
        if self.max_size == 0:
            return 0.0
        return round(self.in_use / self.max_size, 4)


# ─────────────────────────────────────────────────────
# ObjectPool
# ─────────────────────────────────────────────────────
class ObjectPool(Generic[T]):
    """
    Pool genérico de objetos reutilizables, seguro para múltiples
    corrutinas en el mismo event loop.

    Política:
    - Al pedir un objeto se reutiliza uno disponible si lo hay.
    - Si no hay disponibles y no se alcanzó `max_size`, se crea uno nuevo.
    - Si se alcanzó `max_size`, el pedido espera hasta que alguien devuelva
      uno (o hasta `acquire_timeout` → PoolExhaustedError).
    """

    def __init__(
        self,
        factory: Factory[T],
        *,
        max_size: int = 10,
        min_size: int = 0,
        validator: Validator[T] | None = None,
        on_release: Hook[T] | None = None,
        on_close: Hook[T] | None = None,
        acquire_timeout: float = 30.0,
    ) -> None:
        if max_size <= 0:
            raise ValueError("max_size debe ser >= 1")
        if min_size < 0 or min_size > max_size:
            raise ValueError("min_size debe estar en [0, max_size]")

        self._factory = factory
        self._max_size = max_size
        self._min_size = min_size
        self._validator = validator
        self._on_release = on_release
        self._on_close = on_close
        self._acquire_timeout = acquire_timeout

        self._available: deque[T] = deque()
        self._in_use: set[int] = set()  # id() de objetos prestados
        self._created_total = 0
        self._closed = False
        self._cond = asyncio.Condition()

    # ─────────────────────────────────────────
    # API principal
    # ─────────────────────────────────────────
    async def acquire(self) -> T:
        """
        Toma prestado un objeto del pool. Bloquea (await) si todas las
        instancias están en uso, hasta que se libere una o expire el timeout.
        """
        if self._closed:
            raise PoolClosedError()

        loop = asyncio.get_event_loop()
        deadline = loop.time() + self._acquire_timeout

        async with self._cond:
            while True:
                if self._closed:
                    raise PoolClosedError()

                # 1) Reutilizar un objeto disponible y válido.
                obj = await self._take_valid_available()
                if obj is not None:
                    self._in_use.add(id(obj))
                    return obj

                # 2) Crear uno nuevo si hay capacidad.
                if self._live_count() < self._max_size:
                    obj = await self._make()
                    self._in_use.add(id(obj))
                    return obj

                # 3) Esperar a que alguien devuelva uno.
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise PoolExhaustedError(self._max_size, self._acquire_timeout)
                try:
                    await asyncio.wait_for(self._cond.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    raise PoolExhaustedError(
                        self._max_size, self._acquire_timeout
                    ) from None

    async def release(self, obj: T) -> None:
        """
        Devuelve un objeto al pool. Es idempotente frente a objetos que ya
        no pertenecen al pool (los ignora). Si el pool está cerrado, libera
        el objeto en lugar de guardarlo.
        """
        async with self._cond:
            self._in_use.discard(id(obj))

            if self._closed:
                await self._destroy(obj)
                self._cond.notify_all()
                return

            # Resetear estado antes de reutilizar.
            if self._on_release is not None:
                try:
                    await _maybe_await(self._on_release(obj))
                except Exception:
                    # Si el reset falla, descartamos el objeto en vez de
                    # devolver uno potencialmente corrupto al pool.
                    await self._destroy(obj)
                    self._cond.notify()
                    return

            self._available.append(obj)
            self._cond.notify()

    @asynccontextmanager
    async def lease(self) -> AsyncIterator[T]:
        """
        Context manager que toma prestado y devuelve automáticamente,
        incluso si el bloque lanza excepción.

            async with pool.lease() as conn:
                await conn.do_something()
        """
        obj = await self.acquire()
        try:
            yield obj
        finally:
            await self.release(obj)

    # ─────────────────────────────────────────
    # Ciclo de vida
    # ─────────────────────────────────────────
    async def warmup(self, count: int | None = None) -> int:
        """
        Pre-crea instancias (hasta `count`, por defecto `min_size`) para que
        las primeras tareas no paguen el costo de construcción.
        Devuelve cuántas creó.
        """
        target = self._min_size if count is None else count
        created = 0
        async with self._cond:
            while (
                not self._closed
                and len(self._available) < target
                and self._live_count() < self._max_size
            ):
                obj = await self._make()
                self._available.append(obj)
                created += 1
            self._cond.notify_all()
        return created

    async def close(self) -> None:
        """
        Cierra el pool: destruye los objetos disponibles y marca el pool
        como cerrado. Los objetos aún prestados se destruyen al devolverse.
        """
        async with self._cond:
            self._closed = True
            while self._available:
                await self._destroy(self._available.popleft())
            self._cond.notify_all()

    # ─────────────────────────────────────────
    # Inspección
    # ─────────────────────────────────────────
    def stats(self) -> PoolStats:
        return PoolStats(
            size=self._live_count(),
            in_use=len(self._in_use),
            available=len(self._available),
            max_size=self._max_size,
            created_total=self._created_total,
        )

    @property
    def is_closed(self) -> bool:
        return self._closed

    # ─────────────────────────────────────────
    # Internos (asumen el lock tomado, salvo donde se indique)
    # ─────────────────────────────────────────
    def _live_count(self) -> int:
        return len(self._available) + len(self._in_use)

    async def _take_valid_available(self) -> T | None:
        """Saca de `available` el primer objeto que pase el validador."""
        while self._available:
            candidate = self._available.popleft()
            if await self._is_valid(candidate):
                return candidate
            # Inválido: destruirlo y seguir buscando (libera una ranura).
            await self._destroy(candidate)
        return None

    async def _is_valid(self, obj: T) -> bool:
        if self._validator is None:
            return True
        try:
            return bool(await _maybe_await(self._validator(obj)))
        except Exception:
            return False

    async def _make(self) -> T:
        """
        Crea una instancia nueva. Se llama con el lock tomado: para factories
        muy lentas conviene `warmup()` en el arranque para no serializar la
        creación bajo carga.
        """
        obj = await _maybe_await(self._factory())
        self._created_total += 1
        return obj

    async def _destroy(self, obj: T) -> None:
        if self._on_close is not None:
            try:
                await _maybe_await(self._on_close(obj))
            except Exception:
                pass  # liberar nunca debe tumbar al pool


# ─────────────────────────────────────────────────────
# Helper
# ─────────────────────────────────────────────────────
async def _maybe_await(value: Any) -> Any:
    """Await-ea el valor si es awaitable; si no, lo devuelve tal cual."""
    if inspect.isawaitable(value):
        return await value
    return value
