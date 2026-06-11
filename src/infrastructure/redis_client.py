"""
infrastructure/redis_client.py — Cache real con Redis (redis.asyncio).

Entregable de Persona C (FASE 3). Reemplaza la ``InMemoryCache`` de B:

- ``RedisCache``         → cumple el Protocol ``Cache`` de los decoradores de B
  (``async get(key)`` / ``async set(key, value, ttl)``), así que el
  ``CachingProcessor`` lo acepta tal cual. Añade además:
    · ``delete`` / ``exists`` / ``get_or_set`` (patrón cache-aside)
    · ``lock`` (lock distribuido SET NX EX + liberación atómica por token Lua)
    · ``publish`` / ``subscribe`` (pub/sub)
- ``InMemoryRedisCache`` → mismo API, respaldado por dicts. Degradación
  elegante para dev/tests sin Redis (igual que los fakes de A y B).
- ``get_cache()``        → singleton: ``RedisCache`` si ``redis`` está
  instalado, si no el fake en memoria.

``redis`` se importa de forma PEREZOSA: el módulo se importa y testea sin la lib.
"""

from __future__ import annotations

import asyncio
import json
import time
from contextlib import asynccontextmanager
from functools import lru_cache
from typing import Any, AsyncIterator, Awaitable, Callable
from uuid import uuid4

from src.infrastructure.config import get_settings

# Lua: libera el lock solo si el token coincide (evita borrar el lock de otro).
_RELEASE_LOCK_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
else
    return 0
end
"""


class LockNotAcquiredError(RuntimeError):
    """No se pudo adquirir el lock distribuido dentro del tiempo dado."""


def _dumps(value: Any) -> str:
    return json.dumps(value, default=str, separators=(",", ":"))


def _loads(raw: Any) -> Any:
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    return json.loads(raw)


# ─────────────────────────────────────────────────────
# RedisCache (real)
# ─────────────────────────────────────────────────────
class RedisCache:
    """
    Cache async respaldado por Redis. Serializa valores a JSON. Las claves se
    prefijan con ``namespace`` para no colisionar con otros usos del Redis.
    """

    def __init__(
        self,
        *,
        url: str | None = None,
        client: Any = None,
        namespace: str = "analysis",
        default_ttl: float | None = None,
    ) -> None:
        self._url = url or get_settings().redis_url
        self._client = client  # inyectable (tests); si None se crea lazy
        self._namespace = namespace
        self._default_ttl = default_ttl

    # ── Cliente perezoso ─────────────────────────────
    def _ensure_client(self) -> Any:
        if self._client is None:
            try:
                from redis import asyncio as aioredis  # type: ignore
            except ImportError as e:  # pragma: no cover - se prueba con fake
                raise RuntimeError(
                    "redis no está instalado; instala requirements.txt para "
                    "habilitar el cache real."
                ) from e
            # decode_responses=False → manejamos bytes/JSON nosotros.
            self._client = aioredis.from_url(self._url)  # pragma: no cover
        return self._client

    def _key(self, key: str) -> str:
        return f"{self._namespace}:{key}"

    # ── Contrato Cache (B) ───────────────────────────
    async def get(self, key: str) -> Any | None:
        raw = await self._ensure_client().get(self._key(key))
        return _loads(raw)

    async def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        ttl = ttl if ttl is not None else self._default_ttl
        ex = int(ttl) if ttl else None
        await self._ensure_client().set(self._key(key), _dumps(value), ex=ex)

    # ── Extras cache-aside ───────────────────────────
    async def delete(self, key: str) -> bool:
        removed = await self._ensure_client().delete(self._key(key))
        return bool(removed)

    async def exists(self, key: str) -> bool:
        return bool(await self._ensure_client().exists(self._key(key)))

    async def get_or_set(
        self,
        key: str,
        factory: Callable[[], Awaitable[Any]] | Callable[[], Any],
        ttl: float | None = None,
    ) -> Any:
        """Devuelve el valor cacheado o lo calcula con ``factory`` y lo guarda."""
        cached = await self.get(key)
        if cached is not None:
            return cached
        produced = factory()
        if asyncio.iscoroutine(produced):
            produced = await produced
        if produced is not None:
            await self.set(key, produced, ttl=ttl)
        return produced

    # ── Lock distribuido ─────────────────────────────
    @asynccontextmanager
    async def lock(
        self,
        name: str,
        *,
        timeout: float = 10.0,
        blocking_timeout: float = 5.0,
        poll_interval: float = 0.05,
    ) -> AsyncIterator[str]:
        """
        Lock distribuido por ``SET NX EX``. Espera hasta ``blocking_timeout`` a
        adquirirlo; ``timeout`` es el TTL del lock (auto-libera si el dueño
        muere). Libera de forma atómica solo si el token sigue siendo el nuestro.

            async with cache.lock("case:123"):
                ...  # sección crítica distribuida
        """
        client = self._ensure_client()
        lock_key = self._key(f"lock:{name}")
        token = uuid4().hex
        deadline = time.monotonic() + blocking_timeout

        acquired = await client.set(lock_key, token, nx=True, ex=int(timeout) or 1)
        while not acquired and time.monotonic() < deadline:
            await asyncio.sleep(poll_interval)
            acquired = await client.set(lock_key, token, nx=True, ex=int(timeout) or 1)
        if not acquired:
            raise LockNotAcquiredError(f"no se adquirió el lock '{name}'")

        try:
            yield token
        finally:
            try:
                await client.eval(_RELEASE_LOCK_LUA, 1, lock_key, token)
            except Exception:  # pragma: no cover - liberación best-effort
                pass

    # ── Pub/Sub ──────────────────────────────────────
    async def publish(self, channel: str, message: Any) -> int:
        """Publica un mensaje JSON en ``channel``; devuelve nº de suscriptores."""
        return int(await self._ensure_client().publish(channel, _dumps(message)))

    @asynccontextmanager
    async def subscribe(self, *channels: str) -> AsyncIterator[Any]:
        """Context manager que entrega un ``pubsub`` ya suscrito a ``channels``."""
        pubsub = self._ensure_client().pubsub()  # pragma: no cover - requiere redis
        await pubsub.subscribe(*channels)
        try:
            yield pubsub
        finally:
            await pubsub.unsubscribe(*channels)
            await pubsub.close()

    # ── Salud / ciclo de vida ────────────────────────
    async def ping(self) -> bool:
        try:
            return bool(await self._ensure_client().ping())
        except Exception:  # pragma: no cover - depende de redis real
            return False

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()


# ─────────────────────────────────────────────────────
# InMemoryRedisCache (fake dev / tests)
# ─────────────────────────────────────────────────────
class InMemoryRedisCache:
    """
    Implementa el mismo API que ``RedisCache`` con dicts en memoria. No es
    distribuido (los locks solo sirven dentro del proceso) pero permite correr
    todo el sistema sin un Redis real.
    """

    def __init__(
        self,
        *,
        namespace: str = "analysis",
        default_ttl: float | None = None,
    ) -> None:
        self._namespace = namespace
        self._default_ttl = default_ttl
        self._data: dict[str, tuple[str, float | None]] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._channels: dict[str, list[asyncio.Queue[Any]]] = {}
        self._guard = asyncio.Lock()

    def _key(self, key: str) -> str:
        return f"{self._namespace}:{key}"

    def _live(self, full_key: str) -> Any | None:
        entry = self._data.get(full_key)
        if entry is None:
            return None
        raw, expires_at = entry
        if expires_at is not None and time.monotonic() >= expires_at:
            self._data.pop(full_key, None)
            return None
        return raw

    async def get(self, key: str) -> Any | None:
        return _loads(self._live(self._key(key)))

    async def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        ttl = ttl if ttl is not None else self._default_ttl
        expires_at = time.monotonic() + ttl if ttl else None
        self._data[self._key(key)] = (_dumps(value), expires_at)

    async def delete(self, key: str) -> bool:
        return self._data.pop(self._key(key), None) is not None

    async def exists(self, key: str) -> bool:
        return self._live(self._key(key)) is not None

    async def get_or_set(
        self,
        key: str,
        factory: Callable[[], Awaitable[Any]] | Callable[[], Any],
        ttl: float | None = None,
    ) -> Any:
        cached = await self.get(key)
        if cached is not None:
            return cached
        produced = factory()
        if asyncio.iscoroutine(produced):
            produced = await produced
        if produced is not None:
            await self.set(key, produced, ttl=ttl)
        return produced

    @asynccontextmanager
    async def lock(
        self,
        name: str,
        *,
        timeout: float = 10.0,
        blocking_timeout: float = 5.0,
        poll_interval: float = 0.05,
    ) -> AsyncIterator[str]:
        async with self._guard:
            lock = self._locks.setdefault(name, asyncio.Lock())
        try:
            await asyncio.wait_for(lock.acquire(), timeout=blocking_timeout)
        except asyncio.TimeoutError as e:
            raise LockNotAcquiredError(f"no se adquirió el lock '{name}'") from e
        try:
            yield uuid4().hex
        finally:
            lock.release()

    async def publish(self, channel: str, message: Any) -> int:
        queues = self._channels.get(channel, [])
        for q in queues:
            q.put_nowait(_dumps(message))
        return len(queues)

    @asynccontextmanager
    async def subscribe(self, *channels: str) -> AsyncIterator["_InMemoryPubSub"]:
        queue: asyncio.Queue[Any] = asyncio.Queue()
        for ch in channels:
            self._channels.setdefault(ch, []).append(queue)
        pubsub = _InMemoryPubSub(queue)
        try:
            yield pubsub
        finally:
            for ch in channels:
                subs = self._channels.get(ch, [])
                if queue in subs:
                    subs.remove(queue)

    async def ping(self) -> bool:
        return True

    async def close(self) -> None:
        self._data.clear()


class _InMemoryPubSub:
    """Mini pub/sub que imita el ``get_message`` de redis.asyncio para el fake."""

    def __init__(self, queue: asyncio.Queue[Any]) -> None:
        self._queue = queue

    async def get_message(self, timeout: float = 1.0) -> dict[str, Any] | None:
        try:
            data = await asyncio.wait_for(self._queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None
        return {"type": "message", "data": data}


# ─────────────────────────────────────────────────────
# Singleton
# ─────────────────────────────────────────────────────
@lru_cache
def get_cache() -> RedisCache | InMemoryRedisCache:
    """
    Singleton de cache. Devuelve ``RedisCache`` si la lib ``redis`` está
    instalada; si no, ``InMemoryRedisCache`` (degradación elegante en dev).
    """
    try:
        import redis  # type: ignore  # noqa: F401
    except ImportError:
        return InMemoryRedisCache()
    return RedisCache()


__all__ = [
    "RedisCache",
    "InMemoryRedisCache",
    "get_cache",
    "LockNotAcquiredError",
]
