"""
Conexión a PostgreSQL con soporte para 1 Primary + 2 Replicas.

Implementa:
- Object Pool pattern (via SQLAlchemy connection pool)
- CQRS: routing automático escritura/lectura
- Singleton: una sola instancia del manager
"""

from contextlib import asynccontextmanager
from functools import lru_cache
from typing import AsyncIterator
import random

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from src.infrastructure.config import get_settings


class Base(DeclarativeBase):
    """Clase base para todos los modelos SQLAlchemy ORM."""

    pass


class DatabaseConnection:
    """
    Manager de conexiones a PostgreSQL con Primary + Replicas.

    - Writes -> Primary (ACID)
    - Reads -> Replicas (round-robin / random)

    Cada engine maneja internamente un pool de conexiones (Object Pool pattern):
    pool_size conexiones permanentes + max_overflow extras bajo carga.
    """

    def __init__(self) -> None:
        settings = get_settings()

        # ─── Engine Primary (escritura) ─────────────────────
        self._primary_engine: AsyncEngine = create_async_engine(
            settings.database_url,
            pool_size=settings.database_pool_size,
            max_overflow=settings.database_max_overflow,
            pool_pre_ping=True,  # valida la conexión antes de usarla
            pool_recycle=3600,   # recicla conexiones cada hora
            echo=settings.is_development,
        )

        # ─── Engines Replicas (lectura) ─────────────────────
        self._replica_engines: list[AsyncEngine] = [
            create_async_engine(
                settings.database_url_replica1,
                pool_size=settings.database_pool_size,
                max_overflow=settings.database_max_overflow,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False,
            ),
            create_async_engine(
                settings.database_url_replica2,
                pool_size=settings.database_pool_size,
                max_overflow=settings.database_max_overflow,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False,
            ),
        ]

        # ─── Session factories ──────────────────────────────
        self._primary_session = async_sessionmaker(
            bind=self._primary_engine,
            expire_on_commit=False,
            class_=AsyncSession,
        )

        self._replica_sessions = [
            async_sessionmaker(
                bind=engine, expire_on_commit=False, class_=AsyncSession
            )
            for engine in self._replica_engines
        ]

    # ─────────────────────────────────────────
    # Sessions: Write (Primary)
    # ─────────────────────────────────────────
    @asynccontextmanager
    async def write_session(self) -> AsyncIterator[AsyncSession]:
        """
        Session conectada al Primary para escrituras (Commands en CQRS).

        Uso:
            async with db.write_session() as session:
                session.add(case)
                await session.commit()
        """
        async with self._primary_session() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()

    # ─────────────────────────────────────────
    # Sessions: Read (Replicas)
    # ─────────────────────────────────────────
    @asynccontextmanager
    async def read_session(self) -> AsyncIterator[AsyncSession]:
        """
        Session conectada a una replica para lecturas (Queries en CQRS).
        Selecciona una replica aleatoriamente (load balancing simple).

        Uso:
            async with db.read_session() as session:
                result = await session.execute(query)
        """
        session_factory = random.choice(self._replica_sessions)
        async with session_factory() as session:
            try:
                yield session
            finally:
                await session.close()

    # ─────────────────────────────────────────
    # Lifecycle
    # ─────────────────────────────────────────
    async def close(self) -> None:
        """Cierra todos los engines y libera conexiones del pool."""
        await self._primary_engine.dispose()
        for engine in self._replica_engines:
            await engine.dispose()

    @property
    def primary_engine(self) -> AsyncEngine:
        """Acceso directo al engine primary (para migraciones, tests)."""
        return self._primary_engine


@lru_cache
def get_db() -> DatabaseConnection:
    """
    Singleton de DatabaseConnection.
    Garantiza un único pool de conexiones por proceso.
    """
    return DatabaseConnection()
