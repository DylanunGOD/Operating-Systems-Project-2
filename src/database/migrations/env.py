"""
Configuración de Alembic.

Hace dos cosas clave:
1. Carga la URL real de la BD desde Settings (no la del .ini)
2. Importa todos los modelos para que Alembic vea el metadata
   y pueda auto-generar migraciones con `alembic revision --autogenerate`
"""

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

# Importar Base + todos los modelos para que SQLAlchemy los registre
# en Base.metadata y Alembic pueda auto-generar migraciones.
from src.database.connection import Base
from src.infrastructure.config import get_settings
from src.models import case, result  # noqa: F401


# ─── Config de Alembic ─────────────────────────────────
config = context.config

# Sobrescribimos la URL del .ini con la URL real de Settings
settings = get_settings()
config.set_main_option(
    "sqlalchemy.url",
    settings.database_url.replace("postgresql+asyncpg", "postgresql+asyncpg"),
)

# Setup de logging del .ini
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Metadata target para autogenerate
target_metadata = Base.metadata


# ─────────────────────────────────────────────────────
# Modo offline (genera SQL sin conectar)
# ─────────────────────────────────────────────────────
def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


# ─────────────────────────────────────────────────────
# Modo online (conecta y ejecuta)
# ─────────────────────────────────────────────────────
def do_run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,           # detecta cambios de tipo
        compare_server_default=True, # detecta cambios de default
    )

    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


def run_migrations_online() -> None:
    asyncio.run(run_async_migrations())


# ─── Punto de entrada ──────────────────────────────────
if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
