"""
Punto de entrada de la API FastAPI.

Responsabilidades:
- Crear la app FastAPI con metadata
- Configurar middleware (CORS, logging)
- Montar rutas (health, upload, GraphQL)
- Lifespan: inicializar y cerrar recursos
"""

import asyncio
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from strawberry.fastapi import GraphQLRouter

from src.api.graphql_schema import schema
from src.api.resolvers import register_handlers
from src.api.routes import auth, health, upload
from src.database.connection import get_db
from src.infrastructure.config import get_settings
from src.patterns.outbox import ConsoleKafkaPublisher, OutboxPublisher


def _build_kafka_publisher() -> tuple[Any, bool]:
    """
    Elige el publicador de la outbox:
    - ``KAFKA_ENABLED=true`` → ``KafkaManager`` real (Persona C, FASE 3).
    - en otro caso → ``ConsoleKafkaPublisher`` (fake de A, modo dev).

    Devuelve ``(publisher, is_real)``. Degradación elegante: si Kafka está
    habilitado pero la construcción falla, cae al fake en vez de tumbar la API.
    """
    settings = get_settings()
    if not settings.kafka_enabled:
        return ConsoleKafkaPublisher(), False
    try:
        from src.infrastructure.kafka import get_kafka_manager

        return get_kafka_manager(), True
    except Exception as e:  # pragma: no cover - depende de la infra real
        print(f"[startup] Kafka real no disponible ({e}); uso ConsoleKafkaPublisher")
        return ConsoleKafkaPublisher(), False


# ─────────────────────────────────────────
# Lifespan: setup y teardown
# ─────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """
    Hook que se ejecuta al iniciar y apagar la app.

    Startup: instanciar singletons (DB pool), registrar handlers CQRS y arrancar
    el OutboxPublisher (drena la tabla outbox hacia Kafka en background).
    Shutdown: parar el poller y cerrar conexiones limpiamente.
    """
    settings = get_settings()
    print(f"[startup] Iniciando API en modo {settings.app_env}")

    # Inicializar singleton de DB (crea el pool de conexiones)
    get_db()

    # Registrar handlers de CQRS (CommandBus + QueryBus)
    register_handlers()
    print("[startup] Handlers CQRS registrados")

    # Arrancar el OutboxPublisher (Outbox Pattern → Kafka), en background.
    publisher, is_real = _build_kafka_publisher()
    outbox_publisher = OutboxPublisher(kafka_publisher=publisher)
    outbox_task = asyncio.create_task(outbox_publisher.run_forever())
    app.state.kafka_publisher = publisher
    app.state.outbox_publisher = outbox_publisher
    app.state.outbox_task = outbox_task
    print(
        f"[startup] OutboxPublisher iniciado "
        f"({'Kafka real' if is_real else 'Console fake'})"
    )

    yield  # ───── la app corre acá ─────

    # Shutdown: parar el poller y esperar su última iteración
    print("[shutdown] Deteniendo OutboxPublisher...")
    outbox_publisher.stop()
    try:
        await asyncio.wait_for(outbox_task, timeout=5.0)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        outbox_task.cancel()

    # Cerrar el productor de Kafka si era el real
    close = getattr(publisher, "close", None)
    if callable(close):
        try:
            await close()
        except Exception as e:  # pragma: no cover
            print(f"[shutdown] error cerrando Kafka: {e}")

    # Cerrar pool de DB
    print("[shutdown] Cerrando conexiones...")
    db = get_db()
    await db.close()


# ─────────────────────────────────────────
# App factory
# ─────────────────────────────────────────
def create_app() -> FastAPI:
    """
    Factory de la app FastAPI.
    Separar la creación del module-level facilita testing.
    """
    settings = get_settings()

    app = FastAPI(
        title="Analysis Distributed API",
        description="Sistema de análisis multiproceso y distribuido de datos de mensajería",
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        lifespan=lifespan,
    )

    # ─── Middleware: CORS ───────────────────────────────
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"] if settings.is_development else [],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ─── Rutas ──────────────────────────────────────────
    app.include_router(health.router)
    app.include_router(auth.router)
    app.include_router(upload.router)

    # GraphQL endpoint con GraphiQL UI integrado
    graphql_app = GraphQLRouter(schema, graphiql=settings.is_development)
    app.include_router(graphql_app, prefix="/graphql", tags=["graphql"])

    # ─── Métricas Prometheus en /metrics ────────────────
    # Degradación elegante: si prometheus_client no está, la API arranca igual.
    try:
        from src.monitoring.metrics import metrics_asgi_app

        app.mount("/metrics", metrics_asgi_app())
    except Exception as e:  # pragma: no cover - depende de prometheus_client
        print(f"[startup] /metrics no disponible: {e}")

    # ─── Root ───────────────────────────────────────────
    @app.get("/", tags=["root"])
    async def root() -> dict[str, str]:
        return {
            "service": "Analysis Distributed API",
            "version": "1.0.0",
            "docs": "/docs",
            "health": "/health",
        }

    return app


# Instancia para uvicorn: `uvicorn src.api.main:app`
app = create_app()
