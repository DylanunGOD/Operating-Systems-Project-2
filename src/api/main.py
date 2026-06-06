"""
Punto de entrada de la API FastAPI.

Responsabilidades:
- Crear la app FastAPI con metadata
- Configurar middleware (CORS, logging)
- Montar rutas (health, upload, GraphQL)
- Lifespan: inicializar y cerrar recursos
"""

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from strawberry.fastapi import GraphQLRouter

from src.api.graphql_schema import schema
from src.api.resolvers import register_handlers
from src.api.routes import auth, health, upload
from src.database.connection import get_db
from src.infrastructure.config import get_settings


# ─────────────────────────────────────────
# Lifespan: setup y teardown
# ─────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """
    Hook que se ejecuta al iniciar y apagar la app.

    Startup: instanciar singletons (DB pool, Redis, Kafka)
    Shutdown: cerrar conexiones limpiamente
    """
    settings = get_settings()
    print(f"[startup] Iniciando API en modo {settings.app_env}")

    # Inicializar singleton de DB (crea el pool de conexiones)
    get_db()

    # Registrar handlers de CQRS (CommandBus + QueryBus)
    register_handlers()
    print("[startup] Handlers CQRS registrados")

    yield  # ───── la app corre acá ─────

    # Shutdown: cerrar pool de DB
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
