"""
Endpoints de salud del sistema.

- /health      → liveness (la app está viva)
- /health/ready → readiness (la app + dependencias críticas funcionan)
"""

from datetime import datetime

from fastapi import APIRouter
from pydantic import BaseModel
from sqlalchemy import text

from src.database.connection import get_db
from src.infrastructure.config import get_settings

router = APIRouter(prefix="/health", tags=["health"])


# ─────────────────────────────────────────
# Schemas
# ─────────────────────────────────────────
class HealthResponse(BaseModel):
    status: str
    timestamp: datetime
    environment: str
    version: str = "1.0.0"


class ReadinessResponse(BaseModel):
    status: str
    timestamp: datetime
    checks: dict[str, str]


# ─────────────────────────────────────────
# Liveness probe
# ─────────────────────────────────────────
@router.get("", response_model=HealthResponse)
async def health() -> HealthResponse:
    """
    Liveness check - la app está corriendo.
    Usado por Kubernetes liveness probe.
    """
    settings = get_settings()
    return HealthResponse(
        status="ok",
        timestamp=datetime.utcnow(),
        environment=settings.app_env,
    )


# ─────────────────────────────────────────
# Readiness probe
# ─────────────────────────────────────────
@router.get("/ready", response_model=ReadinessResponse)
async def readiness() -> ReadinessResponse:
    """
    Readiness check - la app + dependencias críticas funcionan.
    Usado por Kubernetes readiness probe.

    Verifica:
    - PostgreSQL Primary (write)
    - PostgreSQL Replicas (read)
    """
    checks: dict[str, str] = {}
    overall_status = "ok"

    # ─── PostgreSQL Primary ─────────────────────────────
    try:
        db = get_db()
        async with db.write_session() as session:
            await session.execute(text("SELECT 1"))
        checks["postgres_primary"] = "ok"
    except Exception as e:
        checks["postgres_primary"] = f"error: {type(e).__name__}"
        overall_status = "degraded"

    # ─── PostgreSQL Replicas ────────────────────────────
    try:
        async with db.read_session() as session:
            await session.execute(text("SELECT 1"))
        checks["postgres_replicas"] = "ok"
    except Exception as e:
        checks["postgres_replicas"] = f"error: {type(e).__name__}"
        overall_status = "degraded"

    return ReadinessResponse(
        status=overall_status,
        timestamp=datetime.utcnow(),
        checks=checks,
    )
