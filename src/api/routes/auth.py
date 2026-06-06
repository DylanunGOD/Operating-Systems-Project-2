"""
Endpoints REST de autenticación.

EN ESTA FASE:
    - POST /auth/dev-token  → emite un token para cualquier user/role.
                              **SOLO disponible en development**.
    - GET  /auth/me         → verifica el token y devuelve los claims.

EN PRODUCCIÓN (FASE 3):
    Persona C reemplazará dev-token por un /auth/login real con verificación
    de credenciales contra la BD + refresh tokens.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from src.api.middleware.auth import (
    AuthenticatedUser,
    Role,
    create_access_token,
    get_current_user,
)
from src.infrastructure.config import get_settings


router = APIRouter(prefix="/auth", tags=["auth"])


# ─────────────────────────────────────────────────────
# Schemas
# ─────────────────────────────────────────────────────
class DevTokenRequest(BaseModel):
    user_id: str = Field(..., min_length=1, max_length=64)
    role: Role = Role.VIEWER
    expires_in_hours: int | None = Field(
        default=None, ge=1, le=24 * 30
    )


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "Bearer"
    user_id: str
    role: Role
    expires_at: datetime


class UserInfoResponse(BaseModel):
    user_id: str
    role: Role
    token_expires_at: datetime


# ─────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────
@router.post(
    "/dev-token",
    response_model=TokenResponse,
    summary="(DEV) Emite un JWT sin verificar credenciales",
    description=(
        "Atajo para pruebas locales: genera un token para cualquier "
        "user_id y rol. Bloqueado fuera de development."
    ),
)
async def dev_token(payload: DevTokenRequest) -> TokenResponse:
    settings = get_settings()
    if not settings.is_development:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="dev-token solo disponible en modo development",
        )

    token = create_access_token(
        user_id=payload.user_id,
        role=payload.role,
        expires_in_hours=payload.expires_in_hours,
    )

    # Decodificamos para extraer expires_at exacto
    from src.api.middleware.auth import decode_token
    decoded = decode_token(token)

    return TokenResponse(
        access_token=token,
        user_id=decoded.user_id,
        role=decoded.role,
        expires_at=decoded.expires_at,
    )


@router.get(
    "/me",
    response_model=UserInfoResponse,
    summary="Devuelve los datos del usuario asociado al token",
)
async def me(
    user: Annotated[AuthenticatedUser, Depends(get_current_user)],
) -> UserInfoResponse:
    return UserInfoResponse(
        user_id=user.user_id,
        role=user.role,
        token_expires_at=user.token_expires_at,
    )
