"""
Autenticación con JWT.

ESTRUCTURA:
    - Role: roles soportados (admin / analyst / viewer)
    - TokenPayload / AuthenticatedUser: contratos de datos
    - create_access_token / decode_token: emisión y verificación
    - get_current_user: FastAPI dependency para proteger endpoints
    - get_optional_user: dependency suave (token opcional)
    - require_role: dependency que además exige roles concretos

ALGORITMO:
    HS256 simétrico (firma con secret_key). Más simple para dev.
    Para producción, Persona C debe cambiar a RS256 con par de llaves
    (configurable via Settings.jwt_algorithm).

USO TÍPICO EN UN ENDPOINT:

    @router.get("/secret")
    async def secret(user: AuthenticatedUser = Depends(get_current_user)):
        return {"hello": user.user_id}

    @router.get("/admin-only")
    async def admin(user: AuthenticatedUser = Depends(require_role("admin"))):
        ...
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt

from src.infrastructure.config import get_settings


# ─────────────────────────────────────────────────────
# Modelo de roles
# ─────────────────────────────────────────────────────
class Role(str, Enum):
    """Roles soportados por el sistema (per arquitectura)."""

    ADMIN = "admin"
    ANALYST = "analyst"
    VIEWER = "viewer"


# ─────────────────────────────────────────────────────
# Contratos de datos
# ─────────────────────────────────────────────────────
@dataclass(frozen=True)
class TokenPayload:
    """Datos planos contenidos en el JWT."""

    user_id: str          # 'sub'
    role: Role
    issued_at: datetime   # 'iat'
    expires_at: datetime  # 'exp'
    issuer: str           # 'iss'


@dataclass(frozen=True)
class AuthenticatedUser:
    """
    Representación del usuario autenticado dentro de la request.
    Lo que los endpoints reciben via Depends(get_current_user).
    """

    user_id: str
    role: Role
    token_expires_at: datetime

    @property
    def is_admin(self) -> bool:
        return self.role == Role.ADMIN

    @property
    def is_analyst(self) -> bool:
        return self.role == Role.ANALYST

    @property
    def is_viewer(self) -> bool:
        return self.role == Role.VIEWER


# ─────────────────────────────────────────────────────
# Excepciones de autenticación
# ─────────────────────────────────────────────────────
class AuthError(Exception):
    """Base para todos los errores de autenticación."""


class InvalidTokenError(AuthError):
    """Token mal formado, firma inválida o claims faltantes."""


class ExpiredTokenError(AuthError):
    """Token correcto pero expirado."""


# ─────────────────────────────────────────────────────
# Emisión / verificación
# ─────────────────────────────────────────────────────
def create_access_token(
    user_id: str,
    role: Role | str = Role.VIEWER,
    expires_in_hours: int | None = None,
) -> str:
    """
    Genera un JWT firmado para el usuario indicado.

    Args:
        user_id: identificador del usuario (queda en 'sub').
        role: rol asignado al token.
        expires_in_hours: override del TTL por defecto del Settings.
    """
    settings = get_settings()
    role_value = role.value if isinstance(role, Role) else role
    ttl_hours = expires_in_hours or settings.jwt_expiration_hours

    now = datetime.now(timezone.utc)
    expire = now + timedelta(hours=ttl_hours)

    claims: dict[str, Any] = {
        "sub": user_id,
        "role": role_value,
        "iat": int(now.timestamp()),
        "exp": int(expire.timestamp()),
        "iss": settings.jwt_issuer,
    }

    return jwt.encode(
        claims,
        settings.secret_key,
        algorithm=settings.jwt_algorithm,
    )


def decode_token(token: str) -> TokenPayload:
    """
    Verifica firma + expiración y devuelve el payload tipado.

    Lanza:
        ExpiredTokenError si el token está vencido.
        InvalidTokenError si firma incorrecta o claims faltantes.
    """
    settings = get_settings()
    try:
        claims = jwt.decode(
            token,
            settings.secret_key,
            algorithms=[settings.jwt_algorithm],
            issuer=settings.jwt_issuer,
            options={"require_iat": True, "require_exp": True},
        )
    except jwt.ExpiredSignatureError as e:
        raise ExpiredTokenError("token expirado") from e
    except JWTError as e:
        raise InvalidTokenError(f"token invalido: {e}") from e

    try:
        user_id = str(claims["sub"])
        role = Role(claims["role"])
        issued_at = datetime.fromtimestamp(claims["iat"], tz=timezone.utc)
        expires_at = datetime.fromtimestamp(claims["exp"], tz=timezone.utc)
        issuer = str(claims["iss"])
    except (KeyError, ValueError) as e:
        raise InvalidTokenError(f"claims faltantes o invalidos: {e}") from e

    return TokenPayload(
        user_id=user_id,
        role=role,
        issued_at=issued_at,
        expires_at=expires_at,
        issuer=issuer,
    )


# ─────────────────────────────────────────────────────
# Dependencies de FastAPI
# ─────────────────────────────────────────────────────
# Scheme Bearer (FastAPI lo refleja en Swagger UI automáticamente)
_bearer = HTTPBearer(auto_error=False)


def _raise_unauthorized(detail: str) -> None:
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=detail,
        headers={"WWW-Authenticate": "Bearer"},
    )


async def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer),
) -> AuthenticatedUser:
    """
    Obligatorio: lanza 401 si no hay token o si es inválido.
    """
    if credentials is None:
        _raise_unauthorized("Authorization header requerido")
        # _raise_unauthorized siempre lanza, pero mypy no lo sabe:
        raise AssertionError  # pragma: no cover

    try:
        payload = decode_token(credentials.credentials)
    except ExpiredTokenError:
        _raise_unauthorized("Token expirado")
        raise AssertionError  # pragma: no cover
    except InvalidTokenError as e:
        _raise_unauthorized(str(e))
        raise AssertionError  # pragma: no cover

    return AuthenticatedUser(
        user_id=payload.user_id,
        role=payload.role,
        token_expires_at=payload.expires_at,
    )


async def get_optional_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer),
) -> AuthenticatedUser | None:
    """
    Opcional: devuelve el usuario si hay token válido, None si no hay token.
    Lanza 401 únicamente si el token está presente pero corrupto/expirado.
    """
    if credentials is None:
        return None
    return await get_current_user(credentials=credentials)


def require_role(*roles: Role | str):
    """
    Factory de dependency que además del token exige un rol específico.

    Uso:
        @router.delete(
            "/case/{id}",
            dependencies=[Depends(require_role(Role.ADMIN, Role.ANALYST))],
        )
    """
    allowed = {r.value if isinstance(r, Role) else r for r in roles}

    async def dependency(
        user: AuthenticatedUser = Depends(get_current_user),
    ) -> AuthenticatedUser:
        if user.role.value not in allowed:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    f"Rol '{user.role.value}' insuficiente. "
                    f"Requerido: {sorted(allowed)}"
                ),
            )
        return user

    return dependency
