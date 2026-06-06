"""
Endpoint REST de upload multipart.

¿POR QUÉ REST Y NO GRAPHQL?
    GraphQL no es la opción ideal para subir binarios: el spec multipart
    es complejo y no está estandarizado. REST con multipart/form-data es
    la convención universal para uploads.

FLUJO:
    1. Cliente manda POST /upload/case con archivos + metadatos
    2. Clasificamos cada archivo por MIME type (text/image/audio)
    3. Guardamos los archivos en disco (./uploads/<session>/<name>)
       * En FASE 3, Persona C reemplazará esto por GCS/S3.
    4. Despachamos un CreateCaseCommand al CommandBus
    5. Devolvemos `case_id` + estadística de archivos recibidos

ARCHIVOS RECHAZADOS:
    Cualquier mime type que no caiga en text/* | image/* | audio/* se
    rechaza pero NO tumba el upload completo: queda registrado en
    `rejected_files` dentro de la metadata del caso.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from src.api.resolvers import CreateCaseCommand
from src.patterns.cqrs import command_bus


router = APIRouter(prefix="/upload", tags=["upload"])


# ─────────────────────────────────────────────────────
# Storage
# ─────────────────────────────────────────────────────
# Persona C reemplazará esto por GCS/S3 en FASE 3.
UPLOAD_BASE = Path("uploads")
UPLOAD_BASE.mkdir(exist_ok=True)


# ─────────────────────────────────────────────────────
# Clasificación por MIME
# ─────────────────────────────────────────────────────
EXPLICIT_TEXT_MIMES = {
    "text/plain",
    "text/csv",
    "text/markdown",
    "application/json",
    "application/xml",
}


def classify_mime(mime_type: str | None) -> str | None:
    """
    Devuelve "text" | "image" | "audio" o None si no se soporta.
    """
    if not mime_type:
        return None
    mime_type = mime_type.lower()

    if mime_type in EXPLICIT_TEXT_MIMES or mime_type.startswith("text/"):
        return "text"
    if mime_type.startswith("image/"):
        return "image"
    if mime_type.startswith("audio/"):
        return "audio"
    return None


# ─────────────────────────────────────────────────────
# Helpers de I/O
# ─────────────────────────────────────────────────────
def _save_upload(
    upload: UploadFile, dest_dir: Path
) -> tuple[Path, int]:
    """
    Guarda el upload en dest_dir, resolviendo colisiones con prefijo numérico.
    Devuelve (path_final, size_bytes).
    """
    safe_name = Path(upload.filename or "unnamed").name or "unnamed"
    dest = dest_dir / safe_name

    counter = 0
    while dest.exists():
        counter += 1
        dest = dest_dir / f"{counter}_{safe_name}"

    with dest.open("wb") as f:
        shutil.copyfileobj(upload.file, f)

    return dest, dest.stat().st_size


def _cleanup_session(session_dir: Path) -> None:
    """Borra el directorio de la sesión si algo falla."""
    shutil.rmtree(session_dir, ignore_errors=True)


# ─────────────────────────────────────────────────────
# Endpoint
# ─────────────────────────────────────────────────────
@router.post(
    "/case",
    summary="Crea un caso de análisis a partir de archivos subidos",
    description=(
        "Recibe N archivos en `files` y los clasifica por MIME type "
        "(text/*, image/*, audio/*). Crea el caso, lo encola y devuelve "
        "el `case_id`."
    ),
)
async def upload_case(
    user_id: str = Form(...),
    title: str = Form(...),
    description: str | None = Form(None),
    files: list[UploadFile] = File(...),
) -> dict[str, Any]:
    if not files:
        raise HTTPException(
            status_code=400, detail="Se requiere al menos un archivo"
        )
    if len(files) > 500:
        # Salvaguarda contra DoS por upload masivo
        raise HTTPException(
            status_code=400,
            detail=f"Demasiados archivos ({len(files)}). Máximo: 500",
        )

    # ─── Sesión de upload (carpeta temporal) ─────
    session_id = str(uuid4())
    session_dir = UPLOAD_BASE / session_id
    session_dir.mkdir(parents=True)

    classified: dict[str, list[dict[str, Any]]] = {
        "text": [],
        "image": [],
        "audio": [],
    }
    rejected: list[dict[str, Any]] = []

    try:
        # ─── Guardar y clasificar cada archivo ─────
        for upload in files:
            kind = classify_mime(upload.content_type)
            if kind is None:
                rejected.append(
                    {
                        "filename": upload.filename,
                        "mime_type": upload.content_type,
                    }
                )
                continue

            dest, size = _save_upload(upload, session_dir)
            classified[kind].append(
                {
                    "source_file": str(dest),
                    "original_name": upload.filename,
                    "size_bytes": size,
                    "mime_type": upload.content_type,
                }
            )

        accepted_total = sum(len(v) for v in classified.values())
        if accepted_total == 0:
            _cleanup_session(session_dir)
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Ningún archivo tiene tipo soportado "
                    f"(text/image/audio). Rechazados: {len(rejected)}"
                ),
            )

        # ─── Crear caso vía CommandBus (CQRS) ─────
        result = await command_bus.dispatch(
            CreateCaseCommand(
                user_id=user_id,
                title=title,
                description=description,
                text_items_count=len(classified["text"]),
                image_items_count=len(classified["image"]),
                audio_items_count=len(classified["audio"]),
                metadata={
                    "upload_session_id": session_id,
                    "upload_dir": str(session_dir),
                    "files": classified,
                    "rejected_files": rejected,
                },
            )
        )

        return {
            "case_id": result.case_id,
            "status": result.status.value,
            "message": result.message,
            "upload_session_id": session_id,
            "files_received": {
                "text": len(classified["text"]),
                "image": len(classified["image"]),
                "audio": len(classified["audio"]),
                "rejected": len(rejected),
                "total_accepted": accepted_total,
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        _cleanup_session(session_dir)
        raise HTTPException(
            status_code=500,
            detail=f"Error procesando upload: {type(e).__name__}: {e}",
        ) from e
