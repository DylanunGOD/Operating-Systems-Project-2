"""
ImageWorker — análisis de imágenes.

QUÉ HACE:
    Por cada sub-tarea de imagen: intenta detectar objetos y marca como
    incidentes los objetos peligrosos (armas, etc.). Devuelve además
    metadata de la imagen (dimensiones, formato, tamaño).

MOTOR DE ANÁLISIS (degradación elegante):
    - Si `ultralytics` (YOLO) + `opencv` están instalados, se usa un
      `YoloImageAnalyzer` para detección de objetos.
    - Si no, un `HeuristicImageAnalyzer`: lee dimensiones con Pillow (si está)
      y deduce indicios por el nombre del archivo / metadata del payload.
    - El analizador se reutiliza vía Object Pool (cargar YOLO es caro).
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from src.patterns.object_pool import ObjectPool
from src.workers.base_worker import BaseWorker
from src.workers.contracts import Finding, WorkerAnalysis, WorkerTask
from src.workers.text_worker import scan_findings

# Clases de objetos que, si se detectan, generan un incidente.
DANGEROUS_OBJECTS: dict[str, tuple[str, str]] = {
    # objeto -> (category, severity)
    "knife": ("weapons", "critical"),
    "gun": ("weapons", "critical"),
    "pistol": ("weapons", "critical"),
    "rifle": ("weapons", "critical"),
    "scissors": ("weapons", "medium"),
    "blood": ("violence", "high"),
}


# ─────────────────────────────────────────────────────
# Lectura de metadata de imagen (sin libs pesadas)
# ─────────────────────────────────────────────────────
def _read_image_meta(source_file: str) -> dict[str, Any]:
    """
    Dimensiones/formato si Pillow está disponible; tamaño en disco siempre
    que el archivo exista. Nunca lanza: degrada a un dict parcial.
    """
    meta: dict[str, Any] = {"width": None, "height": None, "format": None,
                            "size_bytes": None}
    path = Path(source_file) if source_file else None
    if path is None or not path.is_file():
        return meta
    try:
        meta["size_bytes"] = path.stat().st_size
    except OSError:
        pass
    try:
        from PIL import Image  # type: ignore

        with Image.open(path) as img:
            meta["width"], meta["height"] = img.size
            meta["format"] = img.format
    except Exception:
        pass
    return meta


# ─────────────────────────────────────────────────────
# Analizadores (pooleables)
# ─────────────────────────────────────────────────────
class HeuristicImageAnalyzer:
    """
    Sin modelo de visión: deduce indicios del nombre del archivo y la
    metadata del payload (ej: una imagen llamada 'gun.jpg' o etiquetada).
    """

    engine = "heuristic"

    def run(self, task: WorkerTask) -> WorkerAnalysis:
        meta = _read_image_meta(task.source_file)
        # Pistas textuales: nombre del archivo + etiquetas del payload.
        hint_text = " ".join(
            str(task.payload.get(k, ""))
            for k in ("original_name", "caption", "tags", "labels")
        )
        hint_text = f"{Path(task.source_file).name} {hint_text}"
        findings = scan_findings(hint_text, task.source_file)

        data: dict[str, Any] = {
            "engine": self.engine,
            "objects": [],
            **meta,
            "hint_source": "filename+metadata",
        }
        confidence = max((f.confidence for f in findings), default=0.0)
        summary = (
            f"Imagen {meta.get('width')}x{meta.get('height')}, "
            f"{len(findings)} hallazgo(s) por heurística."
        )
        return WorkerAnalysis(
            data=data,
            findings=findings,
            confidence_score=round(confidence, 3) if confidence else None,
            summary=summary,
        )


class YoloImageAnalyzer:
    """Detección de objetos con YOLO (si ultralytics está instalado)."""

    engine = "yolo"

    def __init__(self, model: Any) -> None:
        self._model = model

    def run(self, task: WorkerTask) -> WorkerAnalysis:
        meta = _read_image_meta(task.source_file)
        objects = self._detect(task.source_file)
        findings = self._to_findings(objects, task.source_file)
        data = {
            "engine": self.engine,
            "objects": objects,
            **meta,
        }
        confidence = max((f.confidence for f in findings), default=0.0)
        summary = (
            f"{len(objects)} objeto(s) detectado(s), "
            f"{len(findings)} hallazgo(s)."
        )
        return WorkerAnalysis(
            data=data,
            findings=findings,
            confidence_score=round(confidence, 3) if confidence else None,
            summary=summary,
        )

    def _detect(self, source_file: str) -> list[dict[str, Any]]:
        try:
            results = self._model(source_file)
            objects: list[dict[str, Any]] = []
            for r in results:
                names = getattr(r, "names", {})
                for box in getattr(r, "boxes", []):
                    cls_id = int(box.cls[0])
                    objects.append(
                        {
                            "label": names.get(cls_id, str(cls_id)),
                            "confidence": float(box.conf[0]),
                        }
                    )
            return objects
        except Exception:
            return []

    def _to_findings(
        self, objects: list[dict[str, Any]], source_file: str
    ) -> list[Finding]:
        findings: list[Finding] = []
        for obj in objects:
            label = str(obj.get("label", "")).lower()
            if label in DANGEROUS_OBJECTS:
                category, severity = DANGEROUS_OBJECTS[label]
                findings.append(
                    Finding(
                        category=category,
                        severity=severity,
                        title=f"Objeto peligroso detectado: {label}",
                        description=f"YOLO detectó '{label}' en la imagen.",
                        confidence=round(float(obj.get("confidence", 0.0)), 3),
                        source_file=source_file,
                        location_data={"label": label},
                    )
                )
        return findings


def _try_load_yolo() -> Any | None:
    """Carga un modelo YOLO si ultralytics está disponible; si no, None."""
    try:
        from ultralytics import YOLO  # type: ignore

        return YOLO("yolov8n.pt")
    except Exception:
        return None


def build_image_analyzer() -> Any:
    """Factory del analizador: YOLO si se puede, heurístico si no."""
    model = _try_load_yolo()
    return YoloImageAnalyzer(model) if model is not None else HeuristicImageAnalyzer()


# ─────────────────────────────────────────────────────
# ImageWorker
# ─────────────────────────────────────────────────────
class ImageWorker(BaseWorker):
    """Worker especializado en imágenes (detección de objetos)."""

    content_type = "image"

    def __init__(
        self, *, analyzer_pool: ObjectPool[Any] | None = None, **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self._pool = analyzer_pool or ObjectPool(
            factory=build_image_analyzer, max_size=2, min_size=1
        )

    async def on_start(self) -> None:
        await self._pool.warmup()

    async def on_stop(self) -> None:
        await self._pool.close()

    async def analyze(self, task: WorkerTask) -> WorkerAnalysis:
        async with self._pool.lease() as analyzer:
            return await asyncio.to_thread(analyzer.run, task)
