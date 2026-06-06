"""
Patrón Splitter — división de un caso en sub-tareas independientes.

PROBLEMA:
    Un caso de análisis puede tener decenas de archivos (textos, imágenes,
    audios). Si se procesara como una sola unidad, un único worker
    quedaría ocupado mucho tiempo y no se aprovecharía el paralelismo
    horizontal del sistema.

SOLUCIÓN:
    El Splitter recibe un caso + sus items y produce N `SubTask`
    independientes, una por archivo. Cada sub-tarea es autocontenida
    (lleva su case_id, índice, datos del archivo) y va al topic Kafka
    correcto, decidido por el `ContentBasedRouter`.

USO TÍPICO (dentro del handler de CreateCaseCommand):

    splitter = MessageSplitter(router=build_default_router())
    subtasks = splitter.split(
        case_id=case.id,
        text_items=[{"source_file": "msg1.txt", ...}, ...],
        image_items=[{"source_file": "img1.jpg", ...}, ...],
        audio_items=[{"source_file": "audio1.mp3", ...}, ...],
    )
    for st in subtasks:
        add_outbox_event(
            session,
            aggregate_id=case.id,
            aggregate_type="case",
            event_type=f"analysis.{st.type}.requested",
            topic=st.topic,
            payload=st.payload,
        )
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

from src.patterns.message_router import (
    ContentBasedRouter,
    NoRouteMatchedError,
    build_default_router,
)


# ─────────────────────────────────────────────────────
# Tipos de contenido soportados
# ─────────────────────────────────────────────────────
ALLOWED_TYPES = {"text", "image", "audio"}


# ─────────────────────────────────────────────────────
# SubTask
# ─────────────────────────────────────────────────────
@dataclass(frozen=True)
class SubTask:
    """
    Una unidad de trabajo independiente que va a un worker.

    Es auto-contenida: cualquier worker puede procesarla sin necesitar
    cargar el caso completo.
    """

    task_id: str
    case_id: str
    type: str        # "text" | "image" | "audio"
    index: int       # posición dentro del caso (para reporting)
    source_file: str
    topic: str       # topic Kafka destino
    payload: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.type not in ALLOWED_TYPES:
            raise ValueError(
                f"type invalido '{self.type}'. Permitidos: {sorted(ALLOWED_TYPES)}"
            )


# ─────────────────────────────────────────────────────
# Excepciones
# ─────────────────────────────────────────────────────
class EmptyCaseError(Exception):
    """Se intentó splittear un caso sin items."""

    def __init__(self, case_id: str) -> None:
        super().__init__(
            f"El caso '{case_id}' no tiene items para dividir. "
            "Debe tener al menos 1 archivo."
        )


class SplitterRoutingError(Exception):
    """El router no encontró topic para un item del caso."""

    def __init__(self, item: dict[str, Any]) -> None:
        super().__init__(
            f"No se pudo enrutar el item del caso: {item}"
        )


# ─────────────────────────────────────────────────────
# MessageSplitter
# ─────────────────────────────────────────────────────
class MessageSplitter:
    """
    Divide un caso en sub-tareas independientes, una por archivo.

    Para cada item:
    - Genera un task_id único (UUID).
    - Asigna un índice dentro de su tipo (text:0, text:1, image:0, ...).
    - Decide topic Kafka destino mediante el router.
    - Construye el payload final.

    La salida es una lista de SubTask listas para emitir a Kafka
    (via Outbox event).
    """

    def __init__(self, router: ContentBasedRouter | None = None) -> None:
        self._router = router or build_default_router()

    # ─────────────────────────────────────────
    # API principal
    # ─────────────────────────────────────────
    def split(
        self,
        *,
        case_id: str,
        text_items: list[dict[str, Any]] | None = None,
        image_items: list[dict[str, Any]] | None = None,
        audio_items: list[dict[str, Any]] | None = None,
    ) -> list[SubTask]:
        """
        Divide los items del caso en sub-tareas listas para emitir.
        Mantiene el orden: primero todos los text, luego image, luego audio.
        """
        if not case_id:
            raise ValueError("case_id es obligatorio")

        text_items = text_items or []
        image_items = image_items or []
        audio_items = audio_items or []

        total = len(text_items) + len(image_items) + len(audio_items)
        if total == 0:
            raise EmptyCaseError(case_id)

        subtasks: list[SubTask] = []
        subtasks.extend(self._split_group("text", case_id, text_items))
        subtasks.extend(self._split_group("image", case_id, image_items))
        subtasks.extend(self._split_group("audio", case_id, audio_items))
        return subtasks

    def split_items(
        self, case_id: str, items: list[dict[str, Any]]
    ) -> list[SubTask]:
        """
        Variante: recibe una lista heterogénea (cada item trae su `type`)
        y los agrupa internamente antes de splittearlos.

        Útil cuando los archivos vienen mezclados (ej: upload masivo).
        """
        text_items: list[dict[str, Any]] = []
        image_items: list[dict[str, Any]] = []
        audio_items: list[dict[str, Any]] = []

        for item in items:
            t = item.get("type")
            if t == "text":
                text_items.append(item)
            elif t == "image":
                image_items.append(item)
            elif t == "audio":
                audio_items.append(item)
            else:
                raise SplitterRoutingError(item)

        return self.split(
            case_id=case_id,
            text_items=text_items,
            image_items=image_items,
            audio_items=audio_items,
        )

    # ─────────────────────────────────────────
    # Internos
    # ─────────────────────────────────────────
    def _split_group(
        self,
        type_: str,
        case_id: str,
        items: list[dict[str, Any]],
    ) -> list[SubTask]:
        """Crea las SubTask para un grupo del mismo tipo."""
        result: list[SubTask] = []

        for index, item in enumerate(items):
            source_file = item.get("source_file") or item.get("path") or ""
            if not source_file:
                raise SplitterRoutingError({**item, "_reason": "sin source_file"})

            # Resolver topic via router. Aseguramos type para que matchee.
            normalized = {**item, "type": type_}
            try:
                topic = self._router.route_one(normalized)
            except NoRouteMatchedError:
                raise SplitterRoutingError(normalized) from None

            payload = self._build_payload(
                case_id=case_id,
                type_=type_,
                index=index,
                source_file=source_file,
                extra=item,
            )

            result.append(
                SubTask(
                    task_id=str(uuid4()),
                    case_id=case_id,
                    type=type_,
                    index=index,
                    source_file=source_file,
                    topic=topic,
                    payload=payload,
                )
            )

        return result

    def _build_payload(
        self,
        *,
        case_id: str,
        type_: str,
        index: int,
        source_file: str,
        extra: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Construye el cuerpo del mensaje que recibirá el worker.
        Incluye metadata del caso + datos del item.
        """
        # No incluir el campo `type` 2 veces; lo seteamos al final
        clean_extra = {k: v for k, v in extra.items() if k != "type"}
        return {
            "case_id": case_id,
            "type": type_,
            "index": index,
            "source_file": source_file,
            **clean_extra,
        }
