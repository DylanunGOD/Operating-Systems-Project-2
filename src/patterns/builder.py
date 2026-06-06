"""
Patrón Builder — construcción fluida de AnalysisCase.

Resuelve dos problemas reales del proyecto:

1. Un AnalysisCase tiene muchos campos opcionales (description, metadata,
   contadores por tipo). El constructor sería un mar de argumentos.

2. Necesitamos validar reglas que cruzan varios campos *antes* de tocar la BD
   (ej: "un caso debe tener al menos un archivo", "el title no puede estar vacío").

El Builder permite:
    case = (
        AnalysisCaseBuilder()
        .for_user("u-123")
        .with_title("Analisis WhatsApp Mayo")
        .with_description("Casos sospechosos del equipo X")
        .with_text_items(50)
        .with_image_items(20)
        .with_audio_items(5)
        .with_metadata({"source": "whatsapp-export"})
        .build()
    )
"""

from __future__ import annotations

from typing import Any

from src.models.case import AnalysisCase, CaseStatus


# ─────────────────────────────────────────────────────
# Excepciones
# ─────────────────────────────────────────────────────
class BuilderValidationError(ValueError):
    """Las reglas de negocio del caso no se cumplen al hacer .build()."""


# ─────────────────────────────────────────────────────
# Builder
# ─────────────────────────────────────────────────────
class AnalysisCaseBuilder:
    """
    Constructor fluido para AnalysisCase.

    Pattern:
        - Cada `with_*` retorna `self` para encadenamiento
        - `.build()` valida invariantes y devuelve el modelo SQLAlchemy listo
          para `session.add()`

    No persiste nada. Solo construye el objeto.
    """

    # Mínimo de items requeridos para que un caso tenga sentido
    MIN_TOTAL_ITEMS = 1
    MAX_TITLE_LENGTH = 255
    MAX_DESCRIPTION_LENGTH = 2000
    MAX_USER_ID_LENGTH = 64

    def __init__(self) -> None:
        self._user_id: str | None = None
        self._title: str | None = None
        self._description: str | None = None
        self._metadata: dict[str, Any] = {}
        self._text_items: int = 0
        self._image_items: int = 0
        self._audio_items: int = 0

    # ─────────────────────────────────────────
    # Setters fluidos (obligatorios)
    # ─────────────────────────────────────────
    def for_user(self, user_id: str) -> AnalysisCaseBuilder:
        """Identifica al dueño del caso."""
        self._user_id = user_id.strip() if user_id else ""
        return self

    def with_title(self, title: str) -> AnalysisCaseBuilder:
        """Título humano del caso."""
        self._title = title.strip() if title else ""
        return self

    # ─────────────────────────────────────────
    # Setters fluidos (opcionales)
    # ─────────────────────────────────────────
    def with_description(self, description: str | None) -> AnalysisCaseBuilder:
        if description is None:
            self._description = None
        else:
            self._description = description.strip() or None
        return self

    def with_metadata(self, metadata: dict[str, Any]) -> AnalysisCaseBuilder:
        """Reemplaza todo el dict de metadata."""
        self._metadata = dict(metadata) if metadata else {}
        return self

    def add_metadata(self, key: str, value: Any) -> AnalysisCaseBuilder:
        """Agrega una sola entrada de metadata sin pisar lo existente."""
        self._metadata[key] = value
        return self

    # ─────────────────────────────────────────
    # Contadores de archivos
    # ─────────────────────────────────────────
    def with_text_items(self, count: int) -> AnalysisCaseBuilder:
        self._text_items = max(0, int(count))
        return self

    def with_image_items(self, count: int) -> AnalysisCaseBuilder:
        self._image_items = max(0, int(count))
        return self

    def with_audio_items(self, count: int) -> AnalysisCaseBuilder:
        self._audio_items = max(0, int(count))
        return self

    # ─────────────────────────────────────────
    # Validación
    # ─────────────────────────────────────────
    def _validate(self) -> None:
        """
        Verifica que el caso tenga sentido antes de instanciarse.
        Acumula errores y los reporta todos juntos.
        """
        errors: list[str] = []

        # user_id
        if not self._user_id:
            errors.append("user_id es obligatorio")
        elif len(self._user_id) > self.MAX_USER_ID_LENGTH:
            errors.append(
                f"user_id excede {self.MAX_USER_ID_LENGTH} caracteres"
            )

        # title
        if not self._title:
            errors.append("title es obligatorio")
        elif len(self._title) > self.MAX_TITLE_LENGTH:
            errors.append(
                f"title excede {self.MAX_TITLE_LENGTH} caracteres"
            )

        # description (opcional pero con tope)
        if (
            self._description is not None
            and len(self._description) > self.MAX_DESCRIPTION_LENGTH
        ):
            errors.append(
                f"description excede {self.MAX_DESCRIPTION_LENGTH} caracteres"
            )

        # Mínimo de items
        total = self._text_items + self._image_items + self._audio_items
        if total < self.MIN_TOTAL_ITEMS:
            errors.append(
                f"el caso debe tener al menos {self.MIN_TOTAL_ITEMS} archivo "
                f"(texto/imagen/audio); recibidos: {total}"
            )

        if errors:
            raise BuilderValidationError(" | ".join(errors))

    # ─────────────────────────────────────────
    # Construcción final
    # ─────────────────────────────────────────
    def build(self) -> AnalysisCase:
        """
        Valida y crea el AnalysisCase listo para `session.add()`.
        Lanza BuilderValidationError si alguna regla no se cumple.
        """
        self._validate()

        return AnalysisCase(
            # id, created_at se autogeneran (defaults del modelo)
            user_id=self._user_id,
            title=self._title,
            description=self._description,
            status=CaseStatus.QUEUED,
            total_text_items=self._text_items,
            total_image_items=self._image_items,
            total_audio_items=self._audio_items,
            case_metadata=self._metadata,
        )

    # ─────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────
    @classmethod
    def from_request(cls, request: Any) -> AnalysisCaseBuilder:
        """
        Atajo: construye un Builder desde un CreateCaseRequest (Pydantic).
        Útil para los handlers/resolvers, que reciben el schema directamente.
        """
        return (
            cls()
            .for_user(request.user_id)
            .with_title(request.title)
            .with_description(getattr(request, "description", None))
            .with_metadata(getattr(request, "metadata", {}) or {})
        )

    def reset(self) -> AnalysisCaseBuilder:
        """Devuelve el builder a su estado inicial (útil en tests)."""
        self.__init__()  # type: ignore[misc]
        return self
