"""
Modelo SQLAlchemy: AnalysisCase

Representa un caso de análisis: un conjunto de archivos multimedia
(texto, imágenes, audios) que se procesan en conjunto para generar
un reporte consolidado.
"""

from datetime import datetime
from enum import Enum
from uuid import uuid4

from sqlalchemy import JSON, DateTime, Enum as SQLEnum, Index, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.database.connection import Base


class CaseStatus(str, Enum):
    """Estados posibles de un caso de análisis."""

    QUEUED = "queued"           # Recién creado, esperando workers
    PROCESSING = "processing"   # Workers ejecutándose
    CONSOLIDATING = "consolidating"  # Aggregator generando reporte
    COMPLETED = "completed"     # Reporte listo
    FAILED = "failed"           # Falló (ver error_message)
    TIMEOUT = "timeout"         # Excedió el tiempo máximo


class AnalysisCase(Base):
    """
    Caso de análisis - entidad raíz del sistema.

    Un caso agrupa archivos multimedia, se divide en sub-tareas
    (Splitter pattern) y se consolida en un reporte final
    (Aggregator pattern).
    """

    __tablename__ = "analysis_cases"

    # ─── Identificación ─────────────────────────────────────
    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        primary_key=True,
        default=lambda: str(uuid4()),
    )
    user_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(String(2000), nullable=True)

    # ─── Estado y progreso ──────────────────────────────────
    status: Mapped[CaseStatus] = mapped_column(
        SQLEnum(
            CaseStatus,
            name="case_status",
            values_callable=lambda enum: [e.value for e in enum],
        ),
        nullable=False,
        default=CaseStatus.QUEUED,
        index=True,
    )
    error_message: Mapped[str | None] = mapped_column(String(2000), nullable=True)

    # Contadores de archivos del caso
    total_text_items: Mapped[int] = mapped_column(default=0)
    total_image_items: Mapped[int] = mapped_column(default=0)
    total_audio_items: Mapped[int] = mapped_column(default=0)

    # Contadores de progreso (los actualizan los workers)
    processed_text_items: Mapped[int] = mapped_column(default=0)
    processed_image_items: Mapped[int] = mapped_column(default=0)
    processed_audio_items: Mapped[int] = mapped_column(default=0)

    # ─── Metadatos arbitrarios (JSON) ───────────────────────
    case_metadata: Mapped[dict] = mapped_column(JSON, default=dict)

    # ─── Timestamps ─────────────────────────────────────────
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=datetime.utcnow,
        nullable=False,
        index=True,
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    # ─── Relaciones ─────────────────────────────────────────
    results: Mapped[list["AnalysisResult"]] = relationship(
        "AnalysisResult",
        back_populates="case",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    incidents: Mapped[list["Incident"]] = relationship(
        "Incident",
        back_populates="case",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    # ─── Índices compuestos ─────────────────────────────────
    __table_args__ = (
        Index("ix_case_user_status", "user_id", "status"),
        Index("ix_case_status_created", "status", "created_at"),
    )

    # ─── Helpers ────────────────────────────────────────────
    @property
    def total_items(self) -> int:
        return (
            self.total_text_items
            + self.total_image_items
            + self.total_audio_items
        )

    @property
    def processed_items(self) -> int:
        return (
            self.processed_text_items
            + self.processed_image_items
            + self.processed_audio_items
        )

    @property
    def progress_percentage(self) -> float:
        if self.total_items == 0:
            return 0.0
        return round((self.processed_items / self.total_items) * 100, 2)

    @property
    def is_terminal(self) -> bool:
        """True si el caso ya terminó (no admite más cambios de estado)."""
        return self.status in {
            CaseStatus.COMPLETED,
            CaseStatus.FAILED,
            CaseStatus.TIMEOUT,
        }

    def __repr__(self) -> str:
        return f"<AnalysisCase id={self.id} status={self.status.value}>"
