"""
Modelos SQLAlchemy: AnalysisResult, Incident, Evidence

- AnalysisResult: resultado individual de un worker sobre un archivo
- Incident: hallazgo relevante detectado durante el análisis
- Evidence: referencia al archivo/fragmento que respalda un incidente
"""

from datetime import datetime
from enum import Enum
from uuid import uuid4

from sqlalchemy import (
    JSON,
    DateTime,
    Enum as SQLEnum,
    Float,
    ForeignKey,
    Index,
    String,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.database.connection import Base


# ─────────────────────────────────────────────────────
# Enums
# ─────────────────────────────────────────────────────
class WorkerType(str, Enum):
    TEXT = "text"
    IMAGE = "image"
    AUDIO = "audio"


class IncidentSeverity(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class IncidentCategory(str, Enum):
    VIOLENCE = "violence"
    WEAPONS = "weapons"
    HARASSMENT = "harassment"
    THREATS = "threats"
    EXPLICIT_CONTENT = "explicit_content"
    SUSPICIOUS_ACTIVITY = "suspicious_activity"
    OTHER = "other"


# ─────────────────────────────────────────────────────
# AnalysisResult
# ─────────────────────────────────────────────────────
class AnalysisResult(Base):
    """
    Resultado de un worker analizando un archivo específico.
    Cada caso tiene múltiples results (uno por archivo procesado).
    """

    __tablename__ = "analysis_results"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        primary_key=True,
        default=lambda: str(uuid4()),
    )
    case_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        ForeignKey("analysis_cases.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    worker_type: Mapped[WorkerType] = mapped_column(
        SQLEnum(WorkerType, name="worker_type"),
        nullable=False,
    )

    # Referencia al archivo procesado
    source_file: Mapped[str] = mapped_column(String(500), nullable=False)
    source_index: Mapped[int] = mapped_column(default=0)  # posición en el caso

    # ─── Resultados ─────────────────────────────────────────
    # Estructura JSON flexible: cada worker decide qué guardar
    # Ej: {"sentiment": "negative", "score": 0.85, "keywords": [...]}
    data: Mapped[dict] = mapped_column(JSON, default=dict)

    # Métricas de procesamiento
    processing_time_ms: Mapped[int] = mapped_column(default=0)
    confidence_score: Mapped[float | None] = mapped_column(Float, nullable=True)

    # ─── Timestamps ─────────────────────────────────────────
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=datetime.utcnow,
        nullable=False,
    )

    # ─── Relaciones ─────────────────────────────────────────
    case: Mapped["AnalysisCase"] = relationship(
        "AnalysisCase", back_populates="results"
    )

    __table_args__ = (
        Index("ix_result_case_worker", "case_id", "worker_type"),
    )

    def __repr__(self) -> str:
        return (
            f"<AnalysisResult id={self.id} worker={self.worker_type.value} "
            f"file={self.source_file}>"
        )


# ─────────────────────────────────────────────────────
# Incident
# ─────────────────────────────────────────────────────
class Incident(Base):
    """
    Hallazgo relevante detectado durante el análisis del caso.
    Lo genera el ConsolidationWorker al agregar todos los results.
    """

    __tablename__ = "incidents"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        primary_key=True,
        default=lambda: str(uuid4()),
    )
    case_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        ForeignKey("analysis_cases.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    category: Mapped[IncidentCategory] = mapped_column(
        SQLEnum(IncidentCategory, name="incident_category"),
        nullable=False,
        index=True,
    )
    severity: Mapped[IncidentSeverity] = mapped_column(
        SQLEnum(IncidentSeverity, name="incident_severity"),
        nullable=False,
        index=True,
    )

    title: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str] = mapped_column(String(2000), nullable=False)
    confidence_score: Mapped[float] = mapped_column(Float, default=0.0)

    detected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=datetime.utcnow,
        nullable=False,
    )

    case: Mapped["AnalysisCase"] = relationship(
        "AnalysisCase", back_populates="incidents"
    )

    evidences: Mapped[list["Evidence"]] = relationship(
        "Evidence",
        back_populates="incident",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    __table_args__ = (
        Index("ix_incident_severity_category", "severity", "category"),
    )

    def __repr__(self) -> str:
        return (
            f"<Incident id={self.id} category={self.category.value} "
            f"severity={self.severity.value}>"
        )


# ─────────────────────────────────────────────────────
# Evidence
# ─────────────────────────────────────────────────────
class Evidence(Base):
    """
    Evidencia que respalda un incidente: referencia al archivo,
    timestamp en audio/video, bounding box en imagen, etc.
    """

    __tablename__ = "evidences"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        primary_key=True,
        default=lambda: str(uuid4()),
    )
    incident_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        ForeignKey("incidents.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    source_file: Mapped[str] = mapped_column(String(500), nullable=False)
    # Datos específicos del tipo: bbox, timestamp_seconds, char_offset, etc.
    location_data: Mapped[dict] = mapped_column(JSON, default=dict)
    snippet: Mapped[str | None] = mapped_column(String(1000), nullable=True)

    incident: Mapped["Incident"] = relationship(
        "Incident", back_populates="evidences"
    )

    def __repr__(self) -> str:
        return f"<Evidence id={self.id} file={self.source_file}>"
