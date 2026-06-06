"""
Modelo SQLAlchemy: OutboxEvent

Representa una fila de la tabla `outbox`, donde se guardan eventos pendientes
de publicar a Kafka como parte del Outbox Pattern.

Garantiza que el evento se escribe en la *misma transacción* que el cambio
de dominio, eliminando la posibilidad de perder eventos si Kafka falla.
"""

from datetime import datetime
from enum import Enum
from uuid import uuid4

from sqlalchemy import JSON, DateTime, Index, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.database.connection import Base


class AggregateType(str, Enum):
    """A qué tipo de agregado pertenece el evento."""

    CASE = "case"
    RESULT = "result"
    INCIDENT = "incident"


class OutboxEvent(Base):
    """
    Evento pendiente de publicar a Kafka.

    Ciclo de vida:
    1. Handler de Command inserta este evento en la misma transacción que
       el cambio de dominio (case nuevo, result generado, etc).
    2. OutboxPublisher (en patterns/outbox.py) lo lee periódicamente.
    3. Publica a Kafka. Si éxito → `published_at = now()`.
       Si falla → `retry_count += 1`, `error_message = ...`.
    """

    __tablename__ = "outbox"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        primary_key=True,
        default=lambda: str(uuid4()),
    )
    # Id del aggregate al que pertenece el evento (ej: case_id)
    aggregate_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), nullable=False
    )
    aggregate_type: Mapped[str] = mapped_column(String(64), nullable=False)
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    # Topic de Kafka al que debe enviarse
    topic: Mapped[str] = mapped_column(String(128), nullable=False)
    # Cuerpo JSON del mensaje
    payload: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=datetime.utcnow,
        nullable=False,
    )
    # NULL = pendiente. Datetime = ya publicado a Kafka.
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    retry_count: Mapped[int] = mapped_column(default=0)
    error_message: Mapped[str | None] = mapped_column(String(2000))

    # El índice parcial ya existe (ix_outbox_pending) en la migración
    __table_args__ = (
        Index("ix_outbox_aggregate_lookup", "aggregate_id", "aggregate_type"),
        {"extend_existing": True},
    )

    @property
    def is_pending(self) -> bool:
        return self.published_at is None

    def __repr__(self) -> str:
        return (
            f"<OutboxEvent id={self.id} topic={self.topic} "
            f"event_type={self.event_type} pending={self.is_pending}>"
        )
