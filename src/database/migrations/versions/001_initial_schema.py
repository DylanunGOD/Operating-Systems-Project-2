"""Schema inicial: analysis_cases, analysis_results, incidents, evidences + tabla outbox

Revision ID: 001_initial
Revises:
Create Date: 2026-06-02 00:00:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "001_initial"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ─────────────────────────────────────────
    # ENUMS
    # ─────────────────────────────────────────
    case_status = postgresql.ENUM(
        "queued",
        "processing",
        "consolidating",
        "completed",
        "failed",
        "timeout",
        name="case_status",
    )
    case_status.create(op.get_bind(), checkfirst=True)

    worker_type = postgresql.ENUM(
        "text", "image", "audio", name="worker_type"
    )
    worker_type.create(op.get_bind(), checkfirst=True)

    incident_category = postgresql.ENUM(
        "violence",
        "weapons",
        "harassment",
        "threats",
        "explicit_content",
        "suspicious_activity",
        "other",
        name="incident_category",
    )
    incident_category.create(op.get_bind(), checkfirst=True)

    incident_severity = postgresql.ENUM(
        "low", "medium", "high", "critical", name="incident_severity"
    )
    incident_severity.create(op.get_bind(), checkfirst=True)

    # ─────────────────────────────────────────
    # analysis_cases
    # ─────────────────────────────────────────
    op.create_table(
        "analysis_cases",
        sa.Column("id", postgresql.UUID(as_uuid=False), primary_key=True),
        sa.Column("user_id", sa.String(64), nullable=False),
        sa.Column("title", sa.String(255), nullable=False),
        sa.Column("description", sa.String(2000)),
        sa.Column(
            "status",
            postgresql.ENUM(name="case_status", create_type=False),
            nullable=False,
            server_default="queued",
        ),
        sa.Column("error_message", sa.String(2000)),
        sa.Column("total_text_items", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_image_items", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_audio_items", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("processed_text_items", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("processed_image_items", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("processed_audio_items", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("case_metadata", postgresql.JSON, nullable=False, server_default="{}"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
    )

    # ─────────────────────────────────────────
    # analysis_results
    # ─────────────────────────────────────────
    op.create_table(
        "analysis_results",
        sa.Column("id", postgresql.UUID(as_uuid=False), primary_key=True),
        sa.Column(
            "case_id",
            postgresql.UUID(as_uuid=False),
            sa.ForeignKey("analysis_cases.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "worker_type",
            postgresql.ENUM(name="worker_type", create_type=False),
            nullable=False,
        ),
        sa.Column("source_file", sa.String(500), nullable=False),
        sa.Column("source_index", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("data", postgresql.JSON, nullable=False, server_default="{}"),
        sa.Column("processing_time_ms", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("confidence_score", sa.Float()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
    )

    # ─────────────────────────────────────────
    # incidents
    # ─────────────────────────────────────────
    op.create_table(
        "incidents",
        sa.Column("id", postgresql.UUID(as_uuid=False), primary_key=True),
        sa.Column(
            "case_id",
            postgresql.UUID(as_uuid=False),
            sa.ForeignKey("analysis_cases.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "category",
            postgresql.ENUM(name="incident_category", create_type=False),
            nullable=False,
        ),
        sa.Column(
            "severity",
            postgresql.ENUM(name="incident_severity", create_type=False),
            nullable=False,
        ),
        sa.Column("title", sa.String(255), nullable=False),
        sa.Column("description", sa.String(2000), nullable=False),
        sa.Column("confidence_score", sa.Float(), nullable=False, server_default="0"),
        sa.Column(
            "detected_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
    )

    # ─────────────────────────────────────────
    # evidences
    # ─────────────────────────────────────────
    op.create_table(
        "evidences",
        sa.Column("id", postgresql.UUID(as_uuid=False), primary_key=True),
        sa.Column(
            "incident_id",
            postgresql.UUID(as_uuid=False),
            sa.ForeignKey("incidents.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("source_file", sa.String(500), nullable=False),
        sa.Column("location_data", postgresql.JSON, nullable=False, server_default="{}"),
        sa.Column("snippet", sa.String(1000)),
    )

    # ─────────────────────────────────────────
    # outbox (Outbox Pattern - eventos pendientes de publicar a Kafka)
    # ─────────────────────────────────────────
    op.create_table(
        "outbox",
        sa.Column("id", postgresql.UUID(as_uuid=False), primary_key=True),
        sa.Column("aggregate_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("aggregate_type", sa.String(64), nullable=False),
        sa.Column("event_type", sa.String(128), nullable=False),
        sa.Column("topic", sa.String(128), nullable=False),
        sa.Column("payload", postgresql.JSON, nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_message", sa.String(2000)),
    )


def downgrade() -> None:
    op.drop_table("outbox")
    op.drop_table("evidences")
    op.drop_table("incidents")
    op.drop_table("analysis_results")
    op.drop_table("analysis_cases")

    # Eliminar enums
    op.execute("DROP TYPE IF EXISTS incident_severity")
    op.execute("DROP TYPE IF EXISTS incident_category")
    op.execute("DROP TYPE IF EXISTS worker_type")
    op.execute("DROP TYPE IF EXISTS case_status")
