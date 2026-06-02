"""Índices estratégicos para queries frecuentes

Revision ID: 002_indices
Revises: 001_initial
Create Date: 2026-06-02 00:01:00

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "002_indices"
down_revision: Union[str, None] = "001_initial"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ─── analysis_cases ─────────────────────────────────
    op.create_index("ix_case_user_id", "analysis_cases", ["user_id"])
    op.create_index("ix_case_status", "analysis_cases", ["status"])
    op.create_index("ix_case_created_at", "analysis_cases", ["created_at"])
    op.create_index(
        "ix_case_user_status", "analysis_cases", ["user_id", "status"]
    )
    op.create_index(
        "ix_case_status_created", "analysis_cases", ["status", "created_at"]
    )

    # ─── analysis_results ───────────────────────────────
    op.create_index("ix_result_case_id", "analysis_results", ["case_id"])
    op.create_index(
        "ix_result_case_worker",
        "analysis_results",
        ["case_id", "worker_type"],
    )

    # ─── incidents ──────────────────────────────────────
    op.create_index("ix_incident_case_id", "incidents", ["case_id"])
    op.create_index("ix_incident_category", "incidents", ["category"])
    op.create_index("ix_incident_severity", "incidents", ["severity"])
    op.create_index(
        "ix_incident_severity_category",
        "incidents",
        ["severity", "category"],
    )

    # ─── evidences ──────────────────────────────────────
    op.create_index("ix_evidence_incident_id", "evidences", ["incident_id"])

    # ─── outbox ─────────────────────────────────────────
    # Crítico: el poller filtra constantemente por published_at IS NULL
    op.create_index(
        "ix_outbox_pending",
        "outbox",
        ["published_at", "created_at"],
        postgresql_where="published_at IS NULL",
    )
    op.create_index("ix_outbox_aggregate", "outbox", ["aggregate_id"])


def downgrade() -> None:
    op.drop_index("ix_outbox_aggregate", "outbox")
    op.drop_index("ix_outbox_pending", "outbox")

    op.drop_index("ix_evidence_incident_id", "evidences")

    op.drop_index("ix_incident_severity_category", "incidents")
    op.drop_index("ix_incident_severity", "incidents")
    op.drop_index("ix_incident_category", "incidents")
    op.drop_index("ix_incident_case_id", "incidents")

    op.drop_index("ix_result_case_worker", "analysis_results")
    op.drop_index("ix_result_case_id", "analysis_results")

    op.drop_index("ix_case_status_created", "analysis_cases")
    op.drop_index("ix_case_user_status", "analysis_cases")
    op.drop_index("ix_case_created_at", "analysis_cases")
    op.drop_index("ix_case_status", "analysis_cases")
    op.drop_index("ix_case_user_id", "analysis_cases")
