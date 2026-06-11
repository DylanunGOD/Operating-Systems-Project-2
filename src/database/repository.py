"""
database/repository.py — Patrón Repository (acceso a datos centralizado).

Entregable de Persona C (FASE 3). Concentra las consultas dispersas en un único
punto, respetando el routing CQRS de ``DatabaseConnection``:

- Escrituras → ``write_session`` (Primary).
- Lecturas   → ``read_session`` (Réplicas, con fallback al Primary en dev).

Es **aditivo**: A y B pueden adoptarlo gradualmente (sus handlers/stores hoy
usan la sesión directamente). Expone la interfaz pública que el flujo de
desarrollo le pide a C: ``save_case`` / ``get_case`` / ``list_cases`` /
``save_result`` / ``get_results`` / ``save_incident`` / ``get_incidents``.

Nota de cobertura: estos métodos hacen I/O contra PostgreSQL real y se cubren
con tests de integración (``tests/integration``), no con unit tests; por eso el
módulo está en ``omit`` de coverage en ``pyproject.toml``.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Sequence

from sqlalchemy import func, select, update

from src.database.connection import DatabaseConnection, get_db
from src.models.case import AnalysisCase, CaseStatus
from src.models.result import AnalysisResult, Evidence, Incident


class Repository:
    """
    Gateway de datos del dominio. Una instancia por proceso (o usar
    ``get_repository()``). No abre transacciones largas: cada método usa su
    propia sesión y hace commit, salvo los ``*_in_session`` que reciben una
    sesión externa para componerse dentro de la transacción de un handler.
    """

    def __init__(self, db: DatabaseConnection | None = None) -> None:
        self._db = db or get_db()

    # ─────────────────────────────────────────
    # Casos
    # ─────────────────────────────────────────
    async def save_case(self, case: AnalysisCase) -> str:
        """Persiste un caso nuevo y devuelve su id."""
        async with self._db.write_session() as session:
            session.add(case)
            await session.flush()
            case_id = case.id
            await session.commit()
        return case_id

    async def get_case(self, case_id: str) -> AnalysisCase | None:
        async with self._db.read_session() as session:
            return await session.get(AnalysisCase, case_id)

    async def list_cases(
        self,
        *,
        user_id: str | None = None,
        status: CaseStatus | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Sequence[AnalysisCase]:
        stmt = select(AnalysisCase)
        if user_id is not None:
            stmt = stmt.where(AnalysisCase.user_id == user_id)
        if status is not None:
            stmt = stmt.where(AnalysisCase.status == status)
        stmt = (
            stmt.order_by(AnalysisCase.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        async with self._db.read_session() as session:
            return list((await session.execute(stmt)).scalars().all())

    async def update_case_status(
        self,
        case_id: str,
        status: CaseStatus,
        *,
        error_message: str | None = None,
    ) -> None:
        terminal = {CaseStatus.COMPLETED, CaseStatus.FAILED, CaseStatus.TIMEOUT}
        values: dict[str, Any] = {"status": status}
        if error_message is not None:
            values["error_message"] = error_message
        if status in terminal:
            values["completed_at"] = datetime.utcnow()
        async with self._db.write_session() as session:
            await session.execute(
                update(AnalysisCase)
                .where(AnalysisCase.id == case_id)
                .values(**values)
            )
            await session.commit()

    # ─────────────────────────────────────────
    # Resultados
    # ─────────────────────────────────────────
    async def save_result(self, result: AnalysisResult) -> str:
        async with self._db.write_session() as session:
            session.add(result)
            await session.flush()
            result_id = result.id
            await session.commit()
        return result_id

    async def get_results(self, case_id: str) -> Sequence[AnalysisResult]:
        stmt = select(AnalysisResult).where(AnalysisResult.case_id == case_id)
        async with self._db.read_session() as session:
            return list((await session.execute(stmt)).scalars().all())

    # ─────────────────────────────────────────
    # Incidentes
    # ─────────────────────────────────────────
    async def save_incident(self, incident: Incident) -> str:
        async with self._db.write_session() as session:
            session.add(incident)
            await session.flush()
            incident_id = incident.id
            await session.commit()
        return incident_id

    async def get_incidents(self, case_id: str) -> Sequence[Incident]:
        stmt = select(Incident).where(Incident.case_id == case_id)
        async with self._db.read_session() as session:
            return list((await session.execute(stmt)).scalars().all())

    async def get_evidences(self, incident_id: str) -> Sequence[Evidence]:
        stmt = select(Evidence).where(Evidence.incident_id == incident_id)
        async with self._db.read_session() as session:
            return list((await session.execute(stmt)).scalars().all())

    # ─────────────────────────────────────────
    # Agregados / métricas de dominio
    # ─────────────────────────────────────────
    async def count_cases_by_status(self) -> dict[str, int]:
        stmt = select(AnalysisCase.status, func.count()).group_by(
            AnalysisCase.status
        )
        async with self._db.read_session() as session:
            rows = (await session.execute(stmt)).all()
        return {status.value: count for status, count in rows}

    async def count_open_incidents(self, case_id: str) -> int:
        stmt = select(func.count()).where(Incident.case_id == case_id)
        async with self._db.read_session() as session:
            return int((await session.execute(stmt)).scalar() or 0)


_repository: Repository | None = None


def get_repository() -> Repository:
    """Singleton perezoso del Repository (comparte el pool de ``get_db()``)."""
    global _repository
    if _repository is None:
        _repository = Repository()
    return _repository
