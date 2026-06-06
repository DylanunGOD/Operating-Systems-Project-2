"""
Patrón Saga — coordinación de transacciones distribuidas.

PROBLEMA:
    Procesar un caso requiere múltiples pasos (text, image, audio,
    consolidación) que ocurren en distintos servicios. Una transacción
    global con locks es imposible (escala mal, acopla servicios).
    Si el paso 3 falla, hay que "deshacer" los pasos 1 y 2 sin un
    `ROLLBACK` distribuido.

SOLUCIÓN:
    Saga ejecuta una secuencia de SagaSteps. Cada step tiene:
    - `execute()`  → su trabajo (idealmente idempotente)
    - `compensate()` → cómo deshacer si una step posterior falla

    Si todos los `execute` salen bien → SagaStatus.COMPLETED.
    Si uno falla → se ejecutan los `compensate()` en ORDEN INVERSO
    sobre los steps ya completados → SagaStatus.FAILED.
    Si alguna compensación también falla → COMPENSATION_FAILED.

EN ESTE PROYECTO:
    La Saga la dispara el CreateCaseHandler tras persistir el caso.
    Sus steps (TextAnalysisStep, ImageAnalysisStep, etc.) son cascarones
    durante FASE 1: emiten eventos a la outbox. Cuando los workers reales
    existan (FASE 2), cada step esperará a la callback del worker.

USO:

    saga = SagaOrchestrator()
    result = await saga.execute(
        aggregate_id=case_id,
        steps=[StepA(), StepB(), StepC()],
        initial_data={"foo": "bar"},
    )
    if result.is_success:
        ...
"""

from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Awaitable
from uuid import uuid4


# ─────────────────────────────────────────────────────
# Estado
# ─────────────────────────────────────────────────────
class SagaStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"                       # compensaciones OK
    COMPENSATION_FAILED = "compensation_failed"  # alguna compensación rota


@dataclass
class StepResult:
    """Resultado de un paso individual."""

    step_name: str
    success: bool
    data: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


@dataclass
class SagaContext:
    """
    Estado compartido entre steps.
    Cada step puede leer/escribir libremente vía `get` / `set`.
    """

    saga_id: str
    aggregate_id: str
    data: dict[str, Any] = field(default_factory=dict)
    step_results: dict[str, StepResult] = field(default_factory=dict)

    def get(self, key: str, default: Any = None) -> Any:
        return self.data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self.data[key] = value


# ─────────────────────────────────────────────────────
# Step (clase base)
# ─────────────────────────────────────────────────────
class SagaStep(ABC):
    """
    Base de un paso de la saga.

    Subclases deben:
    - Definir `name` (str)
    - Implementar `execute(context) -> StepResult`
    - (Opcional) sobrescribir `compensate(context)` para deshacer

    Una compensación que no hace nada es válida cuando el step es seguro
    de re-ejecutar o cuando su efecto sobrevivir está OK.
    """

    name: str = ""

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if not cls.name and cls.__name__ != "FunctionalStep":
            cls.name = cls.__name__

    @abstractmethod
    async def execute(self, context: SagaContext) -> StepResult: ...

    async def compensate(self, context: SagaContext) -> None:
        """Compensación por defecto: no-op."""
        return None


# ─────────────────────────────────────────────────────
# FunctionalStep (azúcar)
# ─────────────────────────────────────────────────────
ExecuteFn = Callable[[SagaContext], Awaitable[StepResult]]
CompensateFn = Callable[[SagaContext], Awaitable[None]]


class FunctionalStep(SagaStep):
    """
    Permite definir un step pasando funciones en vez de crear una subclase.
    Útil para tests y casos simples.

    Ejemplo:
        async def do(ctx):
            ctx.set("foo", "bar")
            return StepResult(step_name="example", success=True)

        async def undo(ctx):
            ctx.data.pop("foo", None)

        step = FunctionalStep("example", do, undo)
    """

    def __init__(
        self,
        name: str,
        execute_fn: ExecuteFn,
        compensate_fn: CompensateFn | None = None,
    ) -> None:
        self.name = name
        self._execute = execute_fn
        self._compensate = compensate_fn

    async def execute(self, context: SagaContext) -> StepResult:
        return await self._execute(context)

    async def compensate(self, context: SagaContext) -> None:
        if self._compensate is not None:
            await self._compensate(context)


# ─────────────────────────────────────────────────────
# Resultado final
# ─────────────────────────────────────────────────────
@dataclass
class SagaResult:
    saga_id: str
    aggregate_id: str
    status: SagaStatus
    context: SagaContext
    failed_step: str | None = None
    error: str | None = None
    compensated_steps: list[str] = field(default_factory=list)

    @property
    def is_success(self) -> bool:
        return self.status == SagaStatus.COMPLETED

    @property
    def is_failure(self) -> bool:
        return self.status in {
            SagaStatus.FAILED,
            SagaStatus.COMPENSATION_FAILED,
        }


# ─────────────────────────────────────────────────────
# Orchestrator
# ─────────────────────────────────────────────────────
HookFn = Callable[..., None]


class SagaOrchestrator:
    """
    Ejecuta sagas. Stateless: una instancia sirve a muchas sagas.

    Hooks opcionales para logging/metrics:
    - on_step_start(step, context)
    - on_step_complete(step, result)
    - on_compensation(step, error_or_none)
    """

    def __init__(
        self,
        *,
        on_step_start: HookFn | None = None,
        on_step_complete: HookFn | None = None,
        on_compensation: HookFn | None = None,
    ) -> None:
        self._on_step_start = on_step_start
        self._on_step_complete = on_step_complete
        self._on_compensation = on_compensation

    async def execute(
        self,
        *,
        aggregate_id: str,
        steps: list[SagaStep],
        initial_data: dict[str, Any] | None = None,
        saga_id: str | None = None,
    ) -> SagaResult:
        if not steps:
            raise ValueError("La saga necesita al menos un step")

        context = SagaContext(
            saga_id=saga_id or str(uuid4()),
            aggregate_id=aggregate_id,
            data=dict(initial_data or {}),
        )

        completed: list[SagaStep] = []

        for step in steps:
            self._safe_hook(self._on_step_start, step, context)

            try:
                result = await step.execute(context)
                if not isinstance(result, StepResult):
                    raise TypeError(
                        f"{step.name}.execute() debe devolver StepResult, "
                        f"recibido: {type(result).__name__}"
                    )
            except Exception as e:
                result = StepResult(
                    step_name=step.name,
                    success=False,
                    error=f"{type(e).__name__}: {e}",
                )

            context.step_results[step.name] = result
            self._safe_hook(self._on_step_complete, step, result)

            if not result.success:
                compensated, comp_failed = await self._compensate(
                    completed, context
                )
                return SagaResult(
                    saga_id=context.saga_id,
                    aggregate_id=aggregate_id,
                    status=(
                        SagaStatus.COMPENSATION_FAILED
                        if comp_failed
                        else SagaStatus.FAILED
                    ),
                    context=context,
                    failed_step=step.name,
                    error=result.error,
                    compensated_steps=compensated,
                )

            completed.append(step)

        return SagaResult(
            saga_id=context.saga_id,
            aggregate_id=aggregate_id,
            status=SagaStatus.COMPLETED,
            context=context,
            compensated_steps=[],
        )

    # ─────────────────────────────────────────
    # Internos
    # ─────────────────────────────────────────
    async def _compensate(
        self, completed: list[SagaStep], context: SagaContext
    ) -> tuple[list[str], bool]:
        """
        Compensa en orden inverso. Continúa aunque alguna compensación falle.
        Devuelve (lista de steps compensados con éxito, hubo_fallo).
        """
        successfully_compensated: list[str] = []
        any_failure = False
        for step in reversed(completed):
            try:
                await step.compensate(context)
                self._safe_hook(self._on_compensation, step, None)
                successfully_compensated.append(step.name)
            except Exception as e:
                any_failure = True
                self._safe_hook(self._on_compensation, step, e)
        return successfully_compensated, any_failure

    @staticmethod
    def _safe_hook(hook: HookFn | None, *args: Any) -> None:
        if hook is None:
            return
        try:
            res = hook(*args)
            if inspect.iscoroutine(res):
                # los hooks deberian ser sync, pero si alguno es async
                # lo descartamos sin propagar (no bloquear la saga)
                res.close()
        except Exception:
            # hooks defectuosos nunca tumban la saga
            pass
