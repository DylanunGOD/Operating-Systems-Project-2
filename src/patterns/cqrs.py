"""
Patrón CQRS — Command Query Responsibility Segregation.

Separa operaciones de escritura (Commands) de las de lectura (Queries):

- Commands: cambian estado. Van al Primary de la BD. No retornan datos de dominio
  (a lo sumo un id o un resultado mínimo de la operación).
- Queries: leen estado. Van a Replicas. Nunca cambian nada.

Cada Command/Query se despacha a su Handler correspondiente vía Bus.
Esto facilita:
- Logging/metrics centralizadas (middleware en el bus)
- Tests aislados (mockear el bus o registrar fake handlers)
- Escalado independiente de lectura/escritura
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, TypeVar


# ─────────────────────────────────────────────────────
# Tipos base
# ─────────────────────────────────────────────────────
@dataclass
class Command(ABC):
    """
    Marcador para cualquier intención de cambio de estado.
    Los subtipos son dataclasses con los datos necesarios para ejecutarlo.

    Ej: CreateAnalysisCaseCommand(user_id, title, files)
    """


@dataclass
class Query(ABC):
    """
    Marcador para cualquier intención de lectura.
    Los subtipos son dataclasses con los filtros/parámetros.

    Ej: GetCaseByIdQuery(case_id)
    """


TCommand = TypeVar("TCommand", bound=Command)
TQuery = TypeVar("TQuery", bound=Query)
TResult = TypeVar("TResult")


# ─────────────────────────────────────────────────────
# Handlers
# ─────────────────────────────────────────────────────
class CommandHandler(ABC, Generic[TCommand, TResult]):
    """
    Procesa un tipo concreto de Command.
    Toda la lógica de escritura del dominio vive en handlers.
    """

    @abstractmethod
    async def handle(self, command: TCommand) -> TResult:
        """Ejecuta el command y retorna el resultado (id, ack, etc)."""


class QueryHandler(ABC, Generic[TQuery, TResult]):
    """
    Procesa un tipo concreto de Query.
    Toda la lógica de lectura del dominio vive en handlers.
    """

    @abstractmethod
    async def handle(self, query: TQuery) -> TResult:
        """Ejecuta la query y retorna los datos solicitados."""


# ─────────────────────────────────────────────────────
# Excepciones
# ─────────────────────────────────────────────────────
class HandlerNotRegisteredError(Exception):
    """Se intentó despachar un Command/Query sin handler registrado."""

    def __init__(self, message_type: type) -> None:
        super().__init__(
            f"No hay handler registrado para {message_type.__name__}. "
            "Registra uno con bus.register(MessageType, HandlerInstance)."
        )


class HandlerAlreadyRegisteredError(Exception):
    """Se intentó registrar dos handlers para el mismo Command/Query."""

    def __init__(self, message_type: type) -> None:
        super().__init__(
            f"Ya hay un handler registrado para {message_type.__name__}. "
            "Cada Command/Query solo puede tener un handler."
        )


# ─────────────────────────────────────────────────────
# Buses
# ─────────────────────────────────────────────────────
class CommandBus:
    """
    Despacha Commands al handler registrado.

    Uso:
        bus = CommandBus()
        bus.register(CreateCaseCommand, CreateCaseHandler())
        result = await bus.dispatch(CreateCaseCommand(...))
    """

    def __init__(self) -> None:
        self._handlers: dict[type[Command], CommandHandler] = {}

    def register(
        self,
        command_type: type[TCommand],
        handler: CommandHandler[TCommand, TResult],
    ) -> None:
        """Asocia un tipo de Command con su handler. Un handler por tipo."""
        if command_type in self._handlers:
            raise HandlerAlreadyRegisteredError(command_type)
        self._handlers[command_type] = handler

    async def dispatch(self, command: TCommand) -> TResult:
        """Encuentra el handler del command y lo ejecuta."""
        handler = self._handlers.get(type(command))
        if handler is None:
            raise HandlerNotRegisteredError(type(command))
        return await handler.handle(command)

    def is_registered(self, command_type: type[Command]) -> bool:
        return command_type in self._handlers

    def clear(self) -> None:
        """Útil en tests para limpiar entre casos."""
        self._handlers.clear()


class QueryBus:
    """
    Despacha Queries al handler registrado.

    Uso:
        bus = QueryBus()
        bus.register(GetCaseByIdQuery, GetCaseByIdHandler())
        case = await bus.dispatch(GetCaseByIdQuery(case_id="..."))
    """

    def __init__(self) -> None:
        self._handlers: dict[type[Query], QueryHandler] = {}

    def register(
        self,
        query_type: type[TQuery],
        handler: QueryHandler[TQuery, TResult],
    ) -> None:
        if query_type in self._handlers:
            raise HandlerAlreadyRegisteredError(query_type)
        self._handlers[query_type] = handler

    async def dispatch(self, query: TQuery) -> TResult:
        handler = self._handlers.get(type(query))
        if handler is None:
            raise HandlerNotRegisteredError(type(query))
        return await handler.handle(query)

    def is_registered(self, query_type: type[Query]) -> bool:
        return query_type in self._handlers

    def clear(self) -> None:
        self._handlers.clear()


# ─────────────────────────────────────────────────────
# Singletons del proyecto
# ─────────────────────────────────────────────────────
# Instancias compartidas a las que se registran los handlers en startup.
# Los resolvers/routes hacen import de éstos y llaman a `dispatch`.
command_bus = CommandBus()
query_bus = QueryBus()
