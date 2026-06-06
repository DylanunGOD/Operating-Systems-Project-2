"""
Patrón Message Router — Content-Based Routing.

PROBLEMA:
    Cuando llega un nuevo caso, contiene archivos heterogéneos (texto,
    imágenes, audios). Cada tipo debe ir a un topic distinto de Kafka
    para que lo consuma su worker especializado.
    Hardcodear `if type == "text": publish_to_text` en el handler vuelve
    el código rígido y difícil de extender (¿y si mañana hay videos?).

SOLUCIÓN:
    Un `ContentBasedRouter` que registra reglas (predicate + destino).
    Cuando llega un mensaje, evalúa todas las reglas y devuelve los topics
    que matchean. El handler simplemente publica al/los topics resultantes.

USO TÍPICO (en el Splitter o el resolver):

    router = build_default_router()
    for item in case.items:
        topics = router.route(item)
        for topic in topics:
            await kafka.publish(topic, item)

EXTENDER:
    router.add_route(Route(
        name="video-content",
        predicate=match_field("type", "video"),
        topic="analysis.video.tasks",
    ))
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from src.infrastructure.config import get_settings


# Tipo del predicado
Predicate = Callable[[dict[str, Any]], bool]


# ─────────────────────────────────────────────────────
# Route
# ─────────────────────────────────────────────────────
@dataclass(frozen=True)
class Route:
    """
    Regla de enrutamiento.

    Args:
        name: identificador legible (para logging/debug).
        predicate: función que recibe el mensaje (dict) y devuelve True
                   si debe enviarse al topic de esta ruta.
        topic: topic de Kafka destino si el predicate matchea.
    """

    name: str
    predicate: Predicate
    topic: str


# ─────────────────────────────────────────────────────
# Excepciones
# ─────────────────────────────────────────────────────
class NoRouteMatchedError(Exception):
    """No hay ninguna ruta que matchee con el mensaje recibido."""

    def __init__(self, message: dict[str, Any]) -> None:
        keys = sorted(message.keys())
        super().__init__(
            f"Ningun route matcheo con el mensaje. Keys: {keys}"
        )


class DuplicateRouteError(Exception):
    """Se intentó registrar dos rutas con el mismo nombre."""

    def __init__(self, name: str) -> None:
        super().__init__(f"Ya existe una ruta con nombre '{name}'")


# ─────────────────────────────────────────────────────
# Router
# ─────────────────────────────────────────────────────
class ContentBasedRouter:
    """
    Router que decide el topic destino según el contenido del mensaje.

    - Las rutas se evalúan en orden de registro.
    - Un mismo mensaje puede matchear varias rutas (multi-topic broadcasting).
    - Es thread-safe para lecturas (las rutas se registran al iniciar y no
      se modifican en runtime).
    """

    def __init__(self) -> None:
        self._routes: list[Route] = []
        self._names: set[str] = set()

    # ─────────────────────────────────────────
    # Registro
    # ─────────────────────────────────────────
    def add_route(self, route: Route) -> ContentBasedRouter:
        """Agrega una ruta. Devuelve self para encadenamiento."""
        if route.name in self._names:
            raise DuplicateRouteError(route.name)
        self._routes.append(route)
        self._names.add(route.name)
        return self

    def clear(self) -> None:
        """Útil en tests."""
        self._routes.clear()
        self._names.clear()

    @property
    def routes(self) -> list[Route]:
        """Copia inmutable de las rutas registradas."""
        return list(self._routes)

    # ─────────────────────────────────────────
    # Routing
    # ─────────────────────────────────────────
    def route(self, message: dict[str, Any]) -> list[str]:
        """
        Devuelve todos los topics cuyas rutas matcheen con el mensaje.
        Lista vacía si ninguna matchea.
        """
        return [r.topic for r in self._routes if _safe_eval(r, message)]

    def route_one(self, message: dict[str, Any]) -> str:
        """
        Devuelve el primer topic que matchee. Lanza NoRouteMatchedError
        si ninguno aplica.

        Útil cuando esperás que el mensaje vaya a un único destino.
        """
        for r in self._routes:
            if _safe_eval(r, message):
                return r.topic
        raise NoRouteMatchedError(message)

    def has_match(self, message: dict[str, Any]) -> bool:
        return any(_safe_eval(r, message) for r in self._routes)


def _safe_eval(route: Route, message: dict[str, Any]) -> bool:
    """
    Ejecuta el predicate aislando excepciones.
    Si el predicate truena (ej: KeyError por campo faltante), tratamos
    como "no matchea" en vez de tumbar el router.
    """
    try:
        return bool(route.predicate(message))
    except Exception:
        return False


# ─────────────────────────────────────────────────────
# Factory de predicados comunes
# ─────────────────────────────────────────────────────
def match_field(field: str, expected: Any) -> Predicate:
    """`message[field] == expected`"""
    return lambda m: m.get(field) == expected


def match_field_in(field: str, values: set | list | tuple) -> Predicate:
    """`message[field] ∈ values`"""
    accepted = set(values)
    return lambda m: m.get(field) in accepted


def has_field(field: str) -> Predicate:
    """`field ∈ message`"""
    return lambda m: field in m


def match_type(type_value: str) -> Predicate:
    """Atajo para `match_field('type', type_value)`."""
    return match_field("type", type_value)


def match_all(*predicates: Predicate) -> Predicate:
    """AND lógico de varios predicados."""
    return lambda m: all(p(m) for p in predicates)


def match_any(*predicates: Predicate) -> Predicate:
    """OR lógico de varios predicados."""
    return lambda m: any(p(m) for p in predicates)


# ─────────────────────────────────────────────────────
# Router por defecto del proyecto
# ─────────────────────────────────────────────────────
def build_default_router() -> ContentBasedRouter:
    """
    Router con las 3 rutas estándar del sistema:
    text -> analysis.text.tasks
    image -> analysis.image.tasks
    audio -> analysis.audio.tasks

    Los topics salen de Settings, así que cambiarlos no requiere tocar código.
    """
    settings = get_settings()
    router = ContentBasedRouter()
    router.add_route(
        Route(
            name="text-content",
            predicate=match_type("text"),
            topic=settings.kafka_topic_text,
        )
    )
    router.add_route(
        Route(
            name="image-content",
            predicate=match_type("image"),
            topic=settings.kafka_topic_image,
        )
    )
    router.add_route(
        Route(
            name="audio-content",
            predicate=match_type("audio"),
            topic=settings.kafka_topic_audio,
        )
    )
    return router
