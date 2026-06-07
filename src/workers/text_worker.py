"""
TextWorker — análisis de mensajes de texto.

QUÉ HACE:
    Por cada sub-tarea de texto: lee el contenido, detecta entidades,
    estima el sentimiento y —lo más importante para este sistema— escanea
    en busca de indicios de incidentes (amenazas, violencia, armas, acoso).
    Cada indicio se devuelve como un `Finding` que la consolidación
    convierte en `Incident`.

MOTOR DE ANÁLISIS (degradación elegante):
    - Si `spaCy` + un modelo están instalados, se usa un `SpacyTextAnalyzer`
      para NER. Si no (entorno liviano / tests), un `TextAnalyzer` heurístico.
    - El analizador es caro de construir (cargar el modelo), así que se
      reutiliza vía un Object Pool: una sola instancia atiende muchas tareas.
    - El campo `engine` del resultado indica qué motor se usó.

REUTILIZACIÓN:
    `analyze_text_content` y `scan_findings` los reutiliza el AudioWorker
    sobre la transcripción del audio.
"""

from __future__ import annotations

import asyncio
import re
from pathlib import Path
from typing import Any

from src.models.result import IncidentCategory, IncidentSeverity
from src.patterns.object_pool import ObjectPool
from src.workers.base_worker import BaseWorker
from src.workers.contracts import Finding, WorkerAnalysis, WorkerTask


# ─────────────────────────────────────────────────────
# Léxico de detección (heurística base)
# ─────────────────────────────────────────────────────
# category -> (severity por defecto, términos disparadores). Bilingüe
# (es/en) porque los exports de mensajería suelen mezclar idiomas.
LEXICON: dict[IncidentCategory, tuple[IncidentSeverity, set[str]]] = {
    IncidentCategory.THREATS: (
        IncidentSeverity.HIGH,
        {"matar", "te voy a", "amenaza", "kill", "i will hurt", "threat",
         "te mato", "vas a morir"},
    ),
    IncidentCategory.VIOLENCE: (
        IncidentSeverity.HIGH,
        {"golpear", "pegar", "violencia", "sangre", "beat", "violence",
         "attack", "atacar", "paliza"},
    ),
    IncidentCategory.WEAPONS: (
        IncidentSeverity.CRITICAL,
        {"pistola", "arma", "cuchillo", "bomba", "gun", "knife", "weapon",
         "bomb", "rifle", "explosivo"},
    ),
    IncidentCategory.HARASSMENT: (
        IncidentSeverity.MEDIUM,
        {"acoso", "insulto", "idiota", "estupido", "harass", "bully",
         "loser", "imbecil"},
    ),
    IncidentCategory.EXPLICIT_CONTENT: (
        IncidentSeverity.MEDIUM,
        {"explicito", "desnudo", "nude", "explicit", "porn"},
    ),
    IncidentCategory.SUSPICIOUS_ACTIVITY: (
        IncidentSeverity.LOW,
        {"droga", "vender", "ilegal", "drugs", "deal", "smuggle",
         "contrabando", "lavado"},
    ),
}

_POSITIVE_WORDS = {
    "bueno", "genial", "gracias", "feliz", "excelente", "good", "great",
    "thanks", "happy", "love", "amor",
}
_NEGATIVE_WORDS = {
    "malo", "odio", "terrible", "horrible", "triste", "bad", "hate",
    "awful", "sad", "angry", "enojado",
}

_WORD_RE = re.compile(r"[\wáéíóúñü]+", re.IGNORECASE | re.UNICODE)


# ─────────────────────────────────────────────────────
# Funciones de análisis (reutilizables)
# ─────────────────────────────────────────────────────
def _tokenize(text: str) -> list[str]:
    return [w.lower() for w in _WORD_RE.findall(text)]


def _sentiment(tokens: list[str]) -> tuple[str, float]:
    """Sentimiento léxico simple. Devuelve (label, score en [-1, 1])."""
    if not tokens:
        return "neutral", 0.0
    pos = sum(1 for t in tokens if t in _POSITIVE_WORDS)
    neg = sum(1 for t in tokens if t in _NEGATIVE_WORDS)
    total = pos + neg
    if total == 0:
        return "neutral", 0.0
    score = (pos - neg) / total
    label = "positive" if score > 0.1 else "negative" if score < -0.1 else "neutral"
    return label, round(score, 3)


def _snippet(text: str, term: str, *, width: int = 60) -> str:
    """Extrae un fragmento alrededor de la primera aparición del término."""
    idx = text.lower().find(term.lower())
    if idx < 0:
        return text[:width].strip()
    start = max(0, idx - width // 2)
    end = min(len(text), idx + len(term) + width // 2)
    return text[start:end].strip()


def scan_findings(text: str, source_file: str) -> list[Finding]:
    """
    Escanea el texto contra el léxico y devuelve un Finding por categoría
    detectada (con el término que disparó la coincidencia como evidencia).
    """
    lowered = text.lower()
    findings: list[Finding] = []
    for category, (severity, terms) in LEXICON.items():
        matched = sorted({t for t in terms if t in lowered})
        if not matched:
            continue
        # Más términos distintos coincidentes → más confianza (cap 0.95).
        confidence = min(0.95, 0.55 + 0.1 * len(matched))
        findings.append(
            Finding(
                category=category.value,
                severity=severity.value,
                title=f"Posible {category.value} en texto",
                description=(
                    f"Se detectaron términos asociados a "
                    f"'{category.value}': {', '.join(matched)}."
                ),
                confidence=round(confidence, 3),
                source_file=source_file,
                snippet=_snippet(text, matched[0]),
                location_data={"matched_terms": matched},
            )
        )
    return findings


def analyze_text_content(
    text: str,
    source_file: str,
    *,
    engine: str = "heuristic",
    extra: dict[str, Any] | None = None,
) -> WorkerAnalysis:
    """
    Núcleo del análisis de texto, independiente del worker. Lo usa el
    TextWorker (sobre el archivo) y el AudioWorker (sobre la transcripción).
    """
    tokens = _tokenize(text)
    sentiment_label, sentiment_score = _sentiment(tokens)
    findings = scan_findings(text, source_file)

    data: dict[str, Any] = {
        "engine": engine,
        "word_count": len(tokens),
        "char_count": len(text),
        "sentiment": sentiment_label,
        "sentiment_score": sentiment_score,
        "findings_count": len(findings),
    }
    if extra:
        data.update(extra)

    # Confianza global = la del finding más fuerte, o la magnitud del sentimiento.
    confidence = max(
        (f.confidence for f in findings), default=abs(sentiment_score)
    )
    summary = (
        f"{len(tokens)} palabras, sentimiento {sentiment_label}, "
        f"{len(findings)} hallazgo(s)."
    )
    return WorkerAnalysis(
        data=data,
        findings=findings,
        confidence_score=round(confidence, 3) if confidence else None,
        summary=summary,
    )


# ─────────────────────────────────────────────────────
# Lectura del contenido
# ─────────────────────────────────────────────────────
def read_text_source(task: WorkerTask) -> str:
    """
    Obtiene el texto a analizar. Prioridad:
    1. El archivo en disco (`source_file`) si existe.
    2. `payload['content']` si vino embebido.
    3. "" (ej: placeholders de FASE 1 sin archivo real).
    """
    path = Path(task.source_file) if task.source_file else None
    if path is not None and path.is_file():
        try:
            return path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            pass
    embedded = task.payload.get("content")
    return str(embedded) if embedded else ""


# ─────────────────────────────────────────────────────
# Analizadores (pooleables) — heurístico y spaCy
# ─────────────────────────────────────────────────────
class TextAnalyzer:
    """Analizador heurístico. Síncrono (CPU-bound), pensado para correr en hilo."""

    engine = "heuristic"

    def run(self, text: str, source_file: str) -> WorkerAnalysis:
        return analyze_text_content(
            text, source_file, engine=self.engine,
            extra={"entities": self._entities(text)},
        )

    def _entities(self, text: str) -> list[dict[str, str]]:
        return []  # la heurística no extrae entidades nombradas


class SpacyTextAnalyzer(TextAnalyzer):
    """Analizador respaldado por spaCy (si está instalado)."""

    engine = "spacy"

    def __init__(self, nlp: Any) -> None:
        self._nlp = nlp

    def _entities(self, text: str) -> list[dict[str, str]]:
        try:
            doc = self._nlp(text[:100_000])  # cota defensiva de tamaño
            return [
                {"text": e.text, "label": getattr(e, "label_", "")}
                for e in getattr(doc, "ents", [])
            ]
        except Exception:
            return []


def _try_load_spacy() -> Any | None:
    """Carga un modelo de spaCy si está disponible; si no, None."""
    try:
        import spacy  # type: ignore
    except ImportError:
        return None
    for model in ("es_core_news_sm", "en_core_web_sm"):
        try:
            return spacy.load(model)
        except Exception:
            continue
    return None


def build_text_analyzer() -> TextAnalyzer:
    """Factory del analizador: spaCy si se puede, heurístico si no."""
    nlp = _try_load_spacy()
    return SpacyTextAnalyzer(nlp) if nlp is not None else TextAnalyzer()


# ─────────────────────────────────────────────────────
# TextWorker
# ─────────────────────────────────────────────────────
class TextWorker(BaseWorker):
    """Worker especializado en texto (NER + sentimiento + detección)."""

    content_type = "text"

    def __init__(
        self, *, analyzer_pool: ObjectPool[TextAnalyzer] | None = None, **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        # Object Pool de analizadores: construir uno es caro (carga de modelo),
        # así que se comparten y reutilizan entre tareas.
        self._pool = analyzer_pool or ObjectPool(
            factory=build_text_analyzer, max_size=2, min_size=1
        )

    async def on_start(self) -> None:
        await self._pool.warmup()

    async def on_stop(self) -> None:
        await self._pool.close()

    async def analyze(self, task: WorkerTask) -> WorkerAnalysis:
        text = read_text_source(task)
        async with self._pool.lease() as analyzer:
            # El análisis es CPU-bound: a un hilo para no bloquear el loop.
            return await asyncio.to_thread(analyzer.run, text, task.source_file)
