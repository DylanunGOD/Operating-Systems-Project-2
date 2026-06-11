"""
AudioWorker — análisis de audios.

QUÉ HACE:
    Por cada sub-tarea de audio: transcribe el audio a texto y luego corre
    el MISMO análisis de texto del TextWorker sobre la transcripción (así
    una amenaza dicha en un audio se detecta igual que una escrita).

MOTOR DE ANÁLISIS (degradación elegante):
    - Si `openai-whisper` está instalado, un `WhisperAudioAnalyzer` transcribe.
    - Si no, un `HeuristicAudioAnalyzer`: usa la transcripción si vino en el
      payload (ej: provista por otro servicio) y estima la duración por el
      tamaño del archivo. Sin transcripción no hay hallazgos de contenido.
    - El analizador (modelo Whisper) se reutiliza vía Object Pool.

REUTILIZACIÓN:
    Se apoya en `analyze_text_content` del TextWorker → una sola definición
    del léxico de incidentes para texto y audio.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from src.patterns.object_pool import ObjectPool
from src.workers.base_worker import BaseWorker
from src.workers.contracts import WorkerAnalysis, WorkerTask
from src.workers.text_worker import analyze_text_content

# Estimación grosera de duración: bitrate típico de voz comprimida (~16 KB/s).
_APPROX_BYTES_PER_SECOND = 16_000


def _estimate_duration(source_file: str) -> float | None:
    path = Path(source_file) if source_file else None
    if path is None or not path.is_file():
        return None
    try:
        size = path.stat().st_size
    except OSError:
        return None
    return round(size / _APPROX_BYTES_PER_SECOND, 1)


# ─────────────────────────────────────────────────────
# Analizadores (pooleables)
# ─────────────────────────────────────────────────────
class HeuristicAudioAnalyzer:
    """Sin Whisper: usa la transcripción del payload si la hay."""

    engine = "heuristic"

    def run(self, task: WorkerTask) -> WorkerAnalysis:
        transcript = str(task.payload.get("transcript", ""))
        language = str(task.payload.get("language", "unknown"))
        return self._build(task, transcript, language, self.engine)

    @staticmethod
    def _build(
        task: WorkerTask, transcript: str, language: str, engine: str
    ) -> WorkerAnalysis:
        # Analizar la transcripción reutilizando el motor de texto.
        text_analysis = analyze_text_content(
            transcript, task.source_file, engine="heuristic"
        )
        duration = _estimate_duration(task.source_file)
        data: dict[str, Any] = {
            "engine": engine,
            "transcript": transcript[:2000],
            "transcript_length": len(transcript),
            "language": language,
            "duration_seconds": duration,
            "sentiment": text_analysis.data.get("sentiment"),
            "findings_count": len(text_analysis.findings),
        }
        summary = (
            f"Audio (~{duration}s, {language}): "
            f"{len(text_analysis.findings)} hallazgo(s) en la transcripción."
        )
        return WorkerAnalysis(
            data=data,
            findings=text_analysis.findings,
            confidence_score=text_analysis.confidence_score,
            summary=summary,
        )


class WhisperAudioAnalyzer(HeuristicAudioAnalyzer):
    """Transcribe con Whisper y luego analiza la transcripción."""

    engine = "whisper"

    def __init__(self, model: Any) -> None:
        self._model = model

    def run(self, task: WorkerTask) -> WorkerAnalysis:
        transcript, language = self._transcribe(task.source_file)
        if not transcript:
            # Falló la transcripción → caer al payload/heurística.
            transcript = str(task.payload.get("transcript", ""))
            language = str(task.payload.get("language", "unknown"))
        return self._build(task, transcript, language, self.engine)

    def _transcribe(self, source_file: str) -> tuple[str, str]:
        path = Path(source_file) if source_file else None
        if path is None or not path.is_file():
            return "", "unknown"
        try:
            result = self._model.transcribe(str(path))
            return (
                str(result.get("text", "")).strip(),
                str(result.get("language", "unknown")),
            )
        except Exception:
            return "", "unknown"


def _try_load_whisper() -> Any | None:
    """Carga un modelo Whisper si está disponible; si no, None."""
    try:
        import whisper  # type: ignore

        return whisper.load_model("base")
    except Exception:
        return None


def build_audio_analyzer() -> HeuristicAudioAnalyzer:
    """Factory del analizador: Whisper si se puede, heurístico si no."""
    model = _try_load_whisper()
    if model is not None:
        return WhisperAudioAnalyzer(model)
    return HeuristicAudioAnalyzer()


# ─────────────────────────────────────────────────────
# AudioWorker
# ─────────────────────────────────────────────────────
class AudioWorker(BaseWorker):
    """Worker especializado en audio (transcripción + análisis de texto)."""

    content_type = "audio"

    def __init__(
        self,
        *,
        analyzer_pool: ObjectPool[HeuristicAudioAnalyzer] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._pool = analyzer_pool or ObjectPool(
            factory=build_audio_analyzer, max_size=1, min_size=1
        )

    async def on_start(self) -> None:
        await self._pool.warmup()

    async def on_stop(self) -> None:
        await self._pool.close()

    async def analyze(self, task: WorkerTask) -> WorkerAnalysis:
        async with self._pool.lease() as analyzer:
            # Transcripción + análisis son CPU-bound: a un hilo.
            return await asyncio.to_thread(analyzer.run, task)
