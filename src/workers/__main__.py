"""
Entrypoint del paquete de workers.

Permite lanzar un worker con:
    python -m src.workers text
    python -m src.workers image
    python -m src.workers audio
    python -m src.workers consolidation

Los Dockerfiles de cada worker invocan este módulo con su tipo.
"""

from src.workers.worker_factory import main

if __name__ == "__main__":
    main()
