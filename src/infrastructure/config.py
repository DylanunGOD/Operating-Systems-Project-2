"""
Configuración centralizada del sistema usando Pydantic Settings.
Implementa el patrón Singleton para garantizar una única instancia.
"""

from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Todas las variables de entorno del sistema.
    Se cargan automáticamente desde el archivo .env o variables del sistema.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # ─────────────────────────────────────────
    # API
    # ─────────────────────────────────────────
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    app_env: str = "development"
    secret_key: str = "change-me-in-production"
    # HS256 simétrico (dev). En producción usar RS256 con par de llaves.
    jwt_algorithm: str = "HS256"
    jwt_expiration_hours: int = 24
    jwt_issuer: str = "analysis-distributed-api"

    # ─────────────────────────────────────────
    # PostgreSQL Primary
    # ─────────────────────────────────────────
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "analysis_db"
    postgres_user: str = "postgres"
    postgres_password: str = "postgres"
    database_pool_size: int = 10
    database_max_overflow: int = 20

    # PostgreSQL Replicas
    postgres_replica1_host: str = "localhost"
    postgres_replica1_port: int = 5433
    postgres_replica2_host: str = "localhost"
    postgres_replica2_port: int = 5434

    # ─────────────────────────────────────────
    # Redis
    # ─────────────────────────────────────────
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: str = ""

    # ─────────────────────────────────────────
    # Kafka
    # ─────────────────────────────────────────
    # kafka_enabled=False (dev): la API usa el ConsoleKafkaPublisher y los
    # workers el InMemoryConsumer. Ponerlo en True (docker-compose / prod) para
    # conectar al broker real.
    kafka_enabled: bool = False
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_text: str = "analysis.text.tasks"
    kafka_topic_image: str = "analysis.image.tasks"
    kafka_topic_audio: str = "analysis.audio.tasks"
    kafka_topic_results: str = "analysis.results"
    kafka_group_id: str = "analysis-workers"
    kafka_auto_offset_reset: str = "earliest"
    kafka_client_id: str = "analysis-platform"
    # Sondeo de conectividad al construir un consumer/manager real. Si el
    # broker no responde dentro de este tiempo, la infra lanza
    # KafkaUnavailableError y los workers caen al InMemoryConsumer (modo dev).
    kafka_probe_timeout_seconds: float = 5.0

    # ─────────────────────────────────────────
    # Workers
    # ─────────────────────────────────────────
    text_worker_replicas: int = 2
    image_worker_replicas: int = 2
    audio_worker_replicas: int = 2
    worker_timeout_seconds: int = 300

    # ─────────────────────────────────────────
    # Storage
    # ─────────────────────────────────────────
    storage_bucket: str = "analysis-files"
    storage_provider: str = "gcs"
    gcs_project_id: str = "your-gcp-project"

    # ─────────────────────────────────────────
    # Monitoring
    # ─────────────────────────────────────────
    prometheus_port: int = 9090
    grafana_port: int = 3000
    grafana_admin_password: str = "admin"
    # Puerto en el que cada worker expone /metrics (Prometheus lo scrapea).
    worker_metrics_port: int = 8001

    # ─────────────────────────────────────────
    # Propiedades calculadas
    # ─────────────────────────────────────────
    @property
    def database_url(self) -> str:
        """URL de conexión al nodo primary (escritura)."""
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def database_url_replica1(self) -> str:
        """URL de conexión a la replica 1 (lectura)."""
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_replica1_host}:{self.postgres_replica1_port}/{self.postgres_db}"
        )

    @property
    def database_url_replica2(self) -> str:
        """URL de conexión a la replica 2 (lectura)."""
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_replica2_host}:{self.postgres_replica2_port}/{self.postgres_db}"
        )

    @property
    def redis_url(self) -> str:
        """URL de conexión a Redis."""
        if self.redis_password:
            return f"redis://:{self.redis_password}@{self.redis_host}:{self.redis_port}/{self.redis_db}"
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"

    @property
    def is_production(self) -> bool:
        return self.app_env == "production"

    @property
    def is_development(self) -> bool:
        return self.app_env == "development"


@lru_cache
def get_settings() -> Settings:
    """
    Singleton de Settings usando lru_cache.
    La primera llamada crea la instancia; las siguientes devuelven la misma.

    Uso:
        from src.infrastructure.config import get_settings
        settings = get_settings()
    """
    return Settings()
