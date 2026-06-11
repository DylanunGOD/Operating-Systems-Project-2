# Guía de Desarrollo

Setup local del sistema de análisis distribuido. Mantenido por **Persona C**.

## 1. Requisitos

- **Python 3.11** (obligatorio; 3.13+ da problemas con algunas deps).
- **Docker** + Docker Compose (para Postgres/Redis/Kafka locales).
- Git.

## 2. Setup inicial

```powershell
git clone https://github.com/DylanunGOD/Operating-Systems-Project-2.git
cd Operating-Systems-Project-2
git checkout develop

py -3.11 -m venv venv
.\venv\Scripts\Activate.ps1            # Windows
# source venv/bin/activate             # Linux/Mac

# Dependencias de desarrollo (incluye ML; pesado). Para iterar rápido sin ML:
pip install -r requirements-ci.txt
# Stack completo (workers con spaCy/YOLO/Whisper reales):
# pip install -r requirements-dev.txt

Copy-Item .env.example .env
```

## 3. Modos de ejecución

El sistema funciona en dos modos según `KAFKA_ENABLED`:

| Modo | `KAFKA_ENABLED` | Mensajería | Cache | Uso |
|------|-----------------|------------|-------|-----|
| **Dev (fakes)** | `false` (default) | `ConsoleKafkaPublisher` + `InMemoryConsumer` | `InMemoryRedisCache` | Iterar sin infra |
| **Real** | `true` | Kafka real (`KafkaManager` / `KafkaWorkerConsumer`) | `RedisCache` | docker-compose / prod |

En modo dev, los eventos de la outbox se imprimen en consola y los workers
no reciben mensajes reales (degradación elegante). Nada se cae si falta Kafka.

## 4. Correr en local (sin Docker para la app)

```powershell
docker-compose up -d postgres          # solo la BD
alembic upgrade head                   # migraciones
uvicorn src.api.main:app --reload      # API en :8000
```

URLs: `/docs` (Swagger), `/graphql` (GraphiQL), `/health`, `/metrics`.

## 5. Correr el stack completo (Docker)

```powershell
docker-compose up -d                   # API + workers + PG + Redis + Kafka + Prometheus + Grafana
docker-compose logs -f api
```

- Grafana: http://localhost:3000 (admin/admin) — dashboards Workers / SLO / DB / Kafka.
- Prometheus: http://localhost:9090.

## 6. Tests

```powershell
pytest tests/unit/                     # unit (rápido, sin servicios)
pytest tests/ -v                       # todo (integración necesita servicios)
```

`pyproject.toml` aplica cobertura con umbral 80% (`--cov-fail-under=80`).
El módulo `database/repository.py` está excluido de cobertura (I/O contra BD
real, cubierto por integración).

## 7. Calidad

```powershell
ruff check src/ tests/                 # lint
black src/ tests/                      # formato
mypy src/                              # tipos (no estricto)
```

## 8. Migraciones

```powershell
alembic upgrade head                   # aplicar
alembic revision --autogenerate -m "msg"
alembic downgrade -1                   # revertir
```

## 9. Estructura de la capa de plataforma (Persona C)

```
src/infrastructure/
  config.py            Settings (Singleton) + flag KAFKA_ENABLED
  kafka.py             KafkaManager (productor) + KafkaWorkerConsumer + build_consumer
  redis_client.py      RedisCache + InMemoryRedisCache (cache-aside, lock, pub/sub)
src/database/
  connection.py        DatabaseConnection (CQRS: primary + réplicas)
  repository.py        Repository (acceso a datos centralizado)
  replication_config.sql  Setup de streaming replication
src/monitoring/
  metrics.py           PrometheusMetrics (implementa MetricsRecorder de B)
  worker_runner.py     Entrypoint de worker con servidor de métricas
  prometheus_config.yml / alerts.yml / grafana_dashboards/*.json
docker/ kubernetes/ .github/workflows/   Empaquetado y despliegue
```

Ver [ARCHITECTURE.md](ARCHITECTURE.md) y [DEPLOYMENT.md](DEPLOYMENT.md).
