# Flujo de Desarrollo — Sistema de Análisis Distribuido

**Proyecto:** Operating-Systems-Project-2
**Repo:** https://github.com/DylanunGOD/Operating-Systems-Project-2
**Rama de trabajo:** `develop`
**Equipo:** 3 personas (A, B, C)
**Modelo de trabajo:** Lineal por fases (A → B → C → integración)

---

## 📌 División de responsabilidades

Cada persona es **dueña absoluta** de sus archivos durante todo el proyecto.

### 👤 Persona A — Capa de Entrada y Orquestación

**Misión:** todo lo que ocurre desde que llega un request hasta que se publica el evento que dispara a los workers, más la coordinación entre ellos.

| Carpeta / Archivo | Contenido |
|-------------------|-----------|
| `src/api/` (completo) | FastAPI, GraphQL schema, resolvers, REST upload, JWT auth, middleware, health, dependencies |
| `src/patterns/cqrs.py` | CommandBus, QueryBus, handlers base |
| `src/patterns/saga.py` | SagaOrchestrator, SagaStep, compensations |
| `src/patterns/outbox.py` | OutboxPublisher + Poller async |
| `src/patterns/message_router.py` | ContentBasedRouter, MessageRoute |
| `src/patterns/splitter.py` | MessageSplitter (divide casos en sub-tareas) |
| `src/patterns/aggregator.py` | ResultAggregator (con timeout) |
| `src/patterns/builder.py` | AnalysisCaseBuilder (fluent API) |
| `tests/unit/test_patterns.py` (parcial) | Tests de los 8 patrones que toca |
| `tests/integration/test_api_to_kafka.py` | API → outbox → Kafka |
| `tests/integration/test_saga_flow.py` | Flujo completo del orquestador |
| `tests/e2e/test_complete_flow.py` | Cliente → respuesta final |
| `docs/API.md` | Documentación de endpoints |

**Necesita de los otros (contratos):**
- De **B**: `BaseWorker.WORKER_TYPE` y nombres de topics Kafka
- De **C**: `KafkaManager.publish()`, `RedisCache.get/set`, `Repository.save_case()`

---

### 👤 Persona B — Capa de Procesamiento

**Misión:** todo lo que pasa dentro de cada worker — desde que consume un mensaje hasta que escribe el resultado.

| Carpeta / Archivo | Contenido |
|-------------------|-----------|
| `src/workers/` (completo) | base_worker, text_worker, image_worker, audio_worker, consolidation_worker, worker_factory |
| `src/models/case.py` | Modelo SQLAlchemy AnalysisCase |
| `src/models/result.py` | AnalysisResult, Incident, Evidence |
| `src/models/schemas.py` | Pydantic schemas (entrada/salida) |
| `src/patterns/strategy.py` | PriorityStrategy (Simple, SLA, Hybrid) |
| `src/patterns/decorators.py` | CachingDecorator, LoggingDecorator, MetricsDecorator |
| `src/patterns/bulkhead.py` | BulkheadExecutor (ThreadPool por worker) |
| `src/patterns/object_pool.py` | ObjectPool genérico |
| `tests/unit/test_workers.py` | Tests por cada worker |
| `tests/unit/test_models.py` | Tests de modelos y validación |
| `tests/integration/test_workers_to_db.py` | Worker procesa → guarda en BD |
| `tests/load/test_performance.py` | Load test (1000 casos/hora) |
| `docs/PATTERNS.md` | Explicación detallada de cada patrón |

**Necesita de los otros (contratos):**
- De **A**: formato de mensaje JSON que llega por Kafka (definido en docs)
- De **C**: `KafkaWorkerConsumer`, `RedisCache`, `Repository.save_result()`, `db.write_session()`

---

### 👤 Persona C — Plataforma (Data + DevOps)

**Misión:** todo lo que no es lógica de negocio — bases de datos, mensajería, cache, observabilidad, contenedores y despliegue.

| Carpeta / Archivo | Contenido |
|-------------------|-----------|
| `src/database/connection.py` | DatabaseConnection (Primary + Replicas) ✅ ya hecho |
| `src/database/repository.py` | Repository pattern (acceso a datos) |
| `src/database/migrations/` | env.py, alembic.ini, todas las versiones |
| `src/database/replication_config.sql` | Setup de streaming replication |
| `src/infrastructure/config.py` | Settings (Singleton) ✅ ya hecho |
| `src/infrastructure/kafka.py` | KafkaManager, KafkaWorkerConsumer |
| `src/infrastructure/redis_client.py` | RedisCache (cache-aside, locks, pub/sub) |
| `src/monitoring/metrics.py` | Métricas Prometheus (Counter, Gauge, Histogram) |
| `src/monitoring/prometheus_config.yml` | Config de scraping |
| `src/monitoring/alerts.yml` | Reglas de alertas |
| `src/monitoring/grafana_dashboards/*.json` | 4 dashboards (workers, BD, Kafka, SLO) |
| `docker/` (completo) | Dockerfiles api + workers (text, image, audio) |
| `docker-compose.yml` + `.prod.yml` | Stack local + producción |
| `kubernetes/` (completo) | Deployments, StatefulSets, HPA, Ingress, ConfigMap, Secrets |
| `.github/workflows/` | CI/CD (build, test, deploy) |
| `requirements.txt`, `requirements-dev.txt` | **Único responsable de versionar deps** |
| `Makefile`, `.env.example` | Comandos y variables |
| `docs/DEPLOYMENT.md`, `docs/DEVELOPMENT.md` | Setup local y despliegue |

**Interfaces públicas que ofrece a los otros:**
- `KafkaManager.publish(topic, message)`
- `KafkaWorkerConsumer.consume(topic, handler)`
- `RedisCache.get/set/lock`
- `Repository.<save/get/list>_case()`
- `get_db().write_session()` / `read_session()`
- `metrics.counter(...)`, `metrics.histogram(...)`

---

## 📊 Balance de carga

| Persona | Lógica de negocio | Configuración | Tests | Docs |
|---------|-------------------|---------------|-------|------|
| **A** | Alta (CQRS, Saga, Aggregator) | Baja | E2E + integración API | API.md |
| **B** | Alta (ML, multiproceso) | Baja | Unit workers + load test | PATTERNS.md |
| **C** | Media (Repository, Kafka client) | **Alta** (Docker/K8s/Grafana) | Setup de fixtures | DEPLOYMENT.md + DEVELOPMENT.md |

Los 3 tienen carga equivalente, solo distribuida distinta: **A** es la más conceptual, **B** la más técnica de ML, **C** la más operacional.

---

## 🔁 Flujo lineal A → B → C

```
                                    ┌──────────────────────────────────┐
FASE 1 (A)    [API + Patterns]      │ Sistema funciona con FAKES       │
   ↓                                │ • Kafka simulado en consola      │
FASE 2 (B)    [Workers + ML]        │ • Redis simulado en memoria      │
   ↓                                │ • Postgres real (ya está)        │
FASE 3 (C)    [Infra + DevOps]      │ Reemplaza fakes por reales       │
   ↓                                │ Agrega monitoreo y K8s           │
FASE 4 (todos) [Integración]        │ Producción                       │
                                    └──────────────────────────────────┘
```

---

## 🟢 Estado actual (punto de partida)

Ya tenemos hecho:
- ✅ Config (`src/infrastructure/config.py`) — C
- ✅ DB Connection (`src/database/connection.py`) — C
- ✅ Migraciones aplicadas (`src/database/migrations/`) — C
- ✅ Modelos SQLAlchemy + Pydantic (`src/models/`) — B parcial
- ✅ FastAPI base + health (`src/api/main.py`, `src/api/routes/health.py`) — A parcial

---

## FASE 1 — Persona A (API + Orquestación)

**Duración estimada:** 2-3 semanas
**Trabaja con:** fakes/stubs para Kafka, Redis y workers (que aún no existen)

### Orden de implementación

| # | Archivo | Qué hace | Depende de |
|---|---------|----------|------------|
| 1 | `patterns/cqrs.py` | CommandBus, QueryBus, base handlers | nada |
| 2 | `patterns/builder.py` | AnalysisCaseBuilder (fluent API) | modelos ya existen |
| 3 | `patterns/outbox.py` | OutboxPublisher (escribe a tabla `outbox`) | DB ya existe |
| 4 | `patterns/message_router.py` | Routing por tipo de contenido | nada |
| 5 | `patterns/splitter.py` | Divide caso en N sub-tareas | modelos |
| 6 | `api/graphql_schema.py` | Schema Strawberry (queries + mutations) | builder, cqrs |
| 7 | `api/resolvers.py` | Handlers de CreateCase, GetCase, ListCases | cqrs, outbox |
| 8 | `api/routes/upload.py` | Endpoint REST multipart | nada |
| 9 | `api/middleware/auth.py` | JWT validation | nada |
| 10 | `patterns/saga.py` | SagaOrchestrator + steps + compensations | outbox |
| 11 | `patterns/aggregator.py` | ResultAggregator con timeout | redis (fake) |

### Stubs que A escribe temporalmente

- `infrastructure/kafka_fake.py` → versión fake que imprime el mensaje en consola
- `infrastructure/redis_fake.py` → versión fake que usa un dict en memoria

### Cuándo está listo A

- [ ] Levantar la API (`uvicorn src.api.main:app`)
- [ ] `POST /api/cases` crea un caso en BD
- [ ] Se ve el evento publicado (en consola con el fake)
- [ ] `GET /api/cases/{id}` devuelve el caso
- [ ] Tests pasan: `pytest tests/unit/test_patterns.py`

### Entregable

- PR `feature/a/*` → `develop`
- Los 3 revisan los contratos que B y C tendrán que respetar
- Demo del flujo completo con fakes

---

## FASE 2 — Persona B (Workers + ML)

**Duración estimada:** 3-4 semanas
**Empieza con:** la API de A ya funcionando con fakes
**Trabaja con:** fake de Kafka y Redis (los reemplaza al final por los reales que hizo A)

### Orden de implementación

| # | Archivo | Qué hace | Depende de |
|---|---------|----------|------------|
| 1 | `workers/base_worker.py` | Clase base abstracta (consume + process + emit) | cqrs (A) |
| 2 | `workers/worker_factory.py` | Factory que registra workers por tipo | base_worker |
| 3 | `patterns/object_pool.py` | Pool genérico (conexiones, modelos ML) | nada |
| 4 | `patterns/bulkhead.py` | ThreadPoolExecutor por tipo de worker | nada |
| 5 | `patterns/strategy.py` | Simple/SLA/Hybrid priority | nada |
| 6 | `patterns/decorators.py` | Caching, Logging, Metrics decorators | redis fake |
| 7 | `workers/text_worker.py` | spaCy + HuggingFace (sentimiento, keywords) | base, decorators |
| 8 | `workers/image_worker.py` | OpenCV + YOLO (detección objetos) | base, bulkhead |
| 9 | `workers/audio_worker.py` | Whisper (transcripción) | base, bulkhead |
| 10 | `workers/consolidation_worker.py` | Aggregator → genera Incidents | base |

### Cuándo está listo B

- [ ] Correr cada worker standalone: `python -m src.workers.text_worker`
- [ ] Cada worker consume un evento JSON (manual) y escribe resultado en BD
- [ ] Tests pasan: `pytest tests/unit/test_workers.py`
- [ ] Integración: A publica evento → B procesa → resultado en BD (todavía con Kafka fake)

### Entregable

- PR `feature/b/*` → `develop`
- A y C verifican que los contratos de eventos JSON funcionan
- Demo: caso creado por A → procesado por B → reporte completo en BD

---

## FASE 3 — Persona C (Plataforma real)

**Duración estimada:** 2-3 semanas
**Empieza con:** el sistema funcionando end-to-end con fakes (gracias a A y B)
**Misión:** reemplazar fakes por componentes reales y preparar producción

### Orden de implementación

| # | Archivo | Qué hace | Reemplaza |
|---|---------|----------|-----------|
| 1 | `infrastructure/kafka.py` (real) | KafkaManager + KafkaWorkerConsumer con confluent-kafka | fake de A |
| 2 | `infrastructure/redis_client.py` (real) | RedisCache (cache-aside, locks, pub/sub) | fake de A |
| 3 | `database/repository.py` | Repository pattern centralizado | queries dispersas |
| 4 | `database/replication_config.sql` | Setup streaming replication | — |
| 5 | `monitoring/metrics.py` | Counters, Gauges, Histograms Prometheus | — |
| 6 | `monitoring/prometheus_config.yml` | Scrape targets | — |
| 7 | `monitoring/alerts.yml` | Reglas de alerta | — |
| 8 | `monitoring/grafana_dashboards/*.json` | 4 dashboards | — |
| 9 | `docker/Dockerfile.api` + workers | Multi-stage builds | — |
| 10 | `docker-compose.prod.yml` | Stack producción local | — |
| 11 | `kubernetes/*.yaml` | Deployments, StatefulSets, HPA, Ingress | — |
| 12 | `.github/workflows/` | CI/CD pipeline | — |

### Reemplazo de fakes → reales

Después de cada implementación real, C actualiza los imports donde A y B usaban el fake:

```python
# Antes (fake)
from src.infrastructure.kafka_fake import KafkaManager
# Después (real)
from src.infrastructure.kafka import KafkaManager
```

### Cuándo está listo C

- [ ] `docker-compose up` levanta todo: API + 3 workers + Postgres + Redis + Kafka + Prometheus + Grafana
- [ ] Crear un caso → ver en Grafana cómo procesa
- [ ] Tests E2E pasan: `pytest tests/e2e/`
- [ ] CI/CD corre en cada PR

### Entregable

- PR `feature/c/*` → `develop`
- Demo: stack productivo levantado, métricas en Grafana, despliegue a K8s

---

## FASE 4 — Integración + Hardening (los 3 juntos)

**Duración estimada:** 1-2 semanas

- Load testing (1000 casos/hora)
- Replicación BD (1 primary + 2 replicas) → C lidera, A/B revisan
- Documentación final (`docs/ARCHITECTURE.md`, `docs/DEPLOYMENT.md`)
- Despliegue a GKE/EKS
- Demo final

---

## 🌳 Git Workflow

```
main (producción - se toca solo al final de cada fase)
  └── develop (integración - lo que ya funciona junto)
        ├── feature/a/cqrs-base       ← Persona A
        ├── feature/b/text-worker     ← Persona B
        └── feature/c/redis-client    ← Persona C
```

### Reglas

- Cada feature branch parte de `develop` actualizado: `git pull origin develop`
- Branches **cortas** (max 2-3 días). Mejor 5 PRs pequeñas que 1 enorme.
- Nadie hace push directo a `develop` → todo entra por **Pull Request**
- Mínimo **1 reviewer** antes de merge (los otros 2 se revisan entre sí)
- Después de merge en `develop`, los demás hacen `git pull origin develop` y rebasean su branch:
  ```bash
  git checkout feature/mi-branch
  git rebase develop
  ```

### Convención de commits

```
feat(scope): nueva funcionalidad
fix(scope): bug fix
test(scope): tests
docs(scope): documentación
chore(scope): tareas técnicas (deps, config)
refactor(scope): cambio de código sin cambiar comportamiento
```

Ejemplo: `feat(workers): implementar TextWorker con spaCy`

---

## 🤝 Contratos (acordar antes de empezar)

Antes de que cada persona arranque su fase, los 3 deben acordar en una sesión:

1. **Schemas Pydantic** (`src/models/schemas.py`) — quedan **congelados** una vez decididos. Si alguien necesita cambiarlos → PR con revisión de los 3.
2. **Interfaces de los workers** — la clase base `BaseWorker` con qué métodos exactos exponer.
3. **Contratos de eventos Kafka** — qué JSON viaja en cada topic.
4. **Repository interface** — qué métodos públicos tendrá cada repository.

> **Regla de oro:** Si necesitas algo de otro, primero acuerden el contrato (la firma del método o el JSON del evento) y escríbelo en un comentario en el código de quien lo va a implementar. Luego cada uno trabaja con un mock hasta que el dueño entregue la versión real.

---

## 📋 Checklist de inicio para cada persona

### Setup local (los 3)

```powershell
# Clonar y entrar
git clone https://github.com/DylanunGOD/Operating-Systems-Project-2.git
cd Operating-Systems-Project-2
git checkout develop

# Instalar Python 3.11 (obligatorio - 3.13 da problemas con deps)
# https://www.python.org/ftp/python/3.11.9/python-3.11.9-amd64.exe

# Venv
py -3.11 -m venv venv
.\venv\Scripts\Activate.ps1   # Windows
# source venv/bin/activate    # Linux/Mac

# Deps mínimas para arrancar
pip install fastapi==0.111.0 "uvicorn[standard]==0.29.0" python-multipart==0.0.9 sqlalchemy==2.0.36 alembic==1.14.0 asyncpg==0.30.0 psycopg2-binary==2.9.10 redis==5.0.4 pydantic==2.7.1 pydantic-settings==2.2.1 prometheus-client==0.20.0 pytest==8.2.0 pytest-asyncio==0.23.6

# Variables de entorno
Copy-Item .env.example .env

# Levantar PostgreSQL
docker-compose up -d postgres

# Aplicar migraciones
alembic upgrade head

# Correr API
uvicorn src.api.main:app --reload
```

Verificar en navegador:
- http://localhost:8000/ → info básica
- http://localhost:8000/health → liveness
- http://localhost:8000/docs → Swagger UI

---

## ✅ Ventajas del flujo lineal

| Ventaja | Detalle |
|---------|---------|
| **Cero bloqueos** | Cada persona puede empezar sin esperar a nadie (usan fakes) |
| **Validación temprana** | A demuestra que el flujo lógico funciona antes de tocar infra real |
| **Errores aislados** | Si algo falla, sabes en qué fase está el problema |
| **Aprendizaje gradual** | B aprende de la API de A; C aprende del uso real que hicieron A y B |

## ⚠️ Desventajas (y cómo mitigarlas)

| Desventaja | Mitigación |
|------------|------------|
| Cada persona "espera" 2-3 semanas | Pueden trabajar en docs o tests mientras esperan |
| Los fakes pueden ocultar bugs reales | C debe correr tests de integración después de reemplazar cada fake |
| Si A se atrasa, B y C esperan | Definir hitos semanales con demo obligatoria |

---

## 🎯 Hitos clave

| Hito | Quién entrega | Demo |
|------|---------------|------|
| **M1** — Contratos acordados | Los 3 | Documento + diagrama |
| **M2** — API funcional con fakes | A | `POST /api/cases` + ver evento en consola |
| **M3** — Workers procesando | B | Caso end-to-end con fakes |
| **M4** — Stack real corriendo | C | `docker-compose up` + Grafana |
| **M5** — Producción | Los 3 | Desplegado en GKE/EKS |

---

**Última actualización:** 2026-06-02
**Autor:** Dylan
