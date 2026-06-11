# Sistema de Análisis Distribuido de Mensajería

Sistema multiproceso y distribuido que analiza archivos de texto, imagen y audio en busca de amenazas, contenido violento y actividad sospechosa. Implementado con Python 3.11, FastAPI, Kafka y PostgreSQL, desplegado en Docker.

---

## Tabla de contenidos

1. [Descripción del sistema](#descripción-del-sistema)
2. [Arquitectura](#arquitectura)
3. [Patrones de diseño implementados](#patrones-de-diseño-implementados)
4. [Stack tecnológico](#stack-tecnológico)
5. [Estructura del proyecto](#estructura-del-proyecto)
6. [Requisitos](#requisitos)
7. [Levantar el sistema](#levantar-el-sistema)
8. [Demo rápida](#demo-rápida)
9. [Tests](#tests)
10. [Equipo](#equipo)

---

## Descripción del sistema

El sistema recibe casos de análisis que contienen archivos adjuntos (texto, imágenes, audio). Cada archivo se convierte en una sub-tarea que viaja por Kafka al worker especializado correspondiente. Los workers procesan en paralelo y, cuando todos terminan, el ConsolidationWorker genera un reporte con los incidentes detectados.

Conceptos de Sistemas Operativos aplicados:

- **Multiproceso real**: cada worker corre en su propio contenedor (proceso aislado, espacio de memoria independiente).
- **IPC por paso de mensajes**: los procesos se comunican a través de Kafka — nunca por memoria compartida.
- **Sincronización**: semáforos (Bulkhead), condition variables (Aggregator), locks de transacción (Outbox).
- **Scheduling cooperativo**: workers async con `asyncio`; trabajo CPU-bound delegado a hilos via `ThreadPoolExecutor`.

---

## Arquitectura

```
┌──────────────────────────────────────────────────────────────────┐
│                          Cliente                                  │
│              REST (archivos)   GraphQL (consultas)               │
└─────────────────────┬────────────────────────┬───────────────────┘
                      │                        │
┌─────────────────────▼────────────────────────▼───────────────────┐
│                        API  (FastAPI)                             │
│   Builder → Outbox → Splitter → publica en Kafka                 │
└──────────┬───────────────────────────────────────────────────────┘
           │  Kafka topics
    ┌──────┼───────────────────────────────────────┐
    │      │                                       │
    ▼      ▼                                       ▼
┌───────┐ ┌────────────┐ ┌────────────┐   ┌───────────────────────┐
│ Text  │ │   Image    │ │   Audio    │   │  ConsolidationWorker  │
│Worker │ │  Worker    │ │  Worker    │   │  (Aggregator + Saga)  │
└───┬───┘ └─────┬──────┘ └─────┬──────┘   └──────────┬────────────┘
    │           │              │                      │
    └─────────────────────────-┘                      │
              Outbox → analysis.results               │
                                                      ▼
                                              PostgreSQL (CQRS)
                                              Incidents + Evidences
```

---

## Patrones de diseño implementados

### Persona B — Capa de procesamiento (workers)

| Patrón | Archivo | Descripción |
|--------|---------|-------------|
| **Object Pool** | `src/patterns/object_pool.py` | Reutiliza analizadores costosos de inicializar (parsers, lexicons). Evita crear y destruir recursos en cada mensaje. |
| **Bulkhead** | `src/patterns/bulkhead.py` | Semáforo que aísla la concurrencia de cada tipo de worker. Si el worker de imágenes se satura, no bloquea al de texto. |
| **Strategy** | `src/patterns/strategy.py` | Algoritmo de análisis intercambiable: heurística por defecto, ML cuando los modelos están disponibles, sin cambiar el worker. |
| **Decorator** | `src/patterns/decorators.py` | Pipeline componible de logging, métricas, reintentos y timing alrededor de cualquier procesador. |

### Persona A / C — Orquestación e infraestructura

| Patrón | Archivo | Descripción |
|--------|---------|-------------|
| **CQRS** | `src/patterns/cqrs.py` | Separa lecturas (réplicas) de escrituras (primario). |
| **Builder** | `src/patterns/builder.py` | Construye casos validando invariantes antes de persistir. |
| **Outbox** | `src/patterns/outbox.py` | Garantía de entrega: eventos escritos en la misma transacción ACID que el cambio de dominio. |
| **Message Router** | `src/patterns/message_router.py` | Enruta cada sub-tarea al topic de Kafka correcto según el tipo de archivo. |
| **Splitter** | `src/patterns/splitter.py` | Divide un caso en sub-tareas independientes, una por archivo. |
| **Saga** | `src/patterns/saga.py` | Coordina los pasos del flujo con compensación ante fallos. |
| **Aggregator** | `src/patterns/aggregator.py` | Espera todos los resultados de un caso antes de consolidar; tiene timeout configurable. |

---

## Stack tecnológico

| Capa | Tecnología |
|------|-----------|
| API | Python 3.11, FastAPI, Strawberry GraphQL |
| ORM / BD | SQLAlchemy async, Alembic, PostgreSQL 15 |
| Mensajería | Apache Kafka (Confluent), Zookeeper |
| Caché | Redis 7 |
| Observabilidad | Prometheus, Grafana |
| Contenedores | Docker, Docker Compose |
| Tests | pytest, pytest-asyncio, coverage |

---

## Estructura del proyecto

```
.
├── src/
│   ├── api/              # FastAPI: rutas REST + schema GraphQL + resolvers CQRS
│   ├── models/           # Modelos SQLAlchemy (Case, AnalysisResult, Incident, Evidence)
│   ├── patterns/         # 11 patrones de diseño
│   ├── workers/          # TextWorker, ImageWorker, AudioWorker, ConsolidationWorker
│   ├── database/         # Conexión async, migraciones Alembic
│   ├── infrastructure/   # Kafka producer/consumer
│   └── monitoring/       # Configuración Prometheus, Grafana
├── tests/
│   ├── unit/             # 113 tests unitarios (83 % cobertura)
│   ├── integration/      # API → Kafka → workers → DB
│   └── e2e/              # Flujo completo con stack real
├── docker/               # Dockerfiles por servicio + requirements separados
├── docker-compose.yml    # Stack completo (11 servicios)
└── pyproject.toml        # Configuración pytest, ruff, mypy, coverage
```

---

## Requisitos

- Docker Desktop 4.x o superior
- 4 GB RAM disponibles para Docker
- Puertos libres: 8000, 5432, 6379, 9092, 9090, 3000

---

## Levantar el sistema

### Primera vez

```bash
# 1. Clonar y entrar al repo
git clone <url-del-repo>
cd Operating-Systems-Project-2

# 2. Copiar variables de entorno
cp .env.example .env

# 3. Construir imágenes y levantar
docker compose up -d --build

# 4. Correr migraciones (solo la primera vez)
docker compose exec api alembic upgrade head
```

### Arranques siguientes

```bash
docker compose up -d
```

### Verificar que todo esté corriendo

```bash
docker compose ps
# Los 11 servicios deben mostrar "Up"
```

### Apagar

```bash
docker compose down
```

---

## Demo rápida

Con el stack corriendo:

**1. Abrir Swagger UI**
```
http://localhost:8000/docs
```

**2. Autenticarse** — `POST /auth/dev-token`
```json
{
  "user_id": "demo",
  "role": "analyst",
  "expires_in_hours": 24
}
```

**3. Crear un caso** — `POST /upload/case`

Subir cualquier combinación de archivos `.txt`, `.jpg`/`.png`, `.mp3`/`.wav`. Anotar el `case_id` de la respuesta.

**4. Consultar el reporte** — GraphQL en `http://localhost:8000/graphql`
```graphql
query {
  case(id: "CASE_ID") {
    status
    progressPercentage
    incidents {
      category
      severity
      title
      confidenceScore
      evidences {
        sourceFile
        snippet
      }
    }
  }
}
```

El caso pasa de `queued` → `processing` → `completed` en segundos. El reporte final lista los incidentes detectados con su severidad y evidencias.

**5. Ver métricas** — Prometheus en `http://localhost:9090`

Query útil: `up` (todos los servicios monitoreados), `analysis_worker_results_total` (resultados procesados por tipo de worker).

---

## Tests

```bash
# Unit (sin dependencias externas)
pytest tests/unit -o addopts="" -q

# Integration (requiere PostgreSQL y Kafka corriendo)
pytest tests/integration -o addopts="" -q

# E2E (requiere stack completo)
pytest tests/e2e -o addopts="" -q

# Todos con cobertura
pytest
```

Resultado actual: **113 tests unitarios en verde**, cobertura 83 %.

---

## Equipo

| Integrante | Rol |
|-----------|-----|
| **Dylan** | Arquitectura general, API (FastAPI + GraphQL), patrones de orquestación (CQRS, Builder, Outbox, Splitter, Message Router, Saga, Aggregator) |
| **Ian** | Infraestructura Docker, Kafka, Prometheus, Grafana, docker-compose |
| **Arnold** | Workers de procesamiento (Text, Image, Audio, Consolidation), patrones de ejecución (Object Pool, Bulkhead, Strategy, Decorator), tests unitarios FASE 2 |
