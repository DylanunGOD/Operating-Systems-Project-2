# Documentación Técnica — FASE 1 (Persona A)

**Proyecto:** Análisis Multiproceso y Distribuido de Datos de Mensajería
**Repo:** https://github.com/DylanunGOD/Operating-Systems-Project-2
**Rama:** `develop`
**Estado:** ✅ FASE 1 completa y funcional
**Fecha:** 2026-06-06

---

## 0. Resumen de lo que tienes funcionando

Una API en `localhost:8000` que:

- **Recibe casos** vía GraphQL (`mutation createCase`) o REST multipart (`POST /upload/case`)
- **Autentica usuarios** con JWT (HS256)
- **Valida invariantes de dominio** antes de tocar la BD
- **Persiste casos en PostgreSQL** dentro de transacciones atómicas
- **Emite eventos a una tabla `outbox`** en la misma transacción → garantiza cero pérdida de eventos
- **Divide el caso en N sub-tareas** automáticamente (texto / imagen / audio)
- **Encola sub-tareas en topics Kafka** (vía un Outbox Publisher async listo para conectarse)
- **Lista y consulta casos** con CQRS (escrituras al primary, lecturas a réplicas)
- **Ofrece esqueletos para coordinación distribuida**: Saga (orchestrator con compensaciones) y Aggregator (espera múltiples resultados con timeout)

Lo que NO tiene aún (territorio de B y C):
- Workers reales (text/image/audio) — pendiente FASE 2
- Conexión real a Kafka, Redis y replicación PostgreSQL — pendiente FASE 3

---

## 1. Stack tecnológico actual

| Capa | Tecnología | Versión | Para qué |
|------|------------|---------|----------|
| Lenguaje | Python | 3.11.9 | Runtime principal |
| Web framework | FastAPI | 0.111.0 | API async, OpenAPI automático |
| API server | Uvicorn | 0.29.0 | ASGI server con hot-reload |
| GraphQL | Strawberry | 0.227.0 | Schema type-safe, GraphiQL UI |
| ORM | SQLAlchemy | 2.0.36 | Modelos async, conexión a Primary + Réplicas |
| Migraciones | Alembic | 1.14.0 | Versionado del schema BD |
| Driver async | asyncpg | 0.30.0 | Driver nativo async para PostgreSQL |
| Driver sync | psycopg2-binary | 2.9.10 | Driver fallback (compatibilidad) |
| Validación | Pydantic | 2.7.1 | Schemas y validación de inputs |
| Config | pydantic-settings | 2.2.1 | Carga de variables de entorno |
| JWT | python-jose | 3.3.0 | Generación y verificación de tokens |
| BD | PostgreSQL | 15-alpine | Primary (réplicas pendientes para C) |
| Container | Docker | Compose | Stack local |

---

## 2. Arquitectura general

```
                     ┌─────────────────────────────┐
                     │  Cliente HTTP (curl, web)   │
                     └─────────────────────────────┘
                                  │
                                  ▼
                     ┌─────────────────────────────┐
                     │   FastAPI (Uvicorn :8000)   │
                     │                             │
                     │  ┌───────────┐ ┌──────────┐ │
                     │  │ GraphQL   │ │ REST     │ │
                     │  │ /graphql  │ │ /upload  │ │
                     │  │           │ │ /auth    │ │
                     │  │           │ │ /health  │ │
                     │  └───────────┘ └──────────┘ │
                     │       │           │         │
                     │       ▼           ▼         │
                     │  ┌────────────────────┐    │
                     │  │ JWT Middleware     │    │
                     │  └────────────────────┘    │
                     │       │                    │
                     │       ▼                    │
                     │  ┌────────────────────┐    │
                     │  │ CQRS Buses         │    │
                     │  │ CommandBus         │    │
                     │  │ QueryBus           │    │
                     │  └────────────────────┘    │
                     │       │                    │
                     │       ▼                    │
                     │  ┌─────────────────────┐   │
                     │  │ Handlers            │   │
                     │  │ CreateCaseHandler   │   │
                     │  │ GetCaseByIdHandler  │   │
                     │  │ ListCasesHandler    │   │
                     │  └─────────────────────┘   │
                     │     │      │      │        │
                     │     ▼      ▼      ▼        │
                     │  Builder Splitter Outbox   │
                     │  ┌────┐  ┌────┐  ┌────┐    │
                     │  │AC  │  │MR  │  │evt │    │
                     │  │Bld │  │+   │  │tabla│   │
                     │  │    │  │Spl │  │    │    │
                     │  └────┘  └────┘  └────┘    │
                     └────────────┬────────────────┘
                                  │
                                  ▼
                     ┌─────────────────────────────┐
                     │  PostgreSQL (Docker :5432)  │
                     │                             │
                     │  analysis_cases             │
                     │  analysis_results           │
                     │  incidents / evidences      │
                     │  outbox  ← eventos pendientes│
                     └─────────────────────────────┘
                                  │
                                  ▼
                     ┌─────────────────────────────┐
                     │  OutboxPublisher (async)    │
                     │  ConsoleKafkaPublisher (fake)│
                     │  → en FASE 3: Kafka real    │
                     └─────────────────────────────┘
```

---

## 3. Estructura de carpetas implementada

```
Operating-Systems-Project-2/
├── alembic.ini                            ← Config de Alembic (raíz del proyecto)
├── docker-compose.yml                     ← Stack local
├── .env / .env.example                    ← Variables de entorno
├── requirements.txt                       ← Dependencias (C las versionará)
├── pyproject.toml                         ← Metadata + pytest config
├── Makefile                               ← Comandos rápidos
│
└── src/
    ├── api/                               ← Capa de entrada HTTP
    │   ├── main.py                        ← FastAPI app + lifespan
    │   ├── graphql_schema.py              ← Schema Strawberry
    │   ├── resolvers.py                   ← Commands/Queries + handlers
    │   ├── dependencies.py
    │   ├── middleware/
    │   │   ├── auth.py                    ← JWT (dependency + helpers)
    │   │   └── logging.py
    │   └── routes/
    │       ├── health.py                  ← /health y /health/ready
    │       ├── auth.py                    ← /auth/dev-token y /auth/me
    │       └── upload.py                  ← /upload/case (multipart)
    │
    ├── patterns/                          ← Los 11 patrones de diseño
    │   ├── cqrs.py                        ← ✅ CommandBus + QueryBus
    │   ├── builder.py                     ← ✅ AnalysisCaseBuilder
    │   ├── outbox.py                      ← ✅ Outbox + OutboxPublisher
    │   ├── message_router.py              ← ✅ ContentBasedRouter
    │   ├── splitter.py                    ← ✅ MessageSplitter
    │   ├── saga.py                        ← ✅ SagaOrchestrator
    │   ├── aggregator.py                  ← ✅ ResultAggregator
    │   ├── strategy.py                    ← Pendiente B (FASE 2)
    │   ├── decorators.py                  ← Pendiente B
    │   ├── bulkhead.py                    ← Pendiente B
    │   └── object_pool.py                 ← Pendiente B
    │
    ├── models/                            ← Modelos SQLAlchemy
    │   ├── case.py                        ← AnalysisCase + CaseStatus
    │   ├── result.py                      ← AnalysisResult, Incident, Evidence
    │   ├── outbox.py                      ← OutboxEvent
    │   └── schemas.py                     ← Schemas Pydantic
    │
    ├── database/
    │   ├── connection.py                  ← DatabaseConnection (CQRS routing)
    │   └── migrations/
    │       ├── env.py                     ← Setup Alembic
    │       ├── script.py.mako             ← Template para nuevas migraciones
    │       └── versions/
    │           ├── 001_initial_schema.py  ← 5 tablas + 4 enums
    │           └── 002_add_indices.py     ← Índices estratégicos
    │
    ├── infrastructure/
    │   ├── config.py                      ← Settings (Pydantic + Singleton)
    │   ├── kafka.py                       ← Pendiente C
    │   └── redis_client.py                ← Pendiente C
    │
    └── workers/                           ← Pendiente B (FASE 2)
```

---

## 4. Los 7 patrones de diseño implementados en FASE 1

### 4.1 CQRS — Command Query Responsibility Segregation
**Archivo:** `src/patterns/cqrs.py`

**¿Qué es?**
Patrón que separa las operaciones de escritura (Commands) de las de lectura (Queries) en dos pipelines distintos, con buses independientes que despachan a handlers específicos.

**¿Por qué lo aplicamos?**
- Las escrituras y lecturas tienen patrones de carga muy distintos (ratio 1:10 típicamente).
- Permite escalar la lectura horizontalmente (réplicas BD) sin afectar la escritura.
- Hace que la lógica sea testeable en aislamiento (cada handler se prueba solo).

**Alcance en nuestro proyecto:**
- Toda la API GraphQL (mutations y queries) y REST pasan por el bus.
- 1 Command implementado: `CreateCaseCommand`.
- 2 Queries implementadas: `GetCaseByIdQuery`, `ListCasesQuery`.
- Buses singleton (`command_bus`, `query_bus`) accesibles desde cualquier resolver.

**Beneficios concretos:**
1. Cuando C implemente las réplicas reales, las queries automáticamente usarán `read_session()` (ya enrutado) sin cambiar el código de los resolvers.
2. Si mañana queremos cachear lecturas con Redis, se hace en un decorador del QueryHandler sin tocar la API.
3. Los tests E2E pueden registrar handlers fake en los buses para mockear toda la lógica de dominio.

**Interfaz pública:**
```python
command_bus.register(CommandType, HandlerInstance)
result = await command_bus.dispatch(CommandType(...))

query_bus.register(QueryType, HandlerInstance)
result = await query_bus.dispatch(QueryType(...))
```

---

### 4.2 Builder — AnalysisCaseBuilder
**Archivo:** `src/patterns/builder.py`

**¿Qué es?**
Patrón que construye objetos complejos paso a paso mediante una API fluida (encadenable), separando la construcción del producto final.

**¿Por qué lo aplicamos?**
- `AnalysisCase` tiene muchos campos opcionales — un constructor sería un mar de argumentos posicionales.
- Las invariantes de negocio (mínimo 1 archivo, longitudes máximas) se cruzan entre campos, mejor validar todo junto.
- Hace la API legible: leer un test cuenta una historia.

**Alcance:**
- Se usa en el `CreateCaseHandler` de FASE 1.
- Helper `from_request(pydantic_request)` integrado para usar desde resolvers.

**Beneficios concretos:**
1. Si mañana agregamos un campo (ej: `priority`), solo se agrega un método `with_priority()` sin romper código existente.
2. La validación es centralizada y acumula errores (te dice todos los problemas de una sola vez, no de uno en uno).
3. Inmutable: cada caso se construye en aislamiento, ideal para concurrencia.

**Ejemplo:**
```python
case = (
    AnalysisCaseBuilder()
    .for_user("u-123")
    .with_title("Análisis WhatsApp")
    .with_description("Casos sospechosos del equipo X")
    .with_text_items(50)
    .with_image_items(20)
    .with_audio_items(5)
    .with_metadata({"source": "whatsapp-export"})
    .build()  # ← valida + retorna AnalysisCase listo para session.add()
)
```

---

### 4.3 Outbox Pattern
**Archivo:** `src/patterns/outbox.py` + `src/models/outbox.py`

**¿Qué es?**
Patrón que garantiza la publicación de eventos a un broker externo (Kafka) sin riesgo de pérdida, escribiendo el evento en una tabla `outbox` de la BD dentro de la misma transacción que el cambio de dominio.

**¿Por qué lo aplicamos?**
- Si escribimos en BD y luego publicamos a Kafka, hay una **ventana de inconsistencia**: si Kafka cae justo en medio, el cambio queda persistido pero el evento se pierde.
- El Outbox elimina esa ventana usando atomicidad de la BD: o se guardan ambas cosas, o ninguna.

**Alcance:**
- Implementado al 100%: modelo `OutboxEvent`, helper `add_outbox_event()`, poller `OutboxPublisher`.
- Tabla `outbox` con índice parcial `WHERE published_at IS NULL` para que el poller barra solo eventos pendientes.
- `ConsoleKafkaPublisher` (fake) listo para desarrollo. Persona C lo reemplaza por KafkaManager real en FASE 3.

**Beneficios concretos:**
1. **Cero pérdida de eventos**, incluso si Kafka está caído durante minutos.
2. Reintentos automáticos con `retry_count` y `error_message` por evento.
3. `reset_failed()` permite reintentar eventos bloqueados después de arreglar un problema.

**Flujo dentro de la transacción del handler:**
```python
async with db.write_session() as session:
    session.add(case)               # 1. crear caso
    await session.flush()           # 2. obtiene case.id
    add_outbox_event(session, ...)  # 3. encolar evento (mismo trx)
    await session.commit()          # 4. todo atómico
```

---

### 4.4 Message Router — Content-Based Routing
**Archivo:** `src/patterns/message_router.py`

**¿Qué es?**
Patrón que decide a qué destino enviar un mensaje según su contenido, mediante reglas (predicados + topic destino).

**¿Por qué lo aplicamos?**
- Cada tipo de archivo (texto, imagen, audio) debe ir a un topic Kafka distinto para que lo consuma su worker especializado.
- Hardcodear `if type == "text": ...` en el handler vuelve el código rígido y difícil de extender.

**Alcance:**
- `ContentBasedRouter` con rutas registrables.
- Router por defecto del proyecto con 3 rutas: text → `analysis.text.tasks`, image → `analysis.image.tasks`, audio → `analysis.audio.tasks`.
- Predicados seguros: si un predicate truena, se trata como "no matchea" sin romper el router.

**Beneficios concretos:**
1. Agregar un tipo nuevo (ej: video) requiere solo `router.add_route(...)`, sin tocar el código existente.
2. A/B testing de routing: dos rutas pueden matchear el mismo mensaje (multi-topic broadcasting).
3. Los topics se leen desde Settings, así que cambiarlos en producción solo requiere un env var.

**Predicados disponibles:**
- `match_field("type", "text")`
- `match_field_in("priority", {"high", "critical"})`
- `has_field("attachment_url")`
- `match_all(p1, p2)` (AND)
- `match_any(p1, p2)` (OR)

---

### 4.5 Splitter — MessageSplitter
**Archivo:** `src/patterns/splitter.py`

**¿Qué es?**
Patrón que divide un mensaje grande en N mensajes pequeños independientes, cada uno con su propia identidad, para que puedan procesarse en paralelo.

**¿Por qué lo aplicamos?**
- Un caso puede tener 50 mensajes + 20 imágenes + 5 audios. Procesarlos como una sola unidad bloquearía un worker mucho tiempo.
- Dividirlos en sub-tareas independientes habilita el paralelismo horizontal (más réplicas de workers = más rápido).

**Alcance:**
- `MessageSplitter` que toma items y produce `SubTask` (frozen dataclass).
- Cada `SubTask` lleva: `task_id` (UUID único), `case_id`, `type`, `index`, `source_file`, `topic` (decidido por router), `payload`.
- Dos APIs: `split(text_items, image_items, audio_items)` (explícita) y `split_items(items)` (clasifica automáticamente).
- Integrado con `ContentBasedRouter` (el splitter delega la decisión del topic).

**Beneficios concretos:**
1. Workers reciben tareas autocontenidas: no necesitan cargar el caso completo desde BD para procesar.
2. Índices secuenciales por tipo (`text:0, text:1, image:0`) facilitan rastreo y debugging.
3. UUIDs únicos previenen procesamiento duplicado (idempotencia).

---

### 4.6 Saga Pattern
**Archivo:** `src/patterns/saga.py`

**¿Qué es?**
Patrón para coordinar transacciones distribuidas. Una saga es una secuencia de operaciones donde cada paso tiene su `execute()` y opcionalmente su `compensate()`. Si un paso falla, se ejecutan las compensaciones de los pasos previos en orden inverso.

**¿Por qué lo aplicamos?**
- Procesar un caso involucra múltiples servicios (TextWorker, ImageWorker, AudioWorker, ConsolidationWorker). Una transacción ACID global es imposible.
- Si TextWorker tuvo éxito pero ImageWorker falla, necesitamos "deshacer" lo que hizo TextWorker (marcar resultados como inválidos, liberar recursos, etc).

**Alcance:**
- `SagaOrchestrator` ejecuta sagas secuenciales con compensaciones LIFO.
- `SagaStep` (clase base) y `FunctionalStep` (azúcar para definir steps con funciones).
- Hooks observables: `on_step_start`, `on_step_complete`, `on_compensation`.
- Manejo robusto de errores: excepciones en `execute()` se capturan como `StepResult.failure`; hooks defectuosos nunca tumban la saga.

**Beneficios concretos:**
1. Consistencia eventual sin locks distribuidos.
2. Compensaciones siempre se ejecutan en orden inverso (LIFO), incluso si una falla.
3. Estado completo (`SagaResult`) con `failed_step`, `compensated_steps`, contexto preservado para debugging.

**Estados posibles:**
- `COMPLETED` — todos los steps OK
- `FAILED` — un step falló pero todas las compensaciones OK
- `COMPENSATION_FAILED` — un step falló Y alguna compensación también

**Esqueleto, no aún integrado:** Está listo para usarse en FASE 2 cuando los workers existan. Los steps actuales serían:
```python
[TextAnalysisStep, ImageAnalysisStep, AudioAnalysisStep, ConsolidationStep]
```

---

### 4.7 Aggregator — ResultAggregator
**Archivo:** `src/patterns/aggregator.py`

**¿Qué es?**
Patrón que recolecta múltiples resultados parciales y los consolida cuando todos llegan (o cuando expira un timeout).

**¿Por qué lo aplicamos?**
- Un caso se divide en N sub-tareas, cada una procesada por un worker distinto y emitida en momento distinto.
- Para generar el reporte final el ConsolidationWorker necesita **esperar a que todas terminen** (o aceptar resultados parciales si tarda demasiado).

**Alcance:**
- `ResultAggregator` con ventanas asíncronas (`asyncio.Event` por dentro).
- API: `start_window`, `add_part`, `wait`, `discard`, `snapshot`.
- Estado: `OPEN` / `COMPLETE` / `TIMEOUT`.
- Soporta múltiples ventanas en paralelo (un aggregator atiende a varios casos al mismo tiempo).
- Timeout configurable por ventana o global.

**Beneficios concretos:**
1. Si un worker se cuelga, el caso no queda bloqueado para siempre: `TIMEOUT` cierra la ventana con resultados parciales.
2. Idempotencia: re-agregar la misma parte solo actualiza su valor.
3. Implementación en memoria por ahora. Persona C puede reemplazarla por una Redis-backed sin cambiar la API pública.

**Esqueleto, no aún integrado:** En FASE 2, cuando los workers existan, el flujo será:
```python
# Al crear el caso:
await result_aggregator.start_window(case.id, expected_total=7)

# Cada worker al terminar:
await result_aggregator.add_part(case_id, "text:0", result_data)

# El ConsolidationWorker:
result = await result_aggregator.wait(case_id, timeout_seconds=300)
if result.is_complete:
    generate_report(result.parts)
```

---

## 5. La capa de datos

### 5.1 Modelos SQLAlchemy

**Archivos:** `src/models/case.py`, `result.py`, `outbox.py`

| Tabla | Modelo | Qué guarda |
|-------|--------|-----------|
| `analysis_cases` | `AnalysisCase` | El caso raíz (user_id, title, status, contadores por tipo, metadata JSON) |
| `analysis_results` | `AnalysisResult` | Resultado individual de un worker por archivo (data JSON flexible) |
| `incidents` | `Incident` | Hallazgo relevante (categoría, severidad, confidence) |
| `evidences` | `Evidence` | Referencia al archivo/fragmento que respalda un incidente |
| `outbox` | `OutboxEvent` | Eventos pendientes de publicar a Kafka |

**4 enums tipados:**
- `CaseStatus`: queued, processing, consolidating, completed, failed, timeout
- `WorkerType`: text, image, audio
- `IncidentSeverity`: low, medium, high, critical
- `IncidentCategory`: violence, weapons, harassment, threats, explicit_content, suspicious_activity, other

**Relaciones (con cascade delete):**
```
AnalysisCase (1) ─┬─ (N) AnalysisResult
                  └─ (N) Incident (1) ─ (N) Evidence
```

### 5.2 Connection — Routing CQRS

**Archivo:** `src/database/connection.py`

`DatabaseConnection` mantiene **3 engines async**:
- 1 al Primary (escrituras)
- 2 a las Réplicas (lecturas, round-robin random)

Cada engine maneja su propio **Object Pool** de conexiones (10 fijas + 20 overflow). Esto es ya un patrón Object Pool implícito.

API expuesta:
- `db.write_session()` → contextmanager con sesión al Primary
- `db.read_session()` → contextmanager con sesión a una replica
- Singleton via `get_db()` con `@lru_cache`

En dev solo tenemos primary corriendo, así que `read_session` falla a réplicas. Por eso `/health/ready` reporta `degraded` (esperado).

### 5.3 Migraciones — Alembic

**Archivos:** `alembic.ini` (raíz) + `src/database/migrations/`

Dos migraciones aplicadas:
1. **`001_initial_schema`** — 5 tablas + 4 enums + 5 FK con cascade
2. **`002_add_indices`** — 12 índices estratégicos, incluido un **índice parcial** en `outbox` (`WHERE published_at IS NULL`) para que el poller barra solo eventos pendientes con costo mínimo

**Comandos útiles:**
```bash
alembic upgrade head           # aplica todas las pendientes
alembic downgrade -1           # revierte la última
alembic revision -m "msg" --autogenerate  # crea nueva auto-detectada
```

---

## 6. La capa de API

### 6.1 FastAPI + Lifespan

**Archivo:** `src/api/main.py`

El `create_app()` factory:
1. Crea la app con metadata y Swagger en `/docs`
2. Monta middleware CORS
3. Monta routers: `health`, `auth`, `upload`, `graphql`
4. Lifespan async:
   - **startup**: inicializa pool de DB + registra handlers CQRS
   - **shutdown**: cierra pool limpiamente

### 6.2 GraphQL — Strawberry

**Archivo:** `src/api/graphql_schema.py`

Schema generado: 9 types, 4 enums, 1 input, 2 scalars custom (`DateTime`, `JSON`).

**Queries:**
- `ping: String` — health del schema
- `case(id: String): CaseGQL` — caso por id
- `cases(userId, status, limit, offset): [CaseSummaryGQL]` — lista filtrada

**Mutations:**
- `createCase(input: CreateCaseInput): CreateCaseResult` — crea un caso

Los resolvers son thin: solo despachan al `command_bus` / `query_bus`. Toda la lógica vive en los handlers de `resolvers.py`.

**UI integrada:** GraphiQL en `http://localhost:8000/graphql` (solo dev).

### 6.3 REST Upload Multipart

**Archivo:** `src/api/routes/upload.py`

`POST /upload/case` — recibe archivos y crea el caso.

- Form fields: `user_id`, `title`, `description`
- Files: multipart, hasta 500 archivos
- Clasificación automática por MIME type:
  - `text/*` (+ JSON, XML) → text worker
  - `image/*` → image worker
  - `audio/*` → audio worker
  - Otros → rechazados (registrados en metadata pero no fallan el upload)
- Storage local: `./uploads/<session_uuid>/<filename>` (en FASE 3 → GCS/S3)
- Despacha al mismo `CommandBus` que GraphQL → reutiliza toda la lógica

### 6.4 Autenticación JWT

**Archivos:** `src/api/middleware/auth.py` + `routes/auth.py`

**Algoritmo:** HS256 simétrico (firma con `secret_key`). En prod, Persona C migra a RS256 con par de llaves cambiando solo `Settings.jwt_algorithm`.

**Tipos:**
- `Role` enum: `admin`, `analyst`, `viewer`
- `TokenPayload` — claims decodificados
- `AuthenticatedUser` — vista que reciben los endpoints

**Dependencies de FastAPI:**
- `get_current_user` — obligatorio, lanza 401 si falta o inválido
- `get_optional_user` — opcional
- `require_role(Role.ADMIN, ...)` — exige roles específicos (403 si insuficiente)

**Endpoints REST:**
- `POST /auth/dev-token` (solo dev) — emite token sin verificar credenciales
- `GET /auth/me` (protegido) — devuelve claims del token

Por ahora `/graphql` y `/upload/case` **no exigen auth** para no romper tests. Se endurece en FASE 3.

### 6.5 Health checks

**Archivo:** `src/api/routes/health.py`

- `GET /health` — liveness probe (la app está viva). Para K8s `livenessProbe`.
- `GET /health/ready` — readiness con verificación de Primary + Replicas. Para K8s `readinessProbe`.

---

## 7. Flujo completo de un caso (con FASE 1)

### 7.1 Vía GraphQL

```
1. Cliente envía:
   mutation { createCase(input: {userId, title, textItemsCount, ...}) }

2. Strawberry resolver → command_bus.dispatch(CreateCaseCommand(...))

3. CreateCaseHandler.handle():
   a. AnalysisCaseBuilder().for_user(...).build()
      → valida invariantes (user_id, title, mínimo 1 archivo)
   b. async with db.write_session() as session:
      ├─ session.add(case)
      ├─ await session.flush()  # genera case.id
      │
      ├─ add_outbox_event(session, event_type="case.created", ...)
      │
      ├─ MessageSplitter().split(case_id, text_items, image_items, audio_items)
      │     → genera N SubTasks (UUID único, topic via Router)
      │
      ├─ for st in subtasks:
      │     add_outbox_event(session, event_type=f"analysis.{st.type}.requested",
      │                      topic=st.topic, payload=st.payload)
      │
      └─ await session.commit()  # ✅ TODO ATÓMICO

4. CreateCaseResult { caseId, status=QUEUED, message="Caso encolado con N sub-tareas" }
   → respuesta GraphQL al cliente

5. (Background, cuando exista OutboxPublisher en lifespan):
   - Poller barre tabla outbox WHERE published_at IS NULL
   - Para cada evento: kafka.publish(topic, payload)
   - Marca published_at = now()
```

### 7.2 Vía REST Upload

```
1. Cliente sube vía multipart:
   POST /upload/case
   user_id=..., title=..., files=@a.txt;type=text/plain, files=@b.png;type=image/png

2. upload.upload_case():
   a. Genera session_id UUID
   b. Crea ./uploads/<session_id>/
   c. Para cada file:
      - classify_mime(file.content_type) → "text" | "image" | "audio" | None
      - Si soportado: guarda en disco
      - Si no: registra en rejected
   d. Despacha CreateCaseCommand al mismo bus que GraphQL
      → mismo handler, misma lógica

3. Respuesta JSON con case_id, files_received, upload_session_id
```

---

## 8. Estado actual de la BD

Tras la verificación end-to-end:

```sql
analysis_cases:    2 casos creados (1 GraphQL + 1 REST)
outbox:           13 eventos pendientes (5 case.created + 8 *.requested)
analysis_results:  0  (los crearán los workers en FASE 2)
incidents:         0  (los creará ConsolidationWorker en FASE 2)
evidences:         0
```

Cada evento de outbox tiene:
- `topic` (analysis.cases.events / analysis.text.tasks / analysis.image.tasks / analysis.audio.tasks)
- `payload` JSON con todos los datos del caso/sub-tarea
- `published_at = NULL` (esperando al OutboxPublisher en FASE 3)
- `retry_count = 0`

---

## 9. Cómo levantar y probar

### Setup inicial (una vez):

```powershell
# 1. Instalar Python 3.11 (obligatorio)
# 2. Clonar repo
git clone https://github.com/DylanunGOD/Operating-Systems-Project-2.git
cd Operating-Systems-Project-2
git checkout develop

# 3. Crear venv
py -3.11 -m venv venv
.\venv\Scripts\Activate.ps1

# 4. Instalar deps
pip install fastapi==0.111.0 "uvicorn[standard]==0.29.0" python-multipart==0.0.9 `
    sqlalchemy==2.0.36 alembic==1.14.0 asyncpg==0.30.0 psycopg2-binary==2.9.10 `
    redis==5.0.4 pydantic==2.7.1 pydantic-settings==2.2.1 prometheus-client==0.20.0 `
    pytest==8.2.0 pytest-asyncio==0.23.6 `
    "strawberry-graphql[fastapi]==0.227.0" `
    "python-jose[cryptography]==3.3.0"

# 5. Variables de entorno
Copy-Item .env.example .env
```

### Cada vez que se trabaja:

```powershell
# 1. Activar venv
.\venv\Scripts\Activate.ps1

# 2. Levantar Postgres
docker-compose up -d postgres

# 3. Aplicar migraciones (la primera vez o tras cambios)
alembic upgrade head

# 4. Levantar API
uvicorn src.api.main:app --reload
```

### URLs útiles:

- http://localhost:8000/ — info básica
- http://localhost:8000/docs — Swagger UI (REST)
- http://localhost:8000/graphql — GraphiQL UI (GraphQL)
- http://localhost:8000/health — liveness
- http://localhost:8000/health/ready — readiness con checks BD

### Test rápido GraphQL (en GraphiQL):

```graphql
mutation {
  createCase(input: {
    userId: "demo"
    title: "Mi primer caso"
    textItemsCount: 3
    imageItemsCount: 2
    audioItemsCount: 1
  }) {
    caseId
    status
    message
  }
}
```

```graphql
query {
  cases(userId: "demo", limit: 10) {
    id
    title
    status
    progressPercentage
  }
}
```

### Test rápido REST (curl/Postman):

```bash
# Obtener token
TOKEN=$(curl -X POST http://localhost:8000/auth/dev-token \
  -H "Content-Type: application/json" \
  -d '{"user_id":"demo","role":"analyst"}' \
  | jq -r .access_token)

# Verificar
curl -H "Authorization: Bearer $TOKEN" http://localhost:8000/auth/me

# Upload
curl -X POST http://localhost:8000/upload/case \
  -F "user_id=demo" \
  -F "title=Demo" \
  -F "files=@mensaje.txt;type=text/plain" \
  -F "files=@foto.jpg;type=image/jpeg"
```

---

## 10. Decisiones técnicas notables

### 10.1 Por qué FastAPI + Strawberry juntos
- FastAPI da REST + Swagger automático + dependencies tipadas.
- Strawberry da GraphQL type-safe con dataclasses (no decoradores oscuros).
- Ambos viven en la misma app, comparten el mismo bus de CQRS.

### 10.2 Por qué HS256 para JWT en dev
- RS256 (asimétrico) requiere generar par de llaves PEM y distribuirlas. Para dev es overkill.
- HS256 firma con `secret_key` directamente.
- Persona C cambia a RS256 en prod mediante env var, sin tocar código.

### 10.3 Por qué outbox usa índice parcial
- La tabla `outbox` crecerá indefinidamente (eventos publicados se quedan para auditoría).
- El poller solo necesita eventos pendientes. Un índice normal indexaría TODO.
- `WHERE published_at IS NULL` solo indexa los pendientes → consulta del poller es O(log pendientes) en vez de O(log total).

### 10.4 Por qué los enums usan `values_callable`
- SQLAlchemy por defecto serializa enums usando el nombre del miembro (`QUEUED`).
- PostgreSQL ENUM espera el valor (`queued`).
- `values_callable=lambda e: [v.value for v in e]` fuerza el valor correcto.

### 10.5 Por qué CQRS antes de tener réplicas reales
- El routing `write_session` / `read_session` está listo para cuando las réplicas existan.
- En dev ambos caen al primary; en prod se separan automáticamente.
- Persona C solo necesita configurar `POSTGRES_REPLICA1_HOST` y `POSTGRES_REPLICA2_HOST`.

### 10.6 Por qué fakes en vez de Kafka real
- Acoplar A a Kafka real bloquearía progreso hasta que C entregue infra.
- `ConsoleKafkaPublisher` (Protocol-based) cumple el contrato → A puede testear toda la pipeline lógica.
- Cuando C entregue `KafkaManager`, solo se cambia el import en `main.py`.

---

## 11. Contratos que A definió y que B/C deben respetar

### 11.1 Schemas Pydantic (`src/models/schemas.py`)
**Congelados.** Si B o C necesitan cambiarlos, requiere PR con los 3 de acuerdo.

### 11.2 Modelos SQLAlchemy
Los modelos `AnalysisCase`, `AnalysisResult`, `Incident`, `Evidence`, `OutboxEvent` están definidos. B puede agregar relaciones y métodos helper, pero no romper las columnas existentes.

### 11.3 Estructura de eventos Kafka
Cada SubTask emite con esta forma:
```json
{
  "case_id": "uuid",
  "type": "text" | "image" | "audio",
  "index": 0,
  "source_file": "ruta/al/archivo",
  "priority": "...",          // opcional
  "sender": "...",            // opcional, propaga campos extra del item
}
```
Topics: `analysis.text.tasks`, `analysis.image.tasks`, `analysis.audio.tasks`.

### 11.4 Interfaz `KafkaPublisher` (Protocol)
```python
class KafkaPublisher(Protocol):
    async def publish(self, topic: str, payload: dict) -> None: ...
```
Cualquier implementación (real o fake) debe cumplir esto.

### 11.5 Buses CQRS
B y C pueden registrar nuevos Commands/Queries con `command_bus.register(...)` / `query_bus.register(...)`. Las clases base `Command` / `Query` y los handlers están listos.

---

## 12. Próximos pasos (FASE 2 — Persona B)

Cuando B arranque, lo que ya tiene listo de A para usar:

1. **`MessageSplitter`** genera SubTasks con el formato correcto para Kafka
2. **`OutboxPublisher`** está listo para publicar (B solo necesita conectar el `KafkaPublisher` real de C)
3. **`SagaOrchestrator`** está listo para coordinar workers (B define los `SagaStep` concretos)
4. **`ResultAggregator`** está listo para que `ConsolidationWorker` espere resultados
5. **Modelos** `AnalysisResult`, `Incident`, `Evidence` listos para que workers escriban
6. **Bus CQRS** listo para que workers registren sus propios commands (ej: `RecordResultCommand`)

Lo que B necesita implementar:
- `workers/base_worker.py` — clase base que consume de Kafka
- `workers/text_worker.py`, `image_worker.py`, `audio_worker.py` — workers concretos
- `workers/consolidation_worker.py` — usa el Aggregator para generar reportes
- `workers/worker_factory.py` — Factory pattern para crear workers
- `patterns/strategy.py`, `decorators.py`, `bulkhead.py`, `object_pool.py` — patrones que ayudan

---

## 13. Referencia rápida

| Endpoint | Método | Auth | Qué hace |
|----------|--------|------|----------|
| `/` | GET | No | Info básica |
| `/health` | GET | No | Liveness |
| `/health/ready` | GET | No | Readiness con checks |
| `/auth/dev-token` | POST | No | Emite JWT (solo dev) |
| `/auth/me` | GET | Sí | Devuelve claims del token |
| `/upload/case` | POST | No (todavía) | Multipart upload + crea caso |
| `/graphql` | POST | No (todavía) | Schema + GraphiQL UI |
| `/docs` | GET | No | Swagger UI REST |

| Tabla | Filas tras verificación E2E |
|-------|------------------------------|
| `analysis_cases` | 2 |
| `outbox` | 13 eventos pendientes |
| `analysis_results` | 0 (pendiente B) |
| `incidents` | 0 (pendiente B) |

---

**Autor:** Dylan (Persona A)
**Última actualización:** 2026-06-06
**Total de commits en `develop`:** 24
