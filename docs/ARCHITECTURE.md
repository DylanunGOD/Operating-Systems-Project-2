# Arquitectura

Sistema de análisis multiproceso y distribuido de datos de mensajería.
Documento de integración de las 3 capas (A: entrada, B: procesamiento,
C: plataforma).

## Visión general

```
   Cliente
     │ GraphQL / REST (multipart)
     ▼
┌─────────────┐   write (outbox)    ┌──────────────┐
│  API        │────────────────────▶│ PostgreSQL   │
│  (FastAPI)  │                     │ Primary + 2  │
│  CQRS/Saga  │◀──── read (réplicas)│ Réplicas     │
└─────┬───────┘                     └──────────────┘
      │ OutboxPublisher (poller)
      ▼ publish
┌─────────────┐   analysis.<tipo>.tasks   ┌──────────────────────┐
│   Kafka     │──────────────────────────▶│ Workers (text/image/ │
│  (broker)   │                           │ audio) — Bulkhead,    │
│             │◀── analysis.results ───────│ Pool, Strategy, Deco  │
└─────────────┘                           └──────────┬───────────┘
      ▲                                              │ ResultStore (outbox)
      │ analysis.results                             ▼
┌─────────────────────┐                       ┌──────────────┐
│ ConsolidationWorker │──── Incidents/Evidence▶│ PostgreSQL   │
│ (Aggregator)        │                        └──────────────┘
└─────────────────────┘
       Redis: cache-aside, locks, pub/sub   ·   Prometheus/Grafana: métricas
```

## Flujo de un caso

1. **API (A)** recibe el caso, lo valida (Builder), lo persiste y —en la misma
   transacción— escribe eventos en la tabla `outbox` (Outbox Pattern). El
   Splitter divide el caso en N sub-tareas (text/image/audio).
2. **OutboxPublisher (A, cableado por C)** corre en el lifespan de la API y
   drena la outbox hacia Kafka usando `KafkaManager` (C) — o el
   `ConsoleKafkaPublisher` en modo dev.
3. **Workers (B)** consumen sus topics vía `KafkaWorkerConsumer` (C),
   analizan dentro de un Bulkhead con un pipeline de decoradores (logging,
   métricas, retry, cache), y persisten el resultado. Al guardar, emiten —vía
   outbox— un evento `analysis.<tipo>.completed` al topic `analysis.results`.
4. **ConsolidationWorker (B)** espera todos los resultados (Aggregator, con
   timeout), genera `Incident` + `Evidence` y cierra el caso.

## Contratos entre capas

| Productor | Contrato | Consumidor |
|-----------|----------|------------|
| A | `KafkaPublisher` Protocol (`async publish(topic, payload)`) | C (`KafkaManager`) |
| B | `MessageConsumer` Protocol (`poll`/`commit`/`close`) | C (`KafkaWorkerConsumer`) |
| B | `Cache` Protocol (`get`/`set`) | C (`RedisCache`) |
| B | `MetricsRecorder` Protocol (`increment`/`observe`) | C (`PrometheusMetrics`) |
| A | Formato JSON de `SubTask` por topic | B (`WorkerTask.from_payload`) |

Estos Protocols permiten que cada capa se desarrolle con *fakes* y que C
sustituya por las implementaciones reales sin que A/B cambien código.

## Capa de plataforma (Persona C)

### Mensajería — `infrastructure/kafka.py`
- `KafkaManager`: productor idempotente (`acks=all`, `enable.idempotence`),
  publica con confirmación de entrega; llamadas bloqueantes en un hilo.
- `KafkaWorkerConsumer`: consumidor con commit manual (at-least-once).
- `build_consumer(topics)`: fábrica con flag `KAFKA_ENABLED` + sondeo de
  conectividad; si no hay broker lanza `KafkaUnavailableError` y el
  `WorkerFactory` de B cae al `InMemoryConsumer`.

### Cache — `infrastructure/redis_client.py`
- `RedisCache`: cache-aside (`get_or_set`), lock distribuido (SET NX EX +
  liberación atómica por token Lua), pub/sub.
- `InMemoryRedisCache`: fallback sin Redis.

### Datos — `database/`
- `DatabaseConnection`: pool por engine (Object Pool), routing CQRS
  (write→Primary, read→Réplicas round-robin).
- `Repository`: acceso a datos centralizado.
- `replication_config.sql`: streaming replication (1 Primary + 2 Réplicas).

### Observabilidad — `monitoring/`
- `PrometheusMetrics` (implementa `MetricsRecorder` de B) → Counters/Histograms.
- `worker_runner.py`: arranca el worker de B con servidor de métricas e
  inyecta `PrometheusMetrics`.
- Prometheus + Alertas + 4 dashboards Grafana (Workers, SLO, DB, Kafka).

## Patrones de diseño (resumen)

| Patrón | Dónde | Quién |
|--------|-------|-------|
| CQRS, Saga, Outbox, Builder, Splitter, Router, Aggregator | `patterns/`, `api/` | A |
| Object Pool, Bulkhead, Strategy, Decorator, Factory | `patterns/`, `workers/` | B |
| Object Pool (conexiones), Singleton, Repository | `database/`, `infrastructure/` | C |

## Tolerancia a fallos

- **Outbox** → cero pérdida de eventos aunque Kafka esté caído (reintentos).
- **Bulkhead** → un tipo de worker saturado no tumba a los demás.
- **Aggregator timeout** → un worker colgado no bloquea el caso para siempre.
- **Réplicas + HPA** → escalado horizontal de lectura y de workers.
- **Degradación elegante** → sin Kafka/Redis/ML, el sistema corre con fakes/heurísticas.
