# Guía de Despliegue

Mantenido por **Persona C**. Cubre Docker Compose (local/prod) y Kubernetes.

## 1. Imágenes Docker

| Imagen | Dockerfile | Contenido |
|--------|------------|-----------|
| `analysis/api` | `docker/Dockerfile.api` | API FastAPI (slim, sin ML) |
| `analysis/worker-text` | `docker/Dockerfile.worker-text` | spaCy / HuggingFace |
| `analysis/worker-image` | `docker/Dockerfile.worker-image` | OpenCV / YOLO |
| `analysis/worker-audio` | `docker/Dockerfile.worker-audio` | Whisper |
| `analysis/worker` | `docker/Dockerfile.worker` | genérico (todas las libs) |

Todas son multi-stage (builder + runtime), corren como usuario no-root y
dividen dependencias (`docker/requirements-*.txt`) para imágenes pequeñas.

```bash
docker build -f docker/Dockerfile.api -t analysis/api:latest .
docker build -f docker/Dockerfile.worker-text -t analysis/worker-text:latest .
```

## 2. Docker Compose

### Desarrollo
```bash
docker-compose up -d
```
Levanta API + 3 workers + PostgreSQL + Redis + Kafka + Zookeeper + Prometheus +
Grafana. `KAFKA_ENABLED=true` ya está seteado para la app en el compose.

### Producción local
```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```
Diferencias: PostgreSQL **1 Primary + 2 Réplicas** (bitnami, streaming
replication), réplicas de API/workers, límites de recursos, sin bind-mounts.

Variables sensibles vía entorno (no commitear):
`POSTGRES_PASSWORD`, `POSTGRES_REPLICATION_PASSWORD`, `SECRET_KEY`.

## 3. Replicación PostgreSQL

`src/database/replication_config.sql` documenta y automatiza el lado SQL
(rol `replicator`, replication slots, parámetros WAL). El compose de prod usa
imágenes bitnami que configuran la replicación por variables de entorno.

Verificar en el Primary:
```sql
SELECT application_name, state, sync_state FROM pg_stat_replication;
```

El routing CQRS de `src/database/connection.py` usa `read_session` →
réplicas (`POSTGRES_REPLICA1_HOST`, `POSTGRES_REPLICA2_HOST`) y `write_session`
→ Primary automáticamente.

## 4. Kubernetes

```bash
# 1. Namespace + config + secrets
kubectl apply -f kubernetes/configmap.yaml
kubectl apply -f kubernetes/secrets.yaml      # editar secretos antes de prod

# 2. Infra con estado
kubectl apply -f kubernetes/postgres-service.yaml
kubectl apply -f kubernetes/postgres-statefulset.yaml
kubectl apply -f kubernetes/redis-deployment.yaml
kubectl apply -f kubernetes/kafka-deployment.yaml

# 3. App
kubectl apply -f kubernetes/api-deployment.yaml
kubectl apply -f kubernetes/worker-text-deployment.yaml
kubectl apply -f kubernetes/worker-image-deployment.yaml
kubectl apply -f kubernetes/worker-audio-deployment.yaml

# 4. Autoescalado
kubectl apply -f kubernetes/worker-text-hpa.yaml
kubectl apply -f kubernetes/worker-image-hpa.yaml
kubectl apply -f kubernetes/worker-audio-hpa.yaml

# 5. Observabilidad + Ingress
kubectl apply -f kubernetes/prometheus-deployment.yaml
kubectl apply -f kubernetes/grafana-deployment.yaml
kubectl apply -f kubernetes/ingress.yaml

# o todo de una:
kubectl apply -f kubernetes/
```

Notas:
- Las imágenes (`analysis/*:latest`) deben estar en un registry accesible por
  el clúster (ajustar `image:` o usar GHCR vía el workflow CD).
- Los **HPA** escalan los workers por CPU (70%, 2→10 réplicas).
- Las **réplicas de lectura** de PostgreSQL en K8s se recomiendan con un
  operador (CloudNativePG / bitnami chart); el StatefulSet incluido despliega
  el Primary.
- El **Ingress** requiere un controller nginx instalado.

## 5. CI/CD (GitHub Actions)

- `.github/workflows/ci.yml` — en cada push/PR: lint (ruff+black), unit tests
  con cobertura, y build de la imagen de API.
- `.github/workflows/cd.yml` — en push a `main` / tags `v*`: build & push de las
  4 imágenes a GHCR; deploy a K8s manual (`workflow_dispatch`, requiere
  `secrets.KUBECONFIG`).

## 6. Observabilidad

- Cada componente expone `/metrics` (API en :8000, workers en :8001).
- Prometheus scrapea según `src/monitoring/prometheus_config.yml` (o el
  ConfigMap en K8s) y evalúa `alerts.yml`.
- Grafana auto-provisiona el datasource Prometheus y 4 dashboards
  (`grafana_dashboards/*.json`). Los dashboards de DB y Kafka requieren
  `postgres_exporter` y `kafka_exporter` respectivamente.
