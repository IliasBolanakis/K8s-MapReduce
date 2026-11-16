# MapReduce Orchestrator (Manager Service)

The **MapReduce Orchestrator** (`mapreduce-manager/`) is the central control plane for the Kubernetes-native MapReduce framework. It handles multi-tenant job scheduling, O(1) memory data splitting, RabbitMQ task distribution, Redis barrier synchronization, Kubernetes Batch Job provisioning, and autonomous fault recovery.

---

## 🏗 Architecture Overview

The Manager operates as an **Event-Driven, Active-Active Service** backed by four infrastructure components:

1. **PostgreSQL (`asyncpg`):** ACID source of truth for multi-tenant job history, format metadata, and recovery state (`init-db.sql` / `app/k8s/infrastructure/postgres-configmap.yaml`).
2. **Redis (`redis.asyncio`):** Idempotency locks, global worker capacity semaphores, Watchdog/Reconciler leader locks, Lua-scripted Map/Reduce completion barriers, and `job:<id>:worker_endpoints` shuffle routing tables.
3. **RabbitMQ (`aio-pika`):** Dynamic lazy queues (`map_tasks_<jobId>`, `reduce_tasks_<jobId>`), `job_events_queue`, `orchestration_events_queue`, and a bound `FANOUT` Dead Letter Exchange (`dead_letter_exchange` -> `dead_letter_queue`).
4. **Kubernetes API (`kubernetes_asyncio`):** Provisions `worker-template.yaml` Batch V1 Jobs (`mapreduce-worker:v10`) and cleans up phase-scoped jobs (`delete_job(job_id, phase=...)`).

---

## ✨ Core Execution & Resilience

### 1. O(1) Memory Footprint Data Partitioning

Uses Python generators (`orchestrator.generate_chunk_splits`) to stream 128 MiB byte-range boundaries sequentially to RabbitMQ lazy queues without materializing giant split arrays in Manager memory.

### 2. Multi-Tenant Sandboxing & Format Propagation

Tracks tenant ownership via `user_id`, derives Java class names from uploaded `.class` filenames, isolates output keys to `s3://results/<user_id>/<job_id>/`, and propagates `TaskMessage.format` (`JSON`, `AVRO`, `XML`, `TEXT`, `CSV`) end-to-end across initial scheduling, lineage recovery, and Reduce tasks.

### 3. Mid-Stream Local Reduction (Combiners)

Persists `combiner_filename` in PostgreSQL (`init-db.sql` and `postgres-configmap.yaml`) and propagates optional `combiner_code_object` / `combiner_class_name` in both initial Map dispatches and `_recover_lost_map_task` lineage recoveries.

### 4. Stateful Barriers & Data Locality

* **Lua-Backed Barriers:** Atomic Redis Lua scripts track unique completed task IDs (`map_completed_set`, `reduce_completed_set`) and trigger phase transitions exactly once.
* **Locality Scheduling:** Splits `nodeName@@IP:Port` addresses reported by Map workers (`CacheRepository.add_worker_endpoint`), pins Reducer pods via soft Kubernetes `nodeAffinity` (`TARGET_NODE`), and supplies clean `IP:Port` ESS endpoints in `TaskMessage.worker_endpoints`.
* **Phase-Scoped Teardown & Saga Rollback:** `process_orchestration_event` cleans up only Map-phase pods/queues under an idempotency lock (preserving `reduce_tasks_<jobId>`) and uses delayed backoff on cluster capacity starvation. `routes.py` cleans up `SUBMITTED` DB rows and reserved Redis capacity on `503`/`404` or Saga errors.

### 5. Hybrid Retry, Fail-Fast & Reactive Lineage Recovery

* **Manager-Owned Retries & Fail-Fast:** Deterministic user-code errors (`NullPointerException`, `ClassCastException`, `OutOfMemoryError`, `Exception in User Code`) immediately trigger `forcefully_reclaim_job`; transient worker `FAILED` signals (`strikes < 3`) ACK the event signal, provision a replacement pod, and republish the failed Map or Reduce task.
* **Node-Aware Lineage Recovery:** Handles both `SHUFFLE_FETCH_FAILED:<taskId>` and `SHUFFLE_FETCH_FAILED_FROM_NODE:<ip:port>` by looking up affected Map tasks in `job:{job_id}:worker_endpoints` (`CacheRepository.get_tasks_by_endpoint`), waitlisting the stalled Reducer, and surgically re-running only the lost Map chunks.

---

## 🤖 Autonomous Background Services

* **The Watchdog (`app/services/watchdog.py`):** Periodically sweeps stale `RUNNING` jobs exceeding `GLOBAL_JOB_TIMEOUT` and invokes `forcefully_reclaim_job`.
* **The Reconciler (`app/services/reconciler.py`):** Compares Redis capacity reservations against live Kubernetes pods and reclaims phantom allocations with zero active pods past the grace window.

---

## 🔬 Architectural Trade-offs & Future Scalability

1. **Redis CP vs. AP Guarantees:** Redis is single-replica without persistence in the baseline manifest; HA production deployments should add AOF/PVC or Sentinel plus fenced leader leases.
2. **Kubernetes API Polling vs. Informers:** `wait_for_job_completion` and the `Reconciler` poll the Kubernetes API; large-scale clusters should migrate to Kubernetes SharedInformers and `Indexed` Reduce Jobs.
3. **Task Batching vs. Maximum Parallelism:** `get_optimal_worker_count` defaults to `MAP_TASKS_PER_POD = 5` to amortize pod boot overhead across chunks.

---

## 🛠 Technology Stack

* **Framework:** FastAPI (Asynchronous ASGI / Uvicorn)
* **Language:** Python 3.11+ (tested on 3.13)
* **Database:** PostgreSQL (`asyncpg`)
* **Message Broker:** RabbitMQ (`aio-pika`)
* **Cache & Barriers:** Redis (`redis.asyncio`)
* **Infrastructure:** Kubernetes (`kubernetes_asyncio`), MinIO (`minio`)

---

## 🚀 Getting Started

### Prerequisites

* **Python 3.11+**
* **Minikube / Kubernetes** (`minikube start --driver=docker --memory=30720 --cpus=6 --nodes=1`)
* **Docker**

### Kubernetes Deployment (Minikube)

Build the Manager image inside Minikube's Docker daemon (`mapreduce-manager:v1`):

```powershell
& minikube -p minikube docker-env --shell powershell | Invoke-Expression
docker build -t mapreduce-manager:v1 .
```

Apply infrastructure, RBAC, Service (`NodePort 30000`), Deployment (`replicas: 2`), and ESS DaemonSet from `mapreduce-manager/`:

```powershell
kubectl apply -f app/k8s/infrastructure/postgres-configmap.yaml
kubectl apply -f app/k8s/infrastructure/postgres.yaml
kubectl apply -f app/k8s/infrastructure/redis.yaml
kubectl apply -f app/k8s/infrastructure/minio.yaml
kubectl apply -f app/k8s/infrastructure/rabbitmq.yaml

# Fallback if Docker Hub rejects minio/minio:latest pull:
# kubectl set image deployment/minio minio=cgr.dev/chainguard/minio:latest

kubectl apply -f app/k8s/manager/manager-rbac.yaml
kubectl apply -f app/k8s/manager/manager-service.yaml
kubectl apply -f app/k8s/manager/manager-deployment.yaml
kubectl apply -f app/k8s/templates/ess-daemonset.yaml

# Optional for unauthenticated local E2E testing:
# kubectl set env deployment/manager-api ENABLE_AUTH=false
```

### Local Development Setup (Docker Compose + Uvicorn)

1. Start backing services from the sibling node directory (`../mapreduce-node/Docker-compose.yml`):

```bash
docker compose -f ../mapreduce-node/Docker-compose.yml up -d
docker cp init-db.sql local-postgres:/tmp/init-db.sql
docker exec local-postgres psql -U postgres -d dds_db -f /tmp/init-db.sql
```

2. Create/activate `.venv` and install dependencies:

```bash
cd mapreduce-manager
python -m venv .venv
source .venv/bin/activate  # Windows: .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

3. Start the API:

```bash
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload
```

* **API Documentation (Swagger UI):** `http://127.0.0.1:8000/docs`

---

## ⚙️ Configuration (Environment Variables)

Configured via environment variables (see `app/core/config.py`):

| Variable | Description | Default |
| --- | --- | --- |
| `DATABASE_URL` | PostgreSQL connection string | `postgresql://postgres:DisSami2026!@localhost:5432/dds_db` |
| `REDIS_URL` | Redis connection string | `redis://localhost:6379/0` |
| `RABBITMQ_URL` | RabbitMQ connection string | `amqp://guest:guest@localhost:5672/` |
| `MINIO_ENDPOINT` | MinIO S3 endpoint (`host:port`) | `localhost:9000` |
| `MINIO_ACCESS_KEY` | MinIO access key | `minioadmin` |
| `MINIO_SECRET_KEY` | MinIO secret key | `minioadmin` |
| `MINIO_SECURE` | Enable TLS for MinIO client | `False` |
| `K8S_NAMESPACE` | Kubernetes namespace for Workers | `default` |
| `MAX_TOTAL_WORKERS` | Max concurrent pods allowed globally | `50` |
| `CHUNK_SIZE_BYTES` | Default byte split for Map tasks | `134217728` (128MB) |
| `MAP_TO_REDUCE_RATIO` | Default Map-to-Reduce partition ratio | `3` |
| `MAP_TASKS_PER_POD` | Task batching size per Map pod | `5` |
| `GLOBAL_JOB_TIMEOUT` | Seconds before Watchdog reclaims stalled jobs | `3600` |
| `ENABLE_AUTH` | Enforce RS256 Bearer JWT auth (`true` in `manager-deployment.yaml`) | `False` |
| `ESS_SECRET_KEY` | Shared HMAC-SHA256 secret for ESS tokens | `dev-insecure-shared-secret` |
| `MOCK_K8S` | Run with mock Kubernetes provider | `False` |

> **Worker Pod Sizing (`app/k8s/templates/worker-template.yaml`):** Sets `JAVA_OPTS="-Xms128m -Xmx256m -XX:+UseParallelGC -XX:MaxDirectMemorySize=128m"` inside `2Gi` memory limits so the parent worker and child `SandboxRunner` (`-XX:MaxRAMPercentage=50.0`) fit within the pod cgroup without `OOMKilled` (exit 137).

---

## 🧪 Testing

```bash
# Unit tests (48 tests)
pytest tests/unit -q

# Integration tests (Testcontainers PostgreSQL & RabbitMQ)
pytest tests/integration -q

# Live Minikube E2E tests (requires running Minikube cluster with deployed services)
pytest tests/e2e -v
```

`tests/e2e/` includes `test_e2e_cluster.py`, Java fixtures under `tests/e2e/fixtures/`, and aligned PowerShell orchestration scripts (`start_infra_and_seed.ps1`, `reset_and_run_default.ps1`, `reset_and_run_large_file.ps1`, `reset_and_run_multitenant.ps1`, `deep_purge.ps1`).

---

*Project for the 2026 Distributed Systems class, Technical University of Crete. Built by Ilias Bolanakis.*
