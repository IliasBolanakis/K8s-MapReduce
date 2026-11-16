# MapReduce Orchestrator (Manager Service)

The **MapReduce Orchestrator** (`mapreduce-manager/`) is the central control plane for the Kubernetes-native MapReduce framework. It handles multi-tenant job scheduling, O(1) memory/time data splitting, isolated RabbitMQ task distribution, atomic Redis barrier synchronization and capacity swaps, fenced execution attempts, event-driven Kubernetes Batch Job orchestration, and autonomous fault recovery.

---

## 🏗 Architecture Overview

The Manager operates as an **Event-Driven, Active-Active Service** backed by four infrastructure components:

1. **PostgreSQL (`asyncpg`):** ACID source of truth for multi-tenant job history, format metadata, and recovery state (`init-db.sql` / `app/k8s/infrastructure/postgres-configmap.yaml`).
2. **Redis (`redis.asyncio`):** Idempotency locks, atomic global capacity semaphore (`cluster:global_capacity_semaphore`), waiting-Reducer reservation watermark (`cluster:reduce_waiting`, `job:<id>:reduce_waiting_slots`), Watchdog/Reconciler leader locks, monotonic attempt fencing (`job:<id>:attempts`), Lua-scripted Map/Reduce completion barriers, and O(K)/O(M/K) shuffle routing sets (`job:<id>:ess_nodes`, `job:<id>:node_tasks:<ep>`).
3. **RabbitMQ (`aio-pika`):** Isolated consumer channels (`job_events_queue` prefetch `100`, `orchestration_events_queue` prefetch `RABBITMQ_ORCHESTRATION_PREFETCH`), a bounded publisher/RPC channel pool (`acquire_rabbitmq_channel`, `RABBITMQ_CHANNEL_POOL_SIZE=8`) with automatic closed-channel replacement, dynamic lazy queues (`map_tasks_<jobId>`, `reduce_tasks_<jobId>_<pId>`), and a bound `FANOUT` Dead Letter Exchange (`dead_letter_exchange` -> `dead_letter_queue`).
4. **Kubernetes API (`kubernetes_asyncio`):** Lifecycle-managed pooled `ApiClient` on `RealKubernetesProvider` provisioning `worker-template.yaml` Batch V1 Jobs (`mapreduce-worker:v10`), async `watch.Watch()` job completion monitoring (with `410 Gone` relist recovery), single-call batched Reconciler pod checks, and phase-scoped teardown (`delete_job(job_id, phase=..., num_reducers=...)`).

---

## ✨ Core Execution & Resilience

### 1. O(1) Memory & Time Data Partitioning (ADR-05)

* Uses Python generators (`orchestrator.generate_chunk_splits`) to stream 128 MiB byte-range boundaries sequentially to RabbitMQ lazy queues without materializing split arrays in Manager memory.
* Surgical lineage recovery (`TaskPublisher.republish_single_map_task`) uses direct O(1) byte-offset arithmetic via `orchestrator.get_chunk_split(chunk_index, file_size)` rather than generator scanning.

### 2. Indexed Reduce Jobs & Per-Partition Queues (ADR-04)

* **Normal Reduce Phase:** Declares and publishes each partition `p_id` (`0..num_reducers-1`) to its deterministic lazy queue `reduce_tasks_<job_id>_<p_id>` **before** spawning **1** Kubernetes `BatchV1` `Indexed` Job (`completionMode: Indexed`, `completions: num_reducers`, `parallelism: min(num_reducers, MAX_TOTAL_WORKERS)`, `backoffLimitPerIndex: 3`, `restartPolicy: Never`, `terminationGracePeriodSeconds: 40`).
* **Targeted Single-Partition Recovery:** Spawns a single non-indexed Job (`backoffLimit: 4`, `restartPolicy: OnFailure`, `PARTITION_ID` env var injected) consuming from `reduce_tasks_<job_id>_<p_id}` with soft `nodeAffinity` locality.
* **Queue Cleanup:** `delete_job` cleans up `map_tasks_<job_id>`, legacy `reduce_tasks_<job_id>`, and all per-partition `reduce_tasks_<job_id>_<p_id>` queues safely.

### 3. Atomic Map-to-Reduce Capacity Swap & `reduce_waiting` Watermark (ADR-05)

* `CacheRepository.swap_map_to_reduce_capacity` executes a single atomic Lua script that releases a job's Map slots and either immediately claims `num_reducers` slots OR registers `num_reducers` in `cluster:reduce_waiting` (tracked per job in `job:<id>:reduce_waiting_slots`) without holding partial active slots.
* `CacheRepository.reserve_cluster_capacity` bounds incoming Map capacity by `max_capacity - active_workers - reduce_waiting`, preventing new Map jobs from starving waiting Reducers. `release_job_capacity` / `forcefully_reclaim_job` atomically clear both active slots and any waiting watermark on job failure or cancellation.

### 4. Monotonic Attempt Fencing & Staged Reduce Promotion (ADR-06)

* `TaskPublisher` (Map & recovery) and `TaskEventProcessor._trigger_reduce_phase` (Reduce) mint monotonic `attemptId` values via pipelined `HINCRBY job:<id>:attempts <task_id> 1` (24h TTL).
* `TaskEventProcessor.process_message` validates `WorkerSignal.attempt_id` against Redis and drops stale zombie signals with a structured warning (`job_id`, `task_id`, `attempt_id`, `expected_attempt_id`).
* For fenced Reduce completions (`attemptId > 0`), `StorageService.promote_staged_reduce_output` copies `results/<user_id>/<job_id>/_attempts/part_<p>_attempt_<a>.staging` to `results/<user_id>/<job_id>/result_part_<p>.txt` and deletes the staging object before advancing the Reduce barrier. Any leftover `_attempts/` objects are purged on final completion.

### 5. Pooled K8s `ApiClient`, Async Watch & Batched Reconciler (ADR-11)

* `RealKubernetesProvider` reuses one shared `ApiClient` across `spawn_workers`, `wait_for_job_completion`, `delete_job`, and Reconciler pod queries, closed cleanly in FastAPI `lifespan` via `teardown_k8s()`.
* `wait_for_job_completion` uses `kubernetes_asyncio.watch.Watch()` on `list_namespaced_job` (capturing `resource_version` and recovering from `410 Gone` by relisting).
* `reconciler.py` executes a single `list_namespaced_pod(label_selector="app=worker-node")` query per cycle grouped by `labels["job_id"]`.

### 6. O(K) / O(M/K) Redis Shuffle Directory & AMQP Isolation (ADR-12, ADR-13)

* `CacheRepository` stores ESS bind addresses in `SET job:<id>:ess_nodes` (`O(K)`) and maps normalized `IP:Port` endpoints to Map tasks in `SET job:<id>:node_tasks:<norm_ep>` (`O(M/K)`), purged on job teardown.
* `app/core/rabbitmq.py` isolates `job_events_queue` and `orchestration_events_queue` onto dedicated channels and serves publisher/RPC traffic from a bounded `asyncio.Queue` pool (`RABBITMQ_CHANNEL_POOL_SIZE=8`) with transparent replacement of closed channels.

---

## 📊 Structured Logging (MDC ContextVars)

`app/core/logger.py` emits JSON logs to `stdout` via `JobContextFilter`, automatically injecting:

* `asctime`, `levelname`, `name`, `message`
* `job_id` (default `"system"`)
* `task_id` (default `"-"`)
* `attempt_id` (default `0`)

---

## 🤖 Autonomous Background Services

* **The Watchdog (`app/services/watchdog.py`):** Periodically sweeps stale `RUNNING` jobs exceeding `GLOBAL_JOB_TIMEOUT` under a Redis leader lock and invokes `forcefully_reclaim_job`.
* **The Reconciler (`app/services/reconciler.py`):** Performs one batched pod list query per cycle, compares against `cluster:global_capacity_semaphore`, and reclaims phantom allocations with zero active pods past the 60s grace window.

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

Apply infrastructure, RBAC, Service (`NodePort 30000`), Deployment (`replicas: 2`), and ESS DaemonSet (`ESS_MAX_LOCAL_STREAMS=64`, `ESS_MAX_S3_CONCURRENCY=32`, `terminationGracePeriodSeconds=40`) from `mapreduce-manager/`:

```powershell
kubectl apply -f app/k8s/infrastructure/postgres-configmap.yaml
kubectl apply -f app/k8s/infrastructure/postgres.yaml
kubectl apply -f app/k8s/infrastructure/redis.yaml
kubectl apply -f app/k8s/infrastructure/minio.yaml
kubectl apply -f app/k8s/infrastructure/rabbitmq.yaml

kubectl apply -f app/k8s/manager/manager-rbac.yaml
kubectl apply -f app/k8s/manager/manager-service.yaml
kubectl apply -f app/k8s/manager/manager-deployment.yaml
kubectl apply -f app/k8s/templates/ess-daemonset.yaml
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
| `RABBITMQ_CHANNEL_POOL_SIZE` | Bounded AMQP publisher/RPC channel pool size (4–8 recommended) | `8` |
| `RABBITMQ_ORCHESTRATION_PREFETCH` | QoS prefetch limit for isolated `orchestration_events_queue` channel | `10` |
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

> **Worker Pod Sizing (`app/k8s/templates/worker-template.yaml`):** Sets `JAVA_OPTS="-Xms128m -Xmx256m -XX:+UseParallelGC -XX:MaxDirectMemorySize=128m -XX:+TieredCompilation -XX:TieredStopAtLevel=1"` inside `2Gi` memory limits with `terminationGracePeriodSeconds: 40` for fast C1 cold-boot without C2 JIT storms and ADR-14 graceful drain headroom.

---

## 🧪 Testing

```bash
# Unit tests
python -m pytest tests/unit -q

# Integration tests (Testcontainers PostgreSQL, RabbitMQ, and Redis Lua)
python -m pytest tests/integration -q

# Live Minikube E2E tests (requires running Minikube cluster with deployed services)
python -m pytest tests/e2e -v
```

`tests/e2e/` includes `test_e2e_cluster.py`, Java fixtures under `tests/e2e/fixtures/`, and aligned PowerShell orchestration scripts (`start_infra_and_seed.ps1`, `reset_and_run_default.ps1`, `reset_and_run_large_file.ps1`, `reset_and_run_multitenant.ps1`, `deep_purge.ps1`).

---

*Project for the 2026 Distributed Systems class, Technical University of Crete. Built by Ilias Bolanakis.*
