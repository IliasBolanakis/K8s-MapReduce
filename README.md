# Kubernetes MapReduce Framework (K8s-MapReduce)

A cloud-native, high-throughput distributed MapReduce framework built for Kubernetes.

This monorepo contains the complete infrastructure, control plane, execution engine, and shuffle data plane required to schedule, isolate, and execute parallel data processing workloads across Kubernetes clusters. It implements Active-Active state reconciliation, Ephemeral JVM Sandboxing, External Merge Sorting, and Decentralized peer-to-peer (P2P) gRPC Shuffling with MinIO overflow tiering.

---

## 🏗️ System Architecture & Monorepo Structure

The framework is decoupled into three primary subsystems. Each directory contains its own detailed `README.md`:

### 1. [`/mapreduce-manager`](./mapreduce-manager/) (The Control Plane)

The central Orchestrator built in **Python (FastAPI)**. It operates in Active-Active mode backed by PostgreSQL, Redis, and RabbitMQ.

* **O(1) Data Partitioning:** Streams byte-range split generation to RabbitMQ lazy queues without buffering datasets in Manager memory.
* **Stateful Barriers & Lineage Recovery:** Uses atomic Redis Lua barriers and `job:<id>:worker_endpoints` tracking to trigger phase transitions and surgically recover failed Map nodes (`SHUFFLE_FETCH_FAILED:<taskId>` and `SHUFFLE_FETCH_FAILED_FROM_NODE:<ip:port>`).
* **Autonomous Daemons:** Runs Redis-locked *Watchdog* (stalled job reclamation) and *Reconciler* (Redis vs. Kubernetes state drift cleanup) background loops.
* **Fail-Fast & DLQ Protection:** Intercepts deterministic user-code failures and routes rejected tasks through a bound `FANOUT` Dead Letter Exchange (`dead_letter_exchange` -> `dead_letter_queue`).

### 2. [`/mapreduce-node`](./mapreduce-node/) (The Execution Fabric)

The stateless compute worker built in **Java 17**. It consumes phase tasks (`basicQos(1)`) from RabbitMQ and spawns an isolated child JVM per task.

* **Ephemeral JVM Sandboxing:** User code (`Mapper`, `Combiner`, `Reducer`) executes inside an ephemeral `SandboxRunner` child JVM (`-XX:MaxRAMPercentage=50.0`), while the parent worker daemon stays capped (`-Xmx256m` in Kubernetes / `-XX:MaxRAMPercentage=15.0` in Docker) so both fit safely inside the `2Gi` container cgroup limit.
* **Mid-Stream Combiners & Atomic Shuffle Writes:** Optional `Combiner` classes fold records in RAM before writing intermediate `.tmp` files and atomically promoting (`ATOMIC_MOVE`) to `.lz4` on completion.
* **External Merge Sort & Bounded gRPC Shuffle:** Reducers fetch upstream `.lz4` partitions over gRPC with explicit RPC deadlines (`GRPC_FETCH_DEADLINE_SECONDS`, default `300s`) and run K-way merge-sort via lazy iterators.
* Includes **`mapreduce-core`**, exposing the `Mapper`, `Reducer`, `Combiner`, and `Context` developer APIs.

### 3. [`/mapreduce-ess`](./mapreduce-ess/) (The Shuffle Data Plane)

The **External Shuffle Service** deployed as a Kubernetes `DaemonSet` (`hostNetwork: true`, port `7337`).

* **gRPC Peer-to-Peer Streaming:** Streams pre-compressed `.lz4` segments from host NVMe/SSD (`SHUFFLE_DIR`) using Virtual Threads and 65KB NIO Direct Buffers with HTTP/2 flow control.
* **Single-Flight MinIO Overflow & Lease-Safe GC:** `StorageJanitor` spills idle job data to MinIO (`MINIO_BUCKET`, `mapreduce-shuffle-overflow` in K8s) above 85% disk usage while skipping active `.tmp` staging and active stream/restore leases; `MinioStorageService` restores missing partitions atomically under a per-`(jobId, partitionId)` single-flight lock and propagates S3 failures as `Status.UNAVAILABLE`.

---

## 🌟 Core Flow

1. **Submission:** A client submits a Job payload (`format`, S3 filenames, `.class` names, optional `combiner_filename`, `num_mappers`, `num_reducers`) to `mapreduce-manager`.
2. **Provisioning:** The Manager records the job in PostgreSQL, reserves capacity in Redis, declares job-scoped lazy RabbitMQ queues, and provisions Ephemeral `mapreduce-worker:v10` Map Pods via the Kubernetes Batch API.
3. **Map Phase:** Workers atomically stage user `.class` bytecode, run the `SandboxRunner` child JVM, apply optional Combiners, and atomically commit `<mapTaskId>.lz4` files to the host ESS volume (`/mnt/mapreduce-shuffle`).
4. **Barrier Synchronization:** Atomic Redis Lua scripts track Map completion and transition cleanly to Reduce (`delete_job(..., phase="map")` cleans Map pods/queues without deleting `reduce_tasks_<jobId>`).
5. **Reduce Phase:** Reducers use soft Kubernetes `nodeAffinity` toward ESS nodes, stream partitions via gRPC (`GetPartition`) with deadlines, external merge-sort, and write `result_part_<id>.txt` to `s3://results/<user_id>/<job_id>/`.

---

## 🚀 Quick Start (Local Minikube Cluster, >=30 GiB RAM)

### Prerequisites

* **Python 3.11+** (tested with 3.13)
* **Java 17+** & **Maven 3.9+**
* **Docker**, **Minikube**, and **kubectl**

### 1. Start Minikube (>=30 GiB RAM)

```powershell
minikube start --driver=docker --memory=30720 --cpus=6 --nodes=1
minikube status
```

### 2. Build Local Images Inside Minikube

Switch your shell to Minikube's Docker daemon and build all three container images (`mapreduce-ess:v2`, `mapreduce-worker:v10`, `mapreduce-manager:v1`):

```powershell
& minikube -p minikube docker-env --shell powershell | Invoke-Expression

mvn -f mapreduce-ess/pom.xml clean package -DskipTests
docker build -t mapreduce-ess:v2 mapreduce-ess
docker build -t mapreduce-worker:v10 mapreduce-node
docker build -t mapreduce-manager:v1 mapreduce-manager
```

### 3. Deploy Infrastructure, Manager & ESS to Kubernetes

```powershell
# 1. Backing infrastructure (PostgreSQL, Redis, MinIO, RabbitMQ)
kubectl apply -f mapreduce-manager/app/k8s/infrastructure/postgres-configmap.yaml
kubectl apply -f mapreduce-manager/app/k8s/infrastructure/postgres.yaml
kubectl apply -f mapreduce-manager/app/k8s/infrastructure/redis.yaml
kubectl apply -f mapreduce-manager/app/k8s/infrastructure/minio.yaml
kubectl apply -f mapreduce-manager/app/k8s/infrastructure/rabbitmq.yaml

# 2. Control Plane (RBAC, NodePort 30000 Service, 2-replica Deployment) & ESS DaemonSet
kubectl apply -f mapreduce-manager/app/k8s/manager/manager-rbac.yaml
kubectl apply -f mapreduce-manager/app/k8s/manager/manager-service.yaml
kubectl apply -f mapreduce-manager/app/k8s/manager/manager-deployment.yaml
kubectl apply -f mapreduce-manager/app/k8s/templates/ess-daemonset.yaml
```

> **MinIO Image Pull Caveat:** If `minio/minio:latest` fails with `pull access denied` / `ImagePullBackOff` due to Docker Hub repository restrictions, switch the MinIO deployment image to Chainguard's drop-in MinIO image:
> ```powershell
> kubectl set image deployment/minio minio=cgr.dev/chainguard/minio:latest
> ```

> **Local E2E Auth Override:** `manager-deployment.yaml` sets `ENABLE_AUTH="true"` by default. For unauthenticated local E2E testing without issuing RS256 Keycloak JWTs, override on the live deployment:
> ```powershell
> kubectl set env deployment/manager-api ENABLE_AUTH=false
> ```

Verify all pods are `1/1 Running`:

```powershell
kubectl get pods -o wide
```

---

## 🧪 Testing

### Manager Unit, Integration & Live Cluster E2E Tests

```powershell
cd mapreduce-manager
.\.venv\Scripts\Activate.ps1

# Unit tests (48 tests)
pytest tests/unit -q

# Integration tests (Testcontainers Postgres & RabbitMQ)
pytest tests/integration -q

# Live Minikube E2E tests (compiles Java WordCount/CSV fixtures, seeds MinIO, runs K8s jobs)
pytest tests/e2e -v
```

Aligned PowerShell E2E runner scripts are also available in `mapreduce-manager/tests/e2e/` (`start_infra_and_seed.ps1`, `reset_and_run_default.ps1`, `reset_and_run_large_file.ps1`, `reset_and_run_multitenant.ps1`, `deep_purge.ps1`).

### Java Unit Tests (`mapreduce-node` & `mapreduce-ess`)

```powershell
# Node Worker unit tests (exclude S3ClientServiceIntegrationTest when MinIO is not on localhost:9000)
mvn -f mapreduce-node/pom.xml test "-Dtest=!S3ClientServiceIntegrationTest" "-Dsurefire.failIfNoSpecifiedTests=false"

# External Shuffle Service unit & concurrency tests
mvn -f mapreduce-ess/pom.xml test
```

---

## 👨‍💻 Writing a MapReduce Application

Developers can build jobs against `mapreduce-core` by implementing `Mapper`, optional `Combiner`, and `Reducer` interfaces:

```xml
<dependency>
    <groupId>com.github.IliasBolanakis.K8s-MapReduce</groupId>
    <artifactId>mapreduce-core</artifactId>
    <version>main-SNAPSHOT</version>
</dependency>
```

*(See [`mapreduce-node/README.md`](./mapreduce-node/README.md) for `Mapper`, `Combiner`, and `Reducer` code examples and Java 17 compilation commands).*

---

*Project developed for the 2026 Distributed Systems class, Technical University of Crete. Built by Ilias Bolanakis*
