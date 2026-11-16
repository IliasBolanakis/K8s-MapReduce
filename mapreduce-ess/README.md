# External Shuffle Service (ESS)

The **External Shuffle Service (ESS)** (`mapreduce-ess/`) is the node-scoped data plane for the Kubernetes MapReduce framework. Deployed as a Kubernetes **DaemonSet** across compute nodes, it decouples intermediate Map shuffle storage from ephemeral Map worker pods and serves Reduce fetches over gRPC with MinIO overflow tiering.

---

## 🏗️ Core Architecture & Reliability Controls

* **Bounded Local Stream Admission (ADR-02):** `ShuffleServiceImpl` gates concurrent `GetPartition` streams with a fair `Semaphore` sized via `ESS_MAX_LOCAL_STREAMS` (default `64`) and timed acquisition via `ESS_STREAM_ACQUIRE_TIMEOUT_MS` (default `2000` ms). Saturation logs a backpressure warning and fails fast with gRPC `Status.RESOURCE_EXHAUSTED` so downstream Reducers apply jittered retry; permits are released deterministically in `finally` on completion, error, or client cancellation.
* **Pooled Direct `ByteBuffer` Streaming (ADR-10):** Implements server-streaming `GetPartition` reading finalized `.lz4` partition segments (ignoring `.tmp` and `.attempt` marker files) over Virtual Threads (`streamingPool`) with `ReentrantLock` + `Condition` HTTP/2 flow control. Each active stream leases **one** `65000`-byte Direct `ByteBuffer` from a bounded `ArrayBlockingQueue` pool across all segment files in the partition and returns it deterministically in `finally`.
* **Shared Bounded Global S3 Transfer Pool (ADR-02):** `MinioStorageService` routes both parallelized `spillJobData` `.lz4` uploads and `restorePartition` segment downloads through a single shared bounded `ExecutorService` sized via `ESS_MAX_S3_CONCURRENCY` (default `32`), capping total concurrent MinIO operations across the entire daemon and propagating partial spill failures as `IOException` so local data is never prematurely evicted.
* **Single-Flight Atomic MinIO Restore:** On cache misses (`!Files.exists(partitionDir)`), `ShuffleServiceImpl.resolveStorageTier` coordinates concurrent requests with a per-`(jobId, partitionId)` single-flight lock (`restoreLocks`). `MinioStorageService.restorePartition` downloads all S3 segments into a sibling `.tmp` staging directory and atomically promotes it (`ATOMIC_MOVE`, cleaning `.tmp` in `finally`).
* **Fail-Loud Degraded S3 State & Explicit Error Propagation (ADR-15):** When `!Files.exists(partitionDir)` and `minioStorageService == null` (degraded local-only mode), `ShuffleServiceImpl` fails loud with gRPC `Status.FAILED_PRECONDITION` (`"MinIO overflow storage is unavailable and partition directory is absent on local disk"`). Real S3 restore errors propagate as `Status.UNAVAILABLE`. Empty `OK` (`ByteString.EMPTY`) is returned strictly for verified empty partitions (existing local directory with zero finalized `.lz4` files, or S3 restore returning `0` segments).
* **Ordered `SIGTERM` Graceful Drain (ADR-14):** `ShuffleDaemon.shutdownSequence` drains subsystems in strict order: `server.shutdown()` &rarr; await gRPC server &amp; `ShuffleServiceImpl.shutdownAndAwait` (`<=20s`, escalating to `server.shutdownNow()` if stalled) &rarr; `StorageJanitor.stop()` (sets `running = false`, calls `gcExecutor.shutdown()`, and awaits active spill/GC cycle up to `15s` before `shutdownNow()`) &rarr; `MinioStorageService.close()`.
* **Structured SLF4J / MDC Observability:** Configured via `src/main/resources/logback.xml` to emit `[job_id=... partition_id=...]` correlation fields across `ShuffleServiceImpl`, `StorageJanitor`, and `MinioStorageService` worker threads, with zero `System.out` calls and zero `INFO` logs inside the 65KB chunk streaming loop.
* **Zero-Trust Token Verification:** Validates incoming `authorization` gRPC metadata tokens in constant time using `HMAC-SHA256(jobId)` against `ESS_SECRET_KEY`.

---

## 💾 Multi-Tier Storage & Lease-Safe Janitor

To prevent local node `hostPath` disk exhaustion without sacrificing durability, `StorageJanitor` integrates with `MinioStorageService`:

1. **High-Resolution Telemetry:** Polls the underlying NIO `FileStore` every 30 seconds.
2. **Dual-Watermark Reclamation:**
   * **High-Watermark (85%):** Spills oldest idle job directories to MinIO (`MINIO_BUCKET`) before deleting them locally.
   * **Low-Watermark (70%):** Halts reclamation once local usage returns below the safe buffer.
3. **Active Writer & Stream/Restore Lease Protection:** `StorageJanitor.reclaimSpace` skips any job directory that either contains `.tmp` staging artifacts **or** holds active restore/stream leases (`StorageJanitor.acquireJobLease` / `releaseJobLease` managed by `ShuffleServiceImpl`), re-checking under a `ReentrantReadWriteLock` before local deletion.
4. **Hard TTL Sweep:** Purges local job directories older than 24 hours (`HARD_TTL_HOURS`) when no active leases or `.tmp` files remain.

---

## 🔬 Architectural Trade-offs & Future Scalability

1. **Custom Janitor GC vs. K8s Local Persistent Volumes (LPV):** Uses a host `hostPath` volume managed by `StorageJanitor` rather than static LPVs to preserve elastic pod scheduling.
2. **Buffered Virtual-Thread gRPC vs. Netty `FileRegion`:** Uses Protobuf `bytes` chunks (`PartitionChunk`) over Virtual Threads for simplicity; ultra-high-throughput clusters can optimize further with reusable per-stream `ByteBuffer`s or a raw `FileRegion` transfer path.
3. **Node-Level Ephemerality:** ESS protects against *Pod* eviction; full physical *Node* loss relies on the Manager's Reactive Lineage Recovery (`SHUFFLE_FETCH_FAILED_FROM_NODE:<endpoint>`).

---

## ⚙️ Configuration (Environment Variables)

| Variable | Description | Code Default | `ess-daemonset.yaml` Value |
| --- | --- | --- | --- |
| `ESS_PORT` | gRPC listening port | `7337` | `7337` |
| `SHUFFLE_DIR` | Local host path for Map outputs | `/mnt/mapreduce-shuffle` | `/mnt/mapreduce-shuffle` |
| `ESS_SECRET_KEY` | Secret key for HMAC token verification | `dev-insecure-shared-secret` | `dev-insecure-shared-secret` |
| `ESS_MAX_LOCAL_STREAMS` | Maximum concurrent active `GetPartition` streams | `64` | `64` (default) |
| `ESS_STREAM_ACQUIRE_TIMEOUT_MS` | Admission wait timeout (ms) before returning `RESOURCE_EXHAUSTED` | `2000` | `2000` (default) |
| `ESS_MAX_S3_CONCURRENCY` | Global cap on concurrent MinIO uploads &amp; downloads | `32` | `32` (default) |
| `ESS_MAX_CONCURRENT_IO` | Concurrent S3 restore permit ceiling | `150` | `150` (default) |
| `ESS_LOG_LEVEL` | Root Logback log level (`INFO`, `DEBUG`, etc.) | `INFO` | `INFO` (default) |
| `MINIO_ENDPOINT` | MinIO S3 endpoint | `http://minio:9000` | `http://minio-service.default.svc.cluster.local:9000` |
| `MINIO_ACCESS_KEY` | S3 access key | `minioadmin` | `minioadmin` |
| `MINIO_SECRET_KEY` | S3 secret key | `minioadmin` | `minioadmin` |
| `MINIO_BUCKET` | Overflow bucket name (falls back to `MINIO_SHUFFLE_BUCKET`) | `ess-overflow` | `mapreduce-shuffle-overflow` |

---

## 🔍 Structured Logging Fields

`src/main/resources/logback.xml` emits structured SLF4J MDC fields on every log line:

* `job_id`: Sanitized MapReduce job identifier (populated in `ShuffleServiceImpl`, `StorageJanitor`, and `MinioStorageService` transfer threads).
* `partition_id`: Target partition index (populated in `ShuffleServiceImpl` and `MinioStorageService.restorePartition` threads).

---

## 🚀 Building, Testing & Containerization

### 1. Run Unit & Concurrency Tests

```bash
# Executes JUnit 5 suites (MinioStorageServiceTest, ShuffleServiceImplTest, StorageJanitorTest, ShuffleDaemonTest)
mvn -f mapreduce-ess/pom.xml test
```

### 2. Build the Fat JAR

```bash
mvn clean package -DskipTests
```

### 3. Build the Docker Image (Local / Minikube)

Uses `eclipse-temurin:21-jre-alpine`:

```bash
# Inside Minikube Docker env (or local Docker)
docker build -t mapreduce-ess:v2 .
```

---

## 📦 Kubernetes Deployment

Deployed as a `DaemonSet` via `mapreduce-manager/app/k8s/templates/ess-daemonset.yaml`:

* **Host Networking:** Binds directly to the node's physical IP (`hostNetwork: true`, `hostPort: 7337`).
* **Storage Mount:** Mounts `/var/lib/mapreduce/shuffle-data` (`hostPath`) at `/mnt/mapreduce-shuffle`.
* **Priority Class:** `system-cluster-critical`.

```bash
kubectl apply -f ../mapreduce-manager/app/k8s/templates/ess-daemonset.yaml
```

---

## 📜 API Definition (gRPC)

```proto
syntax = "proto3";
package shuffle;
option java_package = "com.iliasbolan.grpc.shuffle";

service ShuffleService {
  rpc GetPartition (PartitionRequest) returns (stream PartitionChunk);
}

message PartitionRequest {
  string job_id = 1;
  int32 partition_id = 2;
}

message PartitionChunk {
  bytes content = 1;
}
```

---

*Project for the 2026 Distributed Systems class, Technical University of Crete. Built by Ilias Bolanakis.*
