# Distributed MapReduce Execution Fabric (Worker Node)

This module (`mapreduce-node/`) contains the parallel execution engine (`mapreduce-worker`) and public developer API (`mapreduce-core`) for the Kubernetes-orchestrated MapReduce framework.

---

## 📂 Project Structure

* **`mapreduce-core`**: Public developer library exposing `Mapper`, `Reducer`, `Combiner`, `Context`, `KeyValuePair`, and `TaskPayload` (including `format` and `attemptId`).
* **`mapreduce-worker`**: Stateless worker daemon handling RabbitMQ task consumption, ephemeral child-JVM sandboxing, atomic attempt-fenced shuffle staging, bounded multi-pass External Merge Sort, bounded parallel gRPC shuffle fetches, and graceful `SIGTERM` draining.

---

## 🏗️ Core Architecture & Execution Engine

### 1. Dynamic Parallelism & Bounded S3 Range Ingestion (ADR-08)

* **`MapTaskProcessor`**: Uses Java's `ForkJoinPool` with container-aware `Runtime.availableProcessors()` discovery and a configurable `PARALLELISM_FACTOR` (default `2.0x`).
* **Clamped S3 Range Reads**: `S3ClientService.readDataChunk` bounds every `GetObjectArgs` request using `.length(clampedLength)` with a `1 MiB` (`1,048,576` bytes) lookahead clamped against cached `statObject` object sizes (preventing HTTP 416 errors). Bounded streams are fully drained prior to closing for OkHttp keep-alive socket reuse, while records straddling past `fetchLength + 1 MiB` trigger bounded continuation fetches from the exact unread byte offset (preserving `offset - 1` newline lookback and the `100 MiB` line limit guard).
* **Atomic Bytecode Staging**: `S3ClientService.downloadUserCode` downloads `.class` files to a sibling `.tmp` file and promotes via `ATOMIC_MOVE` to prevent cross-pod partial reads on shared `SHUFFLE_DIR`.

### 2. Ephemeral JVM Sandboxing, Heartbeats & Cgroup Budgeting (ADR-06)

* Task payloads run inside an isolated Child JVM (`SandboxRunner`) spawned with `-XX:+UseContainerSupport -XX:MaxRAMPercentage=50.0 -XX:MaxMetaspaceSize=64m -XX:+TieredCompilation -XX:CICompilerCount=2`. To prevent container cgroup oversubscription (`OOMKilled` exit 137) and C2 JIT cold-boot contention, the parent daemon `Dockerfile` defaults to `-XX:InitialRAMPercentage=10.0 -XX:MaxRAMPercentage=15.0 -XX:+TieredCompilation -XX:TieredStopAtLevel=1`.
* `TaskExecutor` emits `IN_PROGRESS` heartbeats every **`30` seconds** (carrying `attemptId`) to keep the Manager watchdog accurate.
* **Reduce Output Staging (`attemptId` Fencing)**: When `payload.attemptId() > 0`, `SandboxRunner` uploads reduced partitions to `<objectName>_attempts/part_<p>_attempt_<a>.staging` so `mapreduce-manager` can fence stale zombies and atomically promote only the winning attempt. Standalone/legacy `attemptId == 0` runs write directly to `<objectName>result_part_<p>.txt`.

### 3. Bounded `K=64` Multi-Pass External Merge Sort & 256KB LZ4 Framing (ADR-03 & ADR-09)

* **JVM Heap Accounting**: `ExternalMergeSorter` estimates in-memory `KeyValuePair` footprint at `96 + 2 * (key.length() + value.length())` bytes before spilling sorted runs to disk.
* **Bounded `K=64` Multi-Pass Merge**: Simultaneous open run readers are strictly capped at `MAX_MERGE_FAN_IN = 64`. When more than `64` runs are generated, iterative intermediate merge passes fold up to `64` runs at a time and eagerly delete consumed input runs (`Files.deleteIfExists`) until `<= 64` runs remain for the final streaming `performNWayMergeAndReduce` pass.
* **256KB LZ4 Blocks**: Both `ShufflePartitioner` and `ExternalMergeSorter` write `LZ4FrameOutputStream` with `BLOCKSIZE.SIZE_256KB` wrapped over a `64KB` `BufferedOutputStream` (reducing per-stream off-heap/buffer overhead vs `4MB` blocks).

### 4. Bounded Parallel gRPC Shuffle, Channel Reuse & Jittered Backoff (ADR-01)

* **Bounded Parallel Fetch Pool**: `TaskExecutor.executeReducePhase` fetches unique ESS endpoints concurrently via a bounded pool (`REDUCE_FETCH_CONCURRENCY`, default `8`, clamped `4..8`).
* **Channel Reuse**: Long-lived `ManagedChannel` instances are cached per `host:port` in a `ConcurrentHashMap`, recreated only if shut down/terminated, and closed cleanly on `TaskExecutor.close()`.
* **Full-Jitter Exponential Backoff**: Transient `Status.Code.RESOURCE_EXHAUSTED` and `Status.Code.UNAVAILABLE` responses are retried per endpoint up to `6` times (`sleepMs = random(0, min(10_000, 200 * 2^retryAttempt))`), deleting partial `.tmp` files before every retry.
* **Fail-Loud Sibling Cancellation**: Non-retryable statuses (`FAILED_PRECONDITION`, `UNAUTHENTICATED`, `INVALID_ARGUMENT`, etc.) or exhausted retries immediately cancel all sibling in-flight futures (`cancel(true)`), purge partial `.tmp` files, and emit `SHUFFLE_FETCH_FAILED_FROM_NODE:<endpoint>` (`FAILED`) with `attemptId` for lineage recovery.

### 5. Indexed Reduce Binding, Persistent AMQP & Map Fencing (ADR-04 & ADR-06)

* **Deterministic Partition Binding**: For `PHASE=reduce` with `JOB_ID`, `App` resolves `PARTITION_ID` first (targeted single-partition recovery), then `JOB_COMPLETION_INDEX` (Kubernetes `Indexed` Job), binding `RabbitMqConsumer` to `reduce_tasks_<jobId>_<idx>` in single-partition mode so the worker exits cleanly immediately after `basicAck` without waiting for `IDLE_TIMEOUT_MILLIS`.
* **Persistent Confirm-Enabled AMQP**: `RabbitMqConnectionManager` enables `automaticRecoveryEnabled` and `topologyRecoveryEnabled`. `RabbitMqProducer` maintains a synchronized persistent `Connection` and `Channel` with `confirmSelect()` + `waitForConfirmsOrDie(5_000)` and automatic broken-channel recovery.
* **Map Output Fencing**: `ShufflePartitioner` stages streams to `<mapTaskId>.attempt.<attemptId>.tmp`. On `close()`, it validates `<mapTaskId>.attempt`: if a newer attempt (`existing > attemptId`) already committed, the stale `.tmp` is discarded with a warning without touching `<mapTaskId>.lz4`; otherwise `.tmp` is atomically promoted to `<mapTaskId>.lz4` and `<mapTaskId>.attempt` is updated.

### 6. `SIGTERM` Graceful Drain & Structured MDC Logging (ADR-14)

* On `SIGTERM`, `App` immediately flips `/health` from `200 OK` to `503 UNAVAILABLE`, calls `RabbitMqConsumer.initiateGracefulDrain()` (`basicCancel`, waits up to `20s` for any active task to finish naturally; if still running, escalates `process.destroy()` -> `5s` -> `process.destroyForcibly()`, cleans `.tmp` files under `SHUFFLE_DIR`, and emits `basicNack(deliveryTag, false, true)` to requeue preempted work BEFORE closing the AMQP channel, `RabbitMqProducer`, and `TaskExecutor`).
* `logback.xml` emits structured JSON logs via `LogstashEncoder` including `MDC` keys `job_id`, `task_id`, `attempt_id`, and `phase`.

---

## 👨‍💻 Developer Guide: Writing a MapReduce Job

Users write jobs against `mapreduce-core` and compile `.class` files targeting Java 17.

### 1. Dependency (`pom.xml`)

```xml
<repository>
    <id>jitpack.io</id>
    <url>https://jitpack.io</url>
</repository>

<dependency>
    <groupId>com.github.IliasBolanakis.K8s-MapReduce</groupId>
    <artifactId>mapreduce-core</artifactId>
    <version>main-SNAPSHOT</version>
</dependency>
```

### 2. Implementing the Topology (WordCount Example)

**Mapper:**

```java
import com.iliasbolan.core.Mapper;
import com.iliasbolan.core.Context;

public class WordCountMapper implements Mapper {
    @Override
    public void map(String chunkOfText, Context context) {
        String[] words = chunkOfText.toLowerCase().replaceAll("[^a-z0-9]+", " ").split("\\s+");
        for (String word : words) {
            if (!word.isEmpty()) {
                context.write(word, "1");
            }
        }
    }
}
```

**Combiner (Optional):**

```java
import com.iliasbolan.core.Combiner;
import com.iliasbolan.core.KeyValuePair;
import java.util.Iterator;

public class WordCountCombiner implements Combiner {
    @Override
    public KeyValuePair combine(String key, Iterator<String> values) {
        long localTotal = 0;
        while (values.hasNext()) {
            localTotal += Long.parseLong(values.next());
        }
        return new KeyValuePair(key, String.valueOf(localTotal));
    }
}
```

**Reducer:**

```java
import com.iliasbolan.core.Reducer;
import com.iliasbolan.core.KeyValuePair;
import java.util.Iterator;

public class WordCountReducer implements Reducer {
    @Override
    public KeyValuePair reduce(String key, Iterator<String> values) {
        long globalTotal = 0;
        while (values.hasNext()) {
            globalTotal += Long.parseLong(values.next());
        }
        return new KeyValuePair(key, String.valueOf(globalTotal));
    }
}
```

### 3. Compilation & Submission

Compile targeting Java 17 (against `mapreduce-core` classes/JAR):

```bash
javac --release 17 -cp mapreduce-core/target/classes WordCountMapper.java WordCountCombiner.java WordCountReducer.java
```

Upload the compiled `.class` files to MinIO (`code/<user_id>/`) and submit the job payload to `mapreduce-manager`.

---

## ⚙️ Worker Configuration (Environment Variables)

| Variable | Description | Default |
| --- | --- | --- |
| `JOB_ID` / `PHASE` | Job UUID and execution phase (`map` or `reduce`) | *(unset)* / `map` |
| `PARTITION_ID` | Targeted single-partition override for Reduce recovery (`reduce_tasks_<jobId>_<idx>`) | *(unset)* |
| `JOB_COMPLETION_INDEX` | Kubernetes `Indexed` Job pod index for Reduce queue binding | *(unset)* |
| `RABBITMQ_HOST` | AMQP broker hostname | `localhost` |
| `RABBITMQ_USER` / `RABBITMQ_PASS` | AMQP credentials | `guest` / `guest` |
| `RABBITMQ_QUEUE` | Fallback task queue when `JOB_ID` is unset | `map_tasks_queue` |
| `EVENT_QUEUE` | Manager status/heartbeat event queue | `job_events_queue` |
| `MINIO_ENDPOINT` | S3-compatible object store URL | `http://localhost:9000` |
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | MinIO creds read by parent worker (also fallback for `SandboxRunner`) | `minioadmin` / `minioadmin` |
| `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` | Optional overrides read first by `SandboxRunner` (falls back to `MINIO_ROOT_*`) | *(falls back to `MINIO_ROOT_*`)* |
| `SHUFFLE_DIR` | Host path for ephemeral shuffle I/O & usercode staging | `/mnt/mapreduce-shuffle` |
| `REDUCE_FETCH_CONCURRENCY` | Bounded parallel gRPC fetch pool size (clamped `4..8`) | `8` |
| `GRPC_FETCH_DEADLINE_SECONDS` | Per-host gRPC `GetPartition` deadline in seconds | `300` |
| `IDLE_TIMEOUT_MILLIS` | Inactivity shutdown timeout in milliseconds (`10000` in K8s template) | `5000` |
| `NODE_IP` / `NODE_NAME` | Physical host IP & hostname for ESS routing | `127.0.0.1` / `unknown-node` |
| `PARALLELISM_FACTOR` | CPU scaling multiplier for `ForkJoinPool` | `2.0` |

---

## 🐳 Building & Testing

### Build Container Image

```bash
# Inside Minikube Docker env (or local Docker)
docker build -t mapreduce-worker:v10 .
```

### Run Unit & Integration Tests

```bash
# Unit tests (54 tests across App, core, engine, shuffle, and services)
mvn -f mapreduce-node/pom.xml test "-Dtest=!S3ClientServiceIntegrationTest" "-Dsurefire.failIfNoSpecifiedTests=false"

# Full test suite including S3ClientServiceIntegrationTest (requires MinIO on http://localhost:9000)
mvn -f mapreduce-node/pom.xml test
```

---

*Project for the 2026 Distributed Systems class, Technical University of Crete. Built by Ilias Bolanakis.*
