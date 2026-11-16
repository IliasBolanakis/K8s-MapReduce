# Distributed MapReduce Execution Fabric (Worker Node)

This module (`mapreduce-node/`) contains the parallel execution engine (`mapreduce-worker`) and public developer API (`mapreduce-core`) for the Kubernetes-orchestrated MapReduce framework.

---

## 📂 Project Structure

* **`mapreduce-core`**: Public developer library exposing `Mapper`, `Reducer`, `Combiner`, `Context`, `KeyValuePair`, and `TaskPayload` (including `format`).
* **`mapreduce-worker`**: Stateless worker daemon handling RabbitMQ task consumption, ephemeral child-JVM sandboxing, atomic bytecode/shuffle staging, External Merge Sort, and gRPC shuffle fetches.

---

## 🏗️ Core Architecture & Execution Engine

### 1. Dynamic Parallelism (Container-Aware)

`MapTaskProcessor` uses Java's `ForkJoinPool` with container-aware `Runtime.availableProcessors()` discovery and a configurable `PARALLELISM_FACTOR` (default `2.0x`), dynamically sizing work-stealing thresholds per chunk.

### 2. Ephemeral JVM Sandboxing & Cgroup Memory Budgeting

Task payloads run inside an isolated Child JVM (`SandboxRunner`) spawned with `-XX:+UseContainerSupport -XX:MaxRAMPercentage=50.0 -XX:MaxMetaspaceSize=64m`. To prevent `2Gi` container cgroup oversubscription (`OOMKilled` exit 137), the parent daemon `Dockerfile` defaults to `-XX:InitialRAMPercentage=10.0 -XX:MaxRAMPercentage=15.0` (and `worker-template.yaml` caps parent `JAVA_OPTS` at `-Xms128m -Xmx256m`).

`S3ClientService.downloadUserCode` downloads user `.class` files to a sibling `.tmp` file and promotes via `ATOMIC_MOVE` to prevent cross-pod partial reads on shared `SHUFFLE_DIR`.

### 3. Mid-Stream Local Reduction (Combiner)

When a `Combiner` class is configured on the job, Map tasks fold intermediate key/value micro-batches in memory prior to LZ4 compression and disk flush.

### 4. O(1) Memory External Merge Sort

`ExternalMergeSorter` spills sorted, LZ4-compressed intermediate runs (`run_*.lz4`) and performs K-way merge-sort via lazy `Iterator` streaming (`StreamGroupingIterator`) so user `Reducer` logic processes large partitions with bounded heap.

### 5. Secure Atomic P2P gRPC Shuffle & Manager-Owned Retries

* **Atomic Partition Writes:** `ShufflePartitioner` writes intermediate shuffle streams to `<mapTaskId>.tmp` inside `<SHUFFLE_DIR>/<jobId>/<partitionId>/`, flushes and closes streams on `close()`, and atomically promotes via `Files.move(..., ATOMIC_MOVE, REPLACE_EXISTING)` to `<mapTaskId>.lz4` (deleting `.tmp` files on error).
* **Bounded gRPC Fetches:** Reducers authenticate to ESS nodes (`authorization: <jobToken>`) and apply an explicit RPC deadline (`.withDeadlineAfter(...)`, configured via `GRPC_FETCH_DEADLINE_SECONDS`, default `300s`).
* **Manager-Owned Retries:** On gRPC fetch failures, `TaskExecutor` emits `SHUFFLE_FETCH_FAILED_FROM_NODE:<endpoint>` (`FAILED`) to `job_events_queue`. On task exceptions, `RabbitMqConsumer` emits `FAILED` and calls `basicNack(deliveryTag, false, false)` (without local AMQP republishing), letting `mapreduce-manager` own task retries and lineage recovery.

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
| `RABBITMQ_HOST` | AMQP broker hostname | `localhost` |
| `RABBITMQ_USER` / `RABBITMQ_PASS` | AMQP credentials | `guest` / `guest` |
| `RABBITMQ_QUEUE` | Fallback task queue when `JOB_ID` is unset | `map_tasks_queue` |
| `EVENT_QUEUE` | Manager status/heartbeat event queue | `job_events_queue` |
| `MINIO_ENDPOINT` | S3-compatible object store URL | `http://localhost:9000` |
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | MinIO creds read by parent worker (also fallback for `SandboxRunner`) | `minioadmin` / `minioadmin` |
| `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` | Optional overrides read first by `SandboxRunner` (falls back to `MINIO_ROOT_*`) | *(falls back to `MINIO_ROOT_*`)* |
| `SHUFFLE_DIR` | Host path for ephemeral shuffle I/O & usercode staging | `/mnt/mapreduce-shuffle` |
| `GRPC_FETCH_DEADLINE_SECONDS` | Per-host gRPC `GetPartition` deadline in seconds | `300` |
| `IDLE_TIMEOUT_MILLIS` | Inactivity shutdown timeout in milliseconds (`10000` in K8s template) | `5000` |
| `NODE_IP` / `NODE_NAME` | Physical host IP & hostname for ESS routing | *(Injected by K8s Downward API)* / `127.0.0.1` |
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
# Unit tests (31 tests across core, engine, shuffle, and services)
mvn test "-Dtest=!S3ClientServiceIntegrationTest" "-Dsurefire.failIfNoSpecifiedTests=false"

# Full test suite including S3ClientServiceIntegrationTest (requires MinIO on http://localhost:9000)
mvn test
```

---

*Project for the 2026 Distributed Systems class, Technical University of Crete. Built by Ilias Bolanakis.*
