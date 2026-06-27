# Distributed MapReduce Execution Fabric (Worker Node)

This repository contains the high-performance parallel execution engine and core API for a distributed MapReduce framework orchestrated on Kubernetes. Designed for cloud-native elasticity, this stateless compute worker dynamically processes Map and Reduce tasks using advanced memory-safe sorting algorithms, resilient AMQP messaging, and Zero-Trust peer-to-peer data transfers.

---

## 📂 Project Structure

* **`mapreduce-core`**: The public developer library containing the strictly typed `Mapper`, `Reducer`, `Combiner`, and `Context` contracts.
* **`mapreduce-worker`**: The ephemeral compute daemon that manages AMQP consumption, JVM sandboxing, dynamic bytecode localization, and distributed shuffles.

---

## 🏗️ Core Architecture & Execution Engine

### 1. Dynamic Parallelism (Container-Aware)

The execution engine abandons hardcoded thread counts. It utilizes Java's `ForkJoinPool` with dynamic CPU discovery, automatically detecting Kubernetes vCPU quotas and applying a configurable over-provisioning factor (`PARALLELISM_FACTOR`, default `2.0x`) to mask I/O latency. Work-stealing thresholds are calculated dynamically per-chunk to guarantee thread saturation.

### 2. Ephemeral JVM Sandboxing (Zero Memory Leaks)

To protect the primary Worker Daemon from heap exhaustion (OOM), Metaspace leaks, or malicious user code, all task payloads are executed within an isolated Child JVM process (`SandboxRunner`). Bytecode is dynamically fetched from the multi-tenant MinIO cluster and loaded via Reflection. When the computation completes, the OS physically obliterates the Child JVM, guaranteeing absolute memory reclamation.

### 3. Mid-Stream Local Reduction (Combiner)

Implements an aggressive memory-compression pipeline during the Map phase. If a user supplies a `Combiner`, the Worker intercepts the streaming Map output in transient RAM buffers (micro-batches) and mathematically folds the data before it touches the disk. This drastically reduces network bandwidth during the Shuffle phase and protects downstream Reducers from Zipfian Data Skews.

### 4. O(1) Memory External Merge Sort

Reducers are designed to process massive, multi-gigabyte partitions that far exceed container memory limits. The Worker utilizes a highly optimized K-Way External Merge Sort algorithm. Incoming P2P data streams are chunked, sorted on disk, and lazily evaluated using Java `Iterator` patterns, ensuring a perfectly flat heap memory footprint regardless of dataset scale.

### 5. Secure P2P gRPC Shuffle

Eliminates centralized storage bottlenecks during the Shuffle phase. Reducers establish direct, decentralized peer-to-peer gRPC streams with Map nodes to fetch their partitions.

* **Integrity:** Streams are written atomically to `.lz4` files to prevent corrupted frames in volatile cluster environments.
* **Zero-Trust:** All P2P requests require a cryptographic authorization token injected by the Orchestrator.

---

## 👨‍💻 Developer Guide: Writing a MapReduce Job

Users can write custom Distributed applications by importing the `mapreduce-core` library and implementing the required interfaces.

### 1. Dependency (`pom.xml`)

```xml
<repository>
    <id>jitpack.io</id>
    <url>https://jitpack.io</url>
</repository>

<dependency>
    <groupId>com.github.georgekarabaggelisjr.distributedsystemsproject</groupId>
    <artifactId>mapreduce-core</artifactId>
    <version>v3.0</version>
</dependency>

```

### 2. Implementing the Topology (Word Count Example)

**The Mapper:** Uses the Streaming `Context` pattern to emit data dynamically without retaining master lists in memory.

```java
import com.iliasbolan.core.Mapper;
import com.iliasbolan.core.Context;

public class WordCountMapper implements Mapper {
    @Override
    public void map(String chunkOfText, Context context) {
        String[] words = chunkOfText.toLowerCase().replaceAll("[^a-z0-9]+", " ").split("\\s+");
        for (String word : words) {
            if (word.length() > 1) { 
                context.write(word, "1"); 
            }
        }
    }
}

```

**The Combiner (Optional Optimization):** Executes purely on the Map node to compress network payloads. Implements the strict `Combiner` interface to ensure mathematical commutativity.

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

**The Reducer:** Executes on the Reduce node, processing the fully aggregated and shuffled data streams.

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

Compile your classes (targeting Java 17 compatibility for the Worker Daemon):

```bash
javac --release 17 -cp mapreduce-core-3.0.jar WordCountMapper.java WordCountCombiner.java WordCountReducer.java

```

Upload the compiled `.class` files to your MinIO sandbox, and submit the execution payload to the Manager API!

---

## ⚙️ Worker Configuration (Environment Variables)

The Worker Daemon is configured automatically by the Kubernetes Deployment manifests, but can be overridden via ENV vars:

| Variable | Description | Default |
| --- | --- | --- |
| `RABBITMQ_HOST` | AMQP Broker Hostname | `localhost` |
| `MINIO_ENDPOINT` | S3-Compatible Object Store URL | `http://localhost:9000` |
| `NODE_IP` | The physical Host IP for gRPC P2P Routing | *(Injected by K8s Downward API)* |
| `NODE_NAME` | The physical Hostname for Log tracing | *(Injected by K8s Downward API)* |
| `PARALLELISM_FACTOR` | CPU Scaling factor for Fork/Join Pool | `2.0` |
| `SHUFFLE_BASE_DIR` | Mount path for ephemeral fast-disk I/O | `/mnt/mapreduce-shuffle` |

---

*Project for the 2026 Distributed Systems class, Technical University of Crete. Built by Ilias Bolanakis.*