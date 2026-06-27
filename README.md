# Kubernetes MapReduce Framework (K8s-MapReduce)

A cloud-native, high-throughput distributed MapReduce framework built for Kubernetes. 

This monorepo contains the complete infrastructure, control plane, and execution fabric required to schedule, isolate, and execute massive parallel data processing tasks across volatile compute clusters. It implements advanced distributed systems concepts including Active-Active state reconciliation, Ephemeral JVM Sandboxing, O(1) Memory External Merge Sorting, and Decentralized peer-to-peer (P2P) Shuffling.

---

## 🏗️ System Architecture & Monorepo Structure

The framework is strictly decoupled into three primary subsystems. Each directory contains its own detailed `README.md` for specific architectural and setup instructions.

### 1. [`/mapreduce-manager`](./mapreduce-manager/) (The Control Plane)
The central Orchestrator built in **Python (FastAPI)**. It operates in an Active-Active high-availability mode, backed by PostgreSQL, Redis, and RabbitMQ.
* **O(1) Data Partitioning:** Streams byte-range boundary generation to message queues without loading multi-terabyte datasets into Manager memory.
* **Autonomous Daemons:** Features a Redis-backed Distributed Leader Election system running a *Watchdog* (for garbage collection of stalled K8s jobs) and a *Reconciler* (for healing state drift between Redis and the K8s API).
* **Fail-Fast Protocol:** Surgically intercepts terminal deterministic exceptions (e.g., `UnsupportedClassVersionError`) and immediately reclaims cluster resources to prevent infinite retry deadlocks.

### 2. [`/mapreduce-node`](./mapreduce-node/) (The Execution Fabric)
The stateless compute worker built in **Java 17**. It fetches tasks from RabbitMQ and executes user bytecode.
* **Ephemeral JVM Sandboxing:** User code (`Mapper`, `Combiner`, `Reducer`) is dynamically loaded into an isolated Child JVM process. Once the task finishes, the OS obliterates the process, guaranteeing zero heap exhaustion or Metaspace leaks.
* **Mid-Stream Combiners:** Aggressively compresses data in RAM micro-batches before spilling to disk, mitigating network bottlenecks and downstream data skew.
* **O(1) External Merge Sort:** Reducers process multi-gigabyte shuffle partitions using K-Way external merge sorting, lazily evaluating data via Java Iterators to maintain a perfectly flat heap footprint.
* Includes the **`mapreduce-core`** library, which exposes the API contracts developers use to write custom MapReduce topologies.

### 3. [`/mapreduce-ess`](./mapreduce-ess/) (The Network Fabric)
The **External Shuffle Service** deployed as a Kubernetes `DaemonSet`. 
* **gRPC Peer-to-Peer:** Bypasses centralized storage bottlenecks by allowing Reducers to establish direct, high-speed gRPC streams to the specific physical nodes holding their required Map partitions.
* **Zero-Trust Security:** Secures all cross-node data transfers using cryptographic authorization tokens injected by the Orchestrator.

---

## 🌟 Core Flow

1. **Submission:** A client submits a Job payload (specifying S3 file targets and `.class` file names) to any active `mapreduce-manager` replica.
2. **Provisioning:** The Manager calculates byte boundaries and spins up Ephemeral `mapreduce-worker` Pods via the K8s Batch API.
3. **Map Phase:** Workers securely download user bytecode, isolate it in a Sandbox JVM, process data in chunks, apply Combiner compression, and spill intermediate data to the host node's ESS volume.
4. **Barrier Synchronization:** Atomic Redis Lua scripts track completion. Once the Map phase hits 100%, Reducers are triggered.
5. **Reduce Phase:** Reducers use K8s Node Affinity to schedule near the heaviest partitions, fetch remaining partitions via gRPC P2P streams, perform an External Merge Sort, and write final outputs to a tenant-isolated S3 bucket.

---

## 🚀 Quick Start (Local Development)

### Prerequisites
* **Python 3.11+**
* **Java 17** & **Maven**
* **Docker** & **Docker Compose**
* **Kubernetes** (Minikube or equivalent)

### 1. Bootstrapping Infrastructure
Start the backing infrastructure (PostgreSQL, Redis, RabbitMQ, MinIO) required by the Manager:
```bash
cd mapreduce-manager
docker-compose up -d

```

Initialize the Database schema:

```bash
docker cp init-db.sql local-postgres:/tmp/init-db.sql
docker exec local-postgres psql -U postgres -d dds_db -f /tmp/init-db.sql

```

### 2. Building & Loading Container Images

Compile the Java worker and ESS daemon, build their Docker images, and load them into your local Kubernetes cluster:

```bash
# Build Worker
cd ../mapreduce-node
docker build -t mapreduce-worker:v10 .

# Build ESS
cd ../mapreduce-ess
docker build -t mapreduce-ess:v2 .

# Load into Minikube
minikube image load mapreduce-worker:v10
minikube image load mapreduce-ess:v2

```

### 3. Deploying the Orchestrator

Start the Manager API locally:

```bash
cd ../mapreduce-manager
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Run the active-active API
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload

```

---

## 👨‍💻 Writing a MapReduce Application

Developers can write applications by pulling the `mapreduce-core` framework via JitPack and implementing the required generic interfaces:

```xml
<dependency>
    <groupId>com.github.IliasBolanakis.K8s-MapReduce</groupId>
    <artifactId>mapreduce-core</artifactId>
    <version>main-SNAPSHOT</version>
</dependency>

```

*(See the `mapreduce-node/README.md` for detailed coding examples).*

---

*Project developed for the 2026 Distributed Systems class, Technical University of Crete. Built by Ilias Bolanakis*
