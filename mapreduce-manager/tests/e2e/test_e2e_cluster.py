"""
Live Minikube Kubernetes Cluster End-to-End (E2E) Test Suite.

Validates the full distributed MapReduce pipeline against a live Kubernetes
cluster (Minikube) with deployed PostgreSQL, Redis, MinIO, RabbitMQ,
Active-Active Manager API replicas, and External Shuffle Service (ESS) DaemonSet:

1. Compiles real Java Mapper, Combiner, and Reducer classes against `mapreduce-core`.
2. Seeds MinIO buckets (`data`, `code`, `results`, `mapreduce-shuffle-overflow`).
3. Verifies single-job multi-chunk (>128 MiB) WordCount with Combiner:
   Map K8s Job -> ESS gRPC Shuffle -> Reduce K8s Jobs -> COMPLETED in Postgres,
   Redis capacity cleanup, K8s Job cleanup, and exact reduced totals in MinIO.
4. Verifies multi-tenant concurrent execution (TEXT + CSV jobs simultaneously),
   exact reduced totals per job, and cluster resource cleanup.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import tempfile
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, Generator, List

import asyncpg
import pika
import pytest
import redis
import requests
from minio import Minio

DEFAULT_USER_ID = "22222222-2222-2222-2222-222222222222"
K8S_NAMESPACE = os.environ.get("MR_K8S_NAMESPACE", "default")

E2E_DIR = Path(__file__).resolve().parent
FIXTURES_DIR = E2E_DIR / "fixtures"
MANAGER_ROOT = E2E_DIR.parent.parent
MONOREPO_ROOT = MANAGER_ROOT.parent
NODE_ROOT = MONOREPO_ROOT / "mapreduce-node"
CORE_CLASSES_DIR = NODE_ROOT / "mapreduce-core" / "target" / "classes"


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_tcp(host: str, port: int, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return
        except OSError:
            time.sleep(0.3)
    raise TimeoutError(f"Port-forwarded endpoint {host}:{port} did not become reachable within {timeout}s")


class PortForward:
    """Context manager for `kubectl port-forward` processes."""

    def __init__(self, target: str, remote_port: int, namespace: str = K8S_NAMESPACE) -> None:
        self.target = target
        self.remote_port = remote_port
        self.namespace = namespace
        self.local_port = _find_free_port()
        self._proc: subprocess.Popen[bytes] | None = None

    def __enter__(self) -> "PortForward":
        cmd = [
            "kubectl",
            "port-forward",
            "-n",
            self.namespace,
            self.target,
            f"{self.local_port}:{self.remote_port}",
        ]
        self._proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            _wait_for_tcp("127.0.0.1", self.local_port, timeout=30.0)
        except Exception:
            stderr = b""
            if self._proc and self._proc.poll() is not None and self._proc.stderr:
                stderr = self._proc.stderr.read()
            self.__exit__(None, None, None)
            raise RuntimeError(f"Failed to port-forward {self.target}:{self.remote_port}: {stderr.decode(errors='replace')}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._proc and self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                self._proc.wait(timeout=5)


def _kubectl_json_count(args: List[str]) -> int:
    res = subprocess.run(
        ["kubectl", *args, "-n", K8S_NAMESPACE, "-o", "name"],
        capture_output=True,
        text=True,
        check=False,
    )
    if res.returncode != 0:
        return 0
    lines = [line.strip() for line in res.stdout.splitlines() if line.strip()]
    return len(lines)


def _collect_job_diagnostics(job_id: str) -> str:
    pods = subprocess.run(
        ["kubectl", "get", "pods", "-n", K8S_NAMESPACE, "-l", f"job_id={job_id}", "-o", "wide"],
        capture_output=True,
        text=True,
        check=False,
    )
    mgr_logs = subprocess.run(
        ["kubectl", "logs", "-n", K8S_NAMESPACE, "-l", "app=manager-api", "--tail=30"],
        capture_output=True,
        text=True,
        check=False,
    )
    return f"Pods:\n{pods.stdout}\nManager logs:\n{mgr_logs.stdout}"


@pytest.fixture(scope="module")
def cluster_endpoints() -> Generator[Dict[str, object], None, None]:
    """Verifies live K8s cluster readiness and exposes port-forwarded service endpoints."""
    # Fail fast if cluster/deployments are not healthy
    for label in ("app=postgres", "app=redis", "app=minio", "app=rabbitmq", "app=manager-api", "app=ess"):
        res = subprocess.run(
            ["kubectl", "wait", "--for=condition=ready", "pod", "-l", label, "-n", K8S_NAMESPACE, "--timeout=30s"],
            capture_output=True,
            text=True,
            check=False,
        )
        if res.returncode != 0:
            pytest.skip(f"Live K8s cluster pods for {label} are not ready: {res.stderr.strip()}")

    with (
        PortForward("svc/manager-service", 8000) as pf_mgr,
        PortForward("svc/minio-service", 9000) as pf_minio,
        PortForward("svc/postgres-service", 5432) as pf_pg,
        PortForward("svc/redis-service", 6379) as pf_redis,
        PortForward("svc/rabbitmq-service", 5672) as pf_rmq,
    ):
        # Verify RabbitMQ AMQP handshake works over port-forward
        conn = pika.BlockingConnection(
            pika.ConnectionParameters(
                host="127.0.0.1",
                port=pf_rmq.local_port,
                credentials=pika.PlainCredentials("guest", "guest"),
            )
        )
        conn.close()

        minio_client = Minio(
            f"127.0.0.1:{pf_minio.local_port}",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False,
        )
        redis_client = redis.Redis(host="127.0.0.1", port=pf_redis.local_port, decode_responses=True)

        yield {
            "manager_url": f"http://127.0.0.1:{pf_mgr.local_port}",
            "minio": minio_client,
            "postgres_dsn": f"postgresql://postgres:DisSami2026!@127.0.0.1:{pf_pg.local_port}/dds_db",
            "redis": redis_client,
        }


@pytest.fixture(scope="module")
def compiled_java_classes(cluster_endpoints: Dict[str, object]) -> Generator[Path, None, None]:
    """Compiles real Java Mapper/Combiner/Reducer classes against mapreduce-core and seeds MinIO."""
    if not CORE_CLASSES_DIR.exists():
        mvn_cmd = shutil.which("mvn") or "mvn"
        subprocess.run(
            [mvn_cmd, "-f", str(NODE_ROOT / "pom.xml"), "-pl", "mapreduce-core", "compile"],
            check=True,
        )

    build_dir = Path(tempfile.mkdtemp(prefix="mr-e2e-classes-"))
    try:
        java_files = sorted(str(p) for p in FIXTURES_DIR.glob("*.java"))
        assert java_files, f"No Java fixture files found in {FIXTURES_DIR}"
        subprocess.run(
            ["javac", "--release", "17", "-cp", str(CORE_CLASSES_DIR), "-d", str(build_dir), *java_files],
            check=True,
        )

        minio_client: Minio = cluster_endpoints["minio"]  # type: ignore[assignment]
        for bucket in ("data", "code", "results", "mapreduce-shuffle-overflow"):
            if not minio_client.bucket_exists(bucket):
                minio_client.make_bucket(bucket)

        for class_file in build_dir.glob("*.class"):
            minio_client.fput_object("code", f"{DEFAULT_USER_ID}/{class_file.name}", str(class_file))

        yield build_dir
    finally:
        shutil.rmtree(build_dir, ignore_errors=True)


def _submit_job(manager_url: str, payload: Dict[str, object]) -> Dict[str, object]:
    job_id = str(payload["job_id"])
    resp = requests.post(
        f"{manager_url}/internal/schedule",
        json=payload,
        headers={"Idempotency-Key": job_id},
        timeout=30,
    )
    assert resp.status_code == 202, f"Schedule failed ({resp.status_code}): {resp.text}"
    return resp.json()


def _wait_for_job_completion(manager_url: str, job_id: str, timeout_seconds: float = 360.0) -> Dict[str, object]:
    deadline = time.monotonic() + timeout_seconds
    last_state: Dict[str, object] = {}
    while time.monotonic() < deadline:
        resp = requests.get(f"{manager_url}/internal/jobs/{job_id}/status", timeout=15)
        assert resp.status_code == 200, f"Status fetch failed ({resp.status_code}): {resp.text}"
        last_state = resp.json()
        status = last_state["status"]
        if status == "COMPLETED":
            return last_state
        if status == "FAILED":
            pytest.fail(f"Job {job_id} FAILED: {last_state}\n{_collect_job_diagnostics(job_id)}")
        time.sleep(2.0)

    pytest.fail(f"Job {job_id} timed out after {timeout_seconds}s (last={last_state})\n{_collect_job_diagnostics(job_id)}")


def _read_reduced_results(minio_client: Minio, user_id: str, job_id: str) -> Dict[str, int]:
    prefix = f"{user_id}/{job_id}/"
    objects = list(minio_client.list_objects("results", prefix=prefix, recursive=True))
    assert objects, f"No output objects found in results/{prefix}"

    totals: Dict[str, int] = {}
    for obj in objects:
        if not obj.object_name.endswith(".txt"):
            continue
        response = minio_client.get_object("results", obj.object_name)
        try:
            content = response.read().decode("utf-8")
        finally:
            response.close()
            response.release_conn()
        for line in content.splitlines():
            if not line.strip():
                continue
            key, val = line.split("\t", 1)
            totals[key] = totals.get(key, 0) + int(val)
    return totals


def _assert_redis_capacity_clean(redis_client: redis.Redis, job_ids: List[str]) -> None:
    """Verifies zero leaked per-job or cluster-wide Redis capacity permits."""
    for jid in job_ids:
        assert redis_client.hget("cluster:global_capacity_semaphore", jid) is None
        assert redis_client.get(f"job:{jid}:reduce_waiting_slots") is None
    active_workers = sum(int(v) for v in redis_client.hvals("cluster:global_capacity_semaphore"))
    reduce_waiting = int(redis_client.get("cluster:reduce_waiting") or 0)
    assert active_workers == 0, f"Leaked active_workers in Redis: {active_workers}"
    assert reduce_waiting == 0, f"Leaked reduce_waiting in Redis: {reduce_waiting}"


def _wait_for_k8s_cleanup(job_id: str, timeout_seconds: float = 60.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        job_count = _kubectl_json_count(["get", "jobs", "-l", f"job_id={job_id}"])
        if job_count == 0:
            return
        time.sleep(1.0)
    remaining = _kubectl_json_count(["get", "jobs", "-l", f"job_id={job_id}"])
    assert remaining == 0, f"Expected all K8s jobs for {job_id} to be deleted, found {remaining}"


@pytest.mark.asyncio
async def test_single_job_wordcount_with_combiner_multi_chunk(
    cluster_endpoints: Dict[str, object],
    compiled_java_classes: Path,
) -> None:
    """
    Executes a real multi-chunk (>128 MiB) WordCount job with WordCountCombiner.class:
    Map K8s Job -> ESS gRPC Shuffle -> Reduce K8s Jobs -> COMPLETED in Postgres,
    verifies K8s & Redis cleanup, and asserts exact key/value sums in MinIO results.
    """
    manager_url = str(cluster_endpoints["manager_url"])
    minio_client: Minio = cluster_endpoints["minio"]  # type: ignore[assignment]
    redis_client: redis.Redis = cluster_endpoints["redis"]  # type: ignore[assignment]
    postgres_dsn = str(cluster_endpoints["postgres_dsn"])

    # 3,650,000 * 37 bytes = 135,050,000 bytes (> 128 MiB -> 2 Map chunks)
    repetitions = 3_650_000
    batch_lines = 50_000
    line_bytes = b"alpha beta gamma alpha delta epsilon\n"
    object_name = f"{DEFAULT_USER_ID}/e2e_wordcount_multichunk.txt"

    with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as tmp:
        tmp_path = Path(tmp.name)
        block = line_bytes * batch_lines
        for _ in range(repetitions // batch_lines):
            tmp.write(block)
        file_size = tmp.tell()

    try:
        assert file_size > 128 * 1024 * 1024
        minio_client.fput_object("data", object_name, str(tmp_path))
    finally:
        tmp_path.unlink(missing_ok=True)

    job_id = str(uuid.uuid4())
    payload = {
        "job_id": job_id,
        "format": "TEXT",
        "input_filename": "e2e_wordcount_multichunk.txt",
        "file_size": file_size,
        "mapper_filename": "WordCountMapper.class",
        "combiner_filename": "WordCountCombiner.class",
        "reducer_filename": "WordCountReducer.class",
        "num_reducers": 2,
    }

    schedule_resp = _submit_job(manager_url, payload)
    assert schedule_resp["status"] == "SUCCESS"
    # Auto-scaling batches up to MAP_TASKS_PER_POD=5 map chunks per worker pod
    assert schedule_resp["spawned_workers"] == 1
    assert schedule_resp["num_reducers"] == 2

    final_status = _wait_for_job_completion(manager_url, job_id, timeout_seconds=420.0)
    assert final_status["completed_map_chunks"] == 2
    assert final_status["completed_reduce_chunks"] == 2

    conn = await asyncpg.connect(postgres_dsn)
    try:
        row = await conn.fetchrow(
            """
            SELECT status, format, combiner_filename, total_chunks,
                   completed_map_chunks, completed_reduce_chunks
            FROM public.jobs
            WHERE id = $1
            """,
            uuid.UUID(job_id),
        )
    finally:
        await conn.close()

    assert row is not None
    assert row["status"] == "COMPLETED"
    assert row["format"] == "TEXT"
    assert row["combiner_filename"] == "WordCountCombiner.class"
    assert row["total_chunks"] == 2
    assert row["completed_map_chunks"] == 2
    assert row["completed_reduce_chunks"] == 2

    _wait_for_k8s_cleanup(job_id)
    _assert_redis_capacity_clean(redis_client, [job_id])

    actual_totals = _read_reduced_results(minio_client, DEFAULT_USER_ID, job_id)
    expected_totals = {
        "alpha": 2 * repetitions,
        "beta": repetitions,
        "gamma": repetitions,
        "delta": repetitions,
        "epsilon": repetitions,
    }
    assert actual_totals == expected_totals


@pytest.mark.asyncio
async def test_multitenant_concurrent_text_and_csv_jobs(
    cluster_endpoints: Dict[str, object],
    compiled_java_classes: Path,
) -> None:
    """
    Submits 3 concurrent jobs across TEXT and CSV formats simultaneously,
    verifies all reach COMPLETED in Postgres, frees Redis + K8s resources,
    and asserts exact reduced key/value totals for every job in MinIO.
    """
    manager_url = str(cluster_endpoints["manager_url"])
    minio_client: Minio = cluster_endpoints["minio"]  # type: ignore[assignment]
    redis_client: redis.Redis = cluster_endpoints["redis"]  # type: ignore[assignment]
    postgres_dsn = str(cluster_endpoints["postgres_dsn"])

    text_reps = 20_000
    text_bytes = (b"k8s mapreduce ess k8s shuffle\n" * text_reps)
    csv_reps = 15_000
    csv_bytes = b"category,amount\n" + (b"books,10\ngames,25\nbooks,5\nmusic,12\n" * csv_reps)

    with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f_txt:
        f_txt.write(text_bytes)
        txt_path = Path(f_txt.name)
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as f_csv:
        f_csv.write(csv_bytes)
        csv_path = Path(f_csv.name)

    try:
        minio_client.fput_object("data", f"{DEFAULT_USER_ID}/e2e_mt_words.txt", str(txt_path))
        minio_client.fput_object("data", f"{DEFAULT_USER_ID}/e2e_mt_sales.csv", str(csv_path))
    finally:
        txt_path.unlink(missing_ok=True)
        csv_path.unlink(missing_ok=True)

    job1_id = str(uuid.uuid4())
    job2_id = str(uuid.uuid4())
    job3_id = str(uuid.uuid4())

    payloads = [
        {
            "job_id": job1_id,
            "format": "TEXT",
            "input_filename": "e2e_mt_words.txt",
            "file_size": len(text_bytes),
            "mapper_filename": "WordCountMapper.class",
            "combiner_filename": "WordCountCombiner.class",
            "reducer_filename": "WordCountReducer.class",
            "num_reducers": 2,
        },
        {
            "job_id": job2_id,
            "format": "CSV",
            "input_filename": "e2e_mt_sales.csv",
            "file_size": len(csv_bytes),
            "mapper_filename": "CsvSumMapper.class",
            "reducer_filename": "CsvSumReducer.class",
            "num_reducers": 2,
        },
        {
            "job_id": job3_id,
            "format": "TEXT",
            "input_filename": "e2e_mt_words.txt",
            "file_size": len(text_bytes),
            "mapper_filename": "WordCountMapper.class",
            "reducer_filename": "WordCountReducer.class",
            "num_mappers": 1,
            "num_reducers": 3,
        },
    ]

    with ThreadPoolExecutor(max_workers=3) as pool:
        responses = list(pool.map(lambda p: _submit_job(manager_url, p), payloads))
    for resp in responses:
        assert resp["status"] == "SUCCESS"

    for jid in (job1_id, job2_id, job3_id):
        _wait_for_job_completion(manager_url, jid, timeout_seconds=360.0)
        _wait_for_k8s_cleanup(jid)
        assert redis_client.hget("cluster:global_capacity_semaphore", jid) is None

    _assert_redis_capacity_clean(redis_client, [job1_id, job2_id, job3_id])

    conn = await asyncpg.connect(postgres_dsn)
    try:
        rows = await conn.fetch(
            "SELECT id::text, status, format FROM public.jobs WHERE id = ANY($1::uuid[])",
            [uuid.UUID(job1_id), uuid.UUID(job2_id), uuid.UUID(job3_id)],
        )
    finally:
        await conn.close()

    status_by_id = {r["id"]: (r["status"], r["format"]) for r in rows}
    assert status_by_id[job1_id] == ("COMPLETED", "TEXT")
    assert status_by_id[job2_id] == ("COMPLETED", "CSV")
    assert status_by_id[job3_id] == ("COMPLETED", "TEXT")

    expected_text = {
        "k8s": 2 * text_reps,
        "mapreduce": text_reps,
        "ess": text_reps,
        "shuffle": text_reps,
    }
    expected_csv = {
        "books": 15 * csv_reps,
        "games": 25 * csv_reps,
        "music": 12 * csv_reps,
    }

    assert _read_reduced_results(minio_client, DEFAULT_USER_ID, job1_id) == expected_text
    assert _read_reduced_results(minio_client, DEFAULT_USER_ID, job2_id) == expected_csv
    assert _read_reduced_results(minio_client, DEFAULT_USER_ID, job3_id) == expected_text
