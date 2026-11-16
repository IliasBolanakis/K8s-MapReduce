"""
Repeatable, Deterministic Minikube Benchmark Workload (>=256 MiB, R=16).

Executes an identical ~270 MiB WordCount + Combiner workload against the live
Minikube MapReduce cluster via `kubectl port-forward`, polling `/internal/jobs/{id}/status`
at 0.25s intervals to capture accurate Map phase, Shuffle/Reduce phase, and
total wall-clock durations, and verifying K8s/Redis cleanup and exact results.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import shutil
import socket
import subprocess
import tempfile
import time
import uuid
from pathlib import Path
from typing import Dict

import asyncpg
import redis
import requests
from minio import Minio

DEFAULT_USER_ID = "22222222-2222-2222-2222-222222222222"
K8S_NAMESPACE = "default"
CHUNK_SIZE_BYTES = 128 * 1024 * 1024
TARGET_BYTES = 270 * 1024 * 1024  # >= 256 MiB
NUM_REDUCERS = 16
POLL_INTERVAL_SECONDS = 0.25
BENCHMARK_OBJECT_FILENAME = "e2e_benchmark_270mb.txt"

E2E_DIR = Path(__file__).resolve().parent
FIXTURES_DIR = E2E_DIR / "fixtures"
MANAGER_ROOT = E2E_DIR.parent.parent
MONOREPO_ROOT = MANAGER_ROOT.parent
CORE_CLASSES_DIR = MONOREPO_ROOT / "mapreduce-node" / "mapreduce-core" / "target" / "classes"


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
            time.sleep(0.25)
    raise TimeoutError(f"Endpoint {host}:{port} did not become reachable within {timeout}s")


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
        self._proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            _wait_for_tcp("127.0.0.1", self.local_port, timeout=30.0)
        except Exception:
            self.__exit__(None, None, None)
            raise
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._proc and self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                self._proc.wait(timeout=5)


def _ensure_cluster_ready() -> None:
    for label in ("app=postgres", "app=redis", "app=minio", "app=rabbitmq", "app=manager-api", "app=ess"):
        res = subprocess.run(
            ["kubectl", "wait", "--for=condition=ready", "pod", "-l", label, "-n", K8S_NAMESPACE, "--timeout=30s"],
            capture_output=True,
            text=True,
            check=False,
        )
        if res.returncode != 0:
            raise RuntimeError(f"Cluster pods for {label} are not ready: {res.stderr.strip()}")


def _compile_and_upload_classes(minio_client: Minio) -> None:
    if not CORE_CLASSES_DIR.exists():
        mvn_cmd = shutil.which("mvn") or "mvn"
        subprocess.run(
            [mvn_cmd, "-f", str(MONOREPO_ROOT / "mapreduce-node" / "pom.xml"), "-pl", "mapreduce-core", "compile"],
            check=True,
        )

    build_dir = Path(tempfile.mkdtemp(prefix="mr-bench-classes-"))
    try:
        java_files = sorted(str(p) for p in FIXTURES_DIR.glob("*.java"))
        subprocess.run(
            ["javac", "--release", "17", "-cp", str(CORE_CLASSES_DIR), "-d", str(build_dir), *java_files],
            check=True,
        )
        for bucket in ("data", "code", "results", "mapreduce-shuffle-overflow"):
            if not minio_client.bucket_exists(bucket):
                minio_client.make_bucket(bucket)
        for class_file in build_dir.glob("*.class"):
            minio_client.fput_object("code", f"{DEFAULT_USER_ID}/{class_file.name}", str(class_file))
    finally:
        shutil.rmtree(build_dir, ignore_errors=True)


def _build_deterministic_block() -> tuple[bytes, Dict[str, int]]:
    """
    Constructs a deterministic ~1 MiB block containing 10,000 distinct keys (`k00000`..`k09999`),
    10 keys per line, so WordCountCombiner still leaves meaningful multi-key shuffle payloads.
    """
    lines: list[bytes] = []
    counts: Dict[str, int] = {}
    for i in range(0, 10_000, 10):
        tokens = [f"k{j:05d}" for j in range(i, i + 10)]
        for tok in tokens:
            counts[tok] = counts.get(tok, 0) + 1
        lines.append((" ".join(tokens) + "\n").encode("ascii"))
    # Repeat 14x per block -> ~980 KiB per block
    unit = b"".join(lines)
    block = unit * 14
    block_counts = {k: v * 14 for k, v in counts.items()}
    return block, block_counts


def _ensure_benchmark_dataset(minio_client: Minio) -> tuple[int, Dict[str, int]]:
    block, block_counts = _build_deterministic_block()
    num_blocks = math.ceil(TARGET_BYTES / len(block))
    expected_size = num_blocks * len(block)
    expected_totals = {k: v * num_blocks for k, v in block_counts.items()}

    object_key = f"{DEFAULT_USER_ID}/{BENCHMARK_OBJECT_FILENAME}"
    needs_upload = True
    try:
        stat = minio_client.stat_object("data", object_key)
        if stat.size == expected_size:
            needs_upload = False
    except Exception:
        needs_upload = True

    if needs_upload:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as tmp:
            tmp_path = Path(tmp.name)
            for _ in range(num_blocks):
                tmp.write(block)
        try:
            minio_client.fput_object("data", object_key, str(tmp_path))
        finally:
            tmp_path.unlink(missing_ok=True)

    return expected_size, expected_totals


def _read_reduced_results(minio_client: Minio, job_id: str) -> Dict[str, int]:
    prefix = f"{DEFAULT_USER_ID}/{job_id}/"
    objects = list(minio_client.list_objects("results", prefix=prefix, recursive=True))
    if not objects:
        raise AssertionError(f"No output objects found in results/{prefix}")

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


def _wait_for_k8s_cleanup(job_id: str, timeout_seconds: float = 60.0) -> int:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        res = subprocess.run(
            ["kubectl", "get", "jobs", "-n", K8S_NAMESPACE, "-l", f"job_id={job_id}", "-o", "name"],
            capture_output=True,
            text=True,
            check=False,
        )
        lines = [ln.strip() for ln in res.stdout.splitlines() if ln.strip()]
        if not lines:
            return 0
        time.sleep(1.0)
    res = subprocess.run(
        ["kubectl", "get", "jobs", "-n", K8S_NAMESPACE, "-l", f"job_id={job_id}", "-o", "name"],
        capture_output=True,
        text=True,
        check=False,
    )
    return len([ln.strip() for ln in res.stdout.splitlines() if ln.strip()])


async def _verify_postgres(postgres_dsn: str, job_id: str, expected_chunks: int) -> None:
    conn = await asyncpg.connect(postgres_dsn)
    try:
        row = await conn.fetchrow(
            """
            SELECT status, total_chunks, completed_map_chunks, completed_reduce_chunks
            FROM public.jobs
            WHERE id = $1
            """,
            uuid.UUID(job_id),
        )
    finally:
        await conn.close()
    assert row is not None, f"Job {job_id} missing from Postgres"
    assert row["status"] == "COMPLETED", f"Unexpected Postgres status: {row['status']}"
    assert row["total_chunks"] == expected_chunks
    assert row["completed_map_chunks"] == expected_chunks
    assert row["completed_reduce_chunks"] == NUM_REDUCERS


def run_benchmark(label: str) -> Dict[str, object]:
    _ensure_cluster_ready()

    with (
        PortForward("svc/manager-service", 8000) as pf_mgr,
        PortForward("svc/minio-service", 9000) as pf_minio,
        PortForward("svc/postgres-service", 5432) as pf_pg,
        PortForward("svc/redis-service", 6379) as pf_redis,
    ):
        manager_url = f"http://127.0.0.1:{pf_mgr.local_port}"
        minio_client = Minio(
            f"127.0.0.1:{pf_minio.local_port}",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False,
        )
        redis_client = redis.Redis(host="127.0.0.1", port=pf_redis.local_port, decode_responses=True)
        postgres_dsn = f"postgresql://postgres:DisSami2026!@127.0.0.1:{pf_pg.local_port}/dds_db"

        _compile_and_upload_classes(minio_client)
        file_size, expected_totals = _ensure_benchmark_dataset(minio_client)
        expected_chunks = max(1, math.ceil(file_size / CHUNK_SIZE_BYTES))

        job_id = str(uuid.uuid4())
        payload = {
            "job_id": job_id,
            "format": "TEXT",
            "input_filename": BENCHMARK_OBJECT_FILENAME,
            "file_size": file_size,
            "mapper_filename": "WordCountMapper.class",
            "combiner_filename": "WordCountCombiner.class",
            "reducer_filename": "WordCountReducer.class",
            "num_reducers": NUM_REDUCERS,
        }

        t_submit = time.perf_counter()
        resp = requests.post(
            f"{manager_url}/internal/schedule",
            json=payload,
            headers={"Idempotency-Key": job_id},
            timeout=30,
        )
        if resp.status_code != 202:
            raise RuntimeError(f"Schedule failed ({resp.status_code}): {resp.text}")
        schedule_data = resp.json()

        t_map_done: float | None = None
        t_end: float | None = None
        last_state: Dict[str, object] = {}
        deadline = time.monotonic() + 900.0
        session = requests.Session()
        try:
            while time.monotonic() < deadline:
                try:
                    s_resp = session.get(f"{manager_url}/internal/jobs/{job_id}/status", timeout=(10, 60))
                    s_resp.raise_for_status()
                    last_state = s_resp.json()
                except requests.RequestException:
                    time.sleep(POLL_INTERVAL_SECONDS)
                    continue
                now = time.perf_counter()

                if t_map_done is None and int(last_state.get("completed_map_chunks", 0)) >= expected_chunks:
                    t_map_done = now

                status = last_state.get("status")
                if status == "COMPLETED":
                    if t_map_done is None:
                        t_map_done = now
                    t_end = now
                    break
                if status == "FAILED":
                    raise RuntimeError(f"Benchmark job {job_id} FAILED: {last_state}")

                time.sleep(POLL_INTERVAL_SECONDS)
        finally:
            session.close()

        if t_end is None or t_map_done is None:
            raise TimeoutError(f"Benchmark job {job_id} timed out (last={last_state})")

        remaining_jobs = _wait_for_k8s_cleanup(job_id)
        assert remaining_jobs == 0, f"Leaked K8s jobs for {job_id}: {remaining_jobs}"
        assert redis_client.hget("cluster:global_capacity_semaphore", job_id) is None
        assert redis_client.get(f"job:{job_id}:reduce_waiting_slots") is None
        active_workers = sum(int(v) for v in redis_client.hvals("cluster:global_capacity_semaphore"))
        reduce_waiting = int(redis_client.get("cluster:reduce_waiting") or 0)
        assert active_workers == 0, f"Leaked active_workers in Redis: {active_workers}"
        assert reduce_waiting == 0, f"Leaked reduce_waiting in Redis: {reduce_waiting}"

        asyncio.run(_verify_postgres(postgres_dsn, job_id, expected_chunks))
        actual_totals = _read_reduced_results(minio_client, job_id)
        assert actual_totals == expected_totals, "Reduced totals mismatch!"

        result = {
            "label": label,
            "job_id": job_id,
            "file_size_bytes": file_size,
            "file_size_mib": round(file_size / (1024 * 1024), 2),
            "map_chunks": expected_chunks,
            "num_reducers": NUM_REDUCERS,
            "spawned_map_workers": schedule_data.get("spawned_workers"),
            "map_phase_seconds": round(t_map_done - t_submit, 3),
            "shuffle_reduce_phase_seconds": round(t_end - t_map_done, 3),
            "total_seconds": round(t_end - t_submit, 3),
            "leaked_k8s_jobs": remaining_jobs,
            "distinct_keys_verified": len(actual_totals),
        }
        return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Run deterministic >=256 MiB MapReduce E2E benchmark")
    parser.add_argument("--label", default="baseline", help="Benchmark run label (e.g. T_before, T_after)")
    args = parser.parse_args()
    metrics = run_benchmark(args.label)
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
