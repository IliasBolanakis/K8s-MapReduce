"""
Unit tests for the Kubernetes infrastructure orchestration client.

Validates:
    - ADR-04: Indexed Reduce Batch Jobs (`completionMode: Indexed`,
      `backoffLimitPerIndex: 3`, no top-level `backoffLimit`, `restartPolicy: Never`,
      `terminationGracePeriodSeconds: 40`) vs Map / targeted single-partition recovery
      (`backoffLimit: 4`, `restartPolicy: OnFailure`, `PARTITION_ID`, `RABBITMQ_QUEUE`),
      and per-partition queue deletion (`reduce_tasks_{job_id}_{p_id}`).
    - ADR-11: Lifecycle-managed pooled `ApiClient`, async `Watch` completion
      monitoring with `410 Gone` relist handling, and single-call batched
      `get_active_pod_counts_by_job`.
"""

from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock

import pytest
from kubernetes_asyncio.client.exceptions import ApiException

from app.k8s import client as k8s_client_module
from app.k8s.client import (
    MockKubernetesProvider,
    RealKubernetesProvider,
    get_active_pod_counts_by_job,
    teardown_k8s,
)


@pytest.fixture
def mock_settings(mocker: Any) -> None:
    """Mocks global infrastructure settings for predictable test conditions."""
    mocker.patch("app.k8s.client.settings.MAX_TOTAL_WORKERS", 5)
    mocker.patch("app.k8s.client.settings.K8S_NAMESPACE", "test-namespace")
    mocker.patch("app.k8s.client.settings.GLOBAL_JOB_TIMEOUT", 60)


@pytest.fixture
def real_worker_template() -> str:
    """Loads the actual worker-template.yaml so Jinja2/K8s schema rules are verified directly."""
    template_path = Path(__file__).resolve().parents[3] / "app" / "k8s" / "templates" / "worker-template.yaml"
    return template_path.read_text(encoding="utf-8")


@pytest.fixture
def mock_k8s_api(mocker: Any) -> Dict[str, Any]:
    """Standardizes mocking of the pooled ApiClient, BatchV1Api, and CoreV1Api."""
    mock_api_client_cls = mocker.patch("app.k8s.client.client.ApiClient")
    api_instance = mock_api_client_cls.return_value
    api_instance.close = AsyncMock()
    api_instance.__aenter__ = AsyncMock(return_value=api_instance)
    api_instance.__aexit__ = AsyncMock(return_value=None)
    api_instance.deserialize = MagicMock(side_effect=lambda _model, data, _type: data)

    mock_batch_cls = mocker.patch("app.k8s.client.client.BatchV1Api")
    mock_create_job = AsyncMock()
    mock_batch_cls.return_value.create_namespaced_job = mock_create_job
    mock_list_jobs = AsyncMock()
    mock_batch_cls.return_value.list_namespaced_job = mock_list_jobs
    mock_delete_job = AsyncMock()
    mock_batch_cls.return_value.delete_namespaced_job = mock_delete_job

    mock_core_cls = mocker.patch("app.k8s.client.client.CoreV1Api")
    mock_list_pods = AsyncMock()
    mock_core_cls.return_value.list_namespaced_pod = mock_list_pods

    return {
        "api_client_cls": mock_api_client_cls,
        "api_instance": api_instance,
        "create_job": mock_create_job,
        "list_jobs": mock_list_jobs,
        "delete_job": mock_delete_job,
        "list_pods": mock_list_pods,
    }


@pytest.mark.asyncio
async def test_mock_provider_execution() -> None:
    """Verifies operational safety and API parity of MockKubernetesProvider."""
    provider = MockKubernetesProvider()
    await provider.spawn_workers("job-123", 10, phase="reduce", indexed=True)
    await provider.wait_for_job_completion("job-123")
    await provider.delete_job("job-123", num_reducers=4)


@pytest.mark.asyncio
async def test_spawn_workers_map_renders_real_template_and_reuses_api_client(
    mocker: Any,
    mock_settings: None,
    mock_k8s_api: Dict[str, Any],
    real_worker_template: str,
) -> None:
    """Validates Map Job manifest generation and pooled ApiClient reuse (ADR-04, ADR-11)."""
    mock_file = AsyncMock()
    mock_file.read.return_value = real_worker_template
    mock_open = mocker.patch("app.k8s.client.aiofiles.open")
    mock_open.return_value.__aenter__.return_value = mock_file

    provider = RealKubernetesProvider()
    await provider.spawn_workers(job_id="test-job-uuid", replicas=10, phase="map")
    await provider.spawn_workers(job_id="test-job-uuid-2", replicas=2, phase="map")

    # Pooled ApiClient initialized only once across multiple calls
    mock_k8s_api["api_client_cls"].assert_called_once()
    assert mock_k8s_api["create_job"].call_count == 2

    submitted_manifest = mock_k8s_api["create_job"].call_args_list[0].kwargs["body"]
    spec = submitted_manifest["spec"]
    pod_spec = spec["template"]["spec"]

    assert spec["parallelism"] == 5
    assert spec["completions"] == 5
    assert spec["backoffLimit"] == 4
    assert "backoffLimitPerIndex" not in spec
    assert "completionMode" not in spec
    assert pod_spec["restartPolicy"] == "OnFailure"
    assert pod_spec["terminationGracePeriodSeconds"] == 40

    env_vars = pod_spec["containers"][0]["env"]
    env_map = {e["name"]: e for e in env_vars}
    assert "-XX:+TieredCompilation" in env_map["JAVA_OPTS"]["value"]
    assert "-XX:TieredStopAtLevel=1" in env_map["JAVA_OPTS"]["value"]
    assert env_map["RABBITMQ_QUEUE"]["value"] == "map_tasks_test-job-uuid"
    assert "PARTITION_ID" not in env_map
    assert env_map["NODE_IP"]["valueFrom"]["fieldRef"]["fieldPath"] == "status.hostIP"
    assert env_map["NODE_NAME"]["valueFrom"]["fieldRef"]["fieldPath"] == "spec.nodeName"

    await provider.close()
    mock_k8s_api["api_instance"].close.assert_awaited_once()


@pytest.mark.asyncio
async def test_spawn_workers_indexed_reduce_k8s_spec_rules(
    mocker: Any,
    mock_settings: None,
    mock_k8s_api: Dict[str, Any],
    real_worker_template: str,
) -> None:
    """
    Verifies ADR-04 Indexed Reduce Job spec:
    completionMode=Indexed, completions=num_reducers, parallelism=min(num_reducers, MAX_TOTAL_WORKERS),
    backoffLimitPerIndex=3, NO top-level backoffLimit, restartPolicy=Never, terminationGracePeriodSeconds=40.
    """
    mock_file = AsyncMock()
    mock_file.read.return_value = real_worker_template
    mocker.patch("app.k8s.client.aiofiles.open").return_value.__aenter__.return_value = mock_file

    provider = RealKubernetesProvider()
    await provider.spawn_workers(job_id="indexed-job-1", replicas=8, phase="reduce", indexed=True)

    submitted_manifest = mock_k8s_api["create_job"].call_args.kwargs["body"]
    spec = submitted_manifest["spec"]
    pod_spec = spec["template"]["spec"]

    assert spec["completionMode"] == "Indexed"
    assert spec["completions"] == 8
    assert spec["parallelism"] == 5  # Capped by mock_settings MAX_TOTAL_WORKERS=5
    assert spec["backoffLimitPerIndex"] == 3
    assert "backoffLimit" not in spec
    assert pod_spec["restartPolicy"] == "Never"
    assert pod_spec["terminationGracePeriodSeconds"] == 40


@pytest.mark.asyncio
async def test_spawn_workers_targeted_recovery_injects_partition_id_and_locality(
    mocker: Any,
    mock_settings: None,
    mock_k8s_api: Dict[str, Any],
    real_worker_template: str,
) -> None:
    """Verifies non-indexed targeted recovery injects PARTITION_ID, per-partition queue, and nodeAffinity."""
    mock_file = AsyncMock()
    mock_file.read.return_value = real_worker_template
    mocker.patch("app.k8s.client.aiofiles.open").return_value.__aenter__.return_value = mock_file

    provider = RealKubernetesProvider()
    await provider.spawn_workers(
        job_id="locality-test",
        replicas=1,
        phase="reduce",
        partition_id=2,
        target_node="physical-node-01",
        indexed=False,
    )

    submitted_manifest = mock_k8s_api["create_job"].call_args.kwargs["body"]
    spec = submitted_manifest["spec"]
    pod_spec = spec["template"]["spec"]

    assert spec["backoffLimit"] == 4
    assert "backoffLimitPerIndex" not in spec
    assert pod_spec["restartPolicy"] == "OnFailure"

    pref_terms = pod_spec["affinity"]["nodeAffinity"]["preferredDuringSchedulingIgnoredDuringExecution"]
    assert pref_terms[0]["preference"]["matchExpressions"][0]["values"] == ["physical-node-01"]

    env_map = {e["name"]: e.get("value") for e in pod_spec["containers"][0]["env"]}
    assert env_map["PARTITION_ID"] == "2"
    assert env_map["RABBITMQ_QUEUE"] == "reduce_tasks_locality-test_2"


@pytest.mark.asyncio
async def test_wait_for_job_completion_watch_and_410_relist(
    mocker: Any,
    mock_settings: None,
    mock_k8s_api: Dict[str, Any],
) -> None:
    """
    Verifies ADR-11 async Watch completion monitoring:
    1. Initial list captures resource_version.
    2. Watch handles 410 Gone by resetting resource_version and relisting.
    3. Subsequent Watch MODIFIED event completes when succeeded >= completions.
    """
    job_running = MagicMock()
    job_running.metadata.name = "worker-reduce-1"
    job_running.metadata.resource_version = "100"
    job_running.spec.completions = 2
    job_running.status.succeeded = 1
    job_running.status.conditions = []

    job_done = MagicMock()
    job_done.metadata.name = "worker-reduce-1"
    job_done.metadata.resource_version = "105"
    job_done.spec.completions = 2
    job_done.status.succeeded = 2
    job_done.status.conditions = []

    list_resp_1 = MagicMock(items=[job_running])
    list_resp_1.metadata.resource_version = "100"
    list_resp_2 = MagicMock(items=[job_running])
    list_resp_2.metadata.resource_version = "101"
    mock_k8s_api["list_jobs"].side_effect = [list_resp_1, list_resp_2]

    class _FakeWatchStream:
        def __init__(self, events: List[Any]) -> None:
            self._events = list(events)
            self.stopped = False

        async def __aenter__(self) -> "_FakeWatchStream":
            return self

        async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            return None

        def stream(self, _func: Any, **kwargs: Any) -> "_FakeWatchStream":
            self.kwargs = kwargs
            return self

        def stop(self) -> None:
            self.stopped = True

        def __aiter__(self) -> "_FakeWatchStream":
            return self

        async def __anext__(self) -> Any:
            if self.stopped or not self._events:
                raise StopAsyncIteration
            nxt = self._events.pop(0)
            if isinstance(nxt, Exception):
                raise nxt
            return nxt

    w1 = _FakeWatchStream([ApiException(status=410, reason="Expired")])
    w2 = _FakeWatchStream([{"type": "MODIFIED", "object": job_done}])
    mocker.patch("app.k8s.client.watch.Watch", side_effect=[w1, w2])

    provider = RealKubernetesProvider()
    await provider.wait_for_job_completion("job-watch-410")

    assert mock_k8s_api["list_jobs"].call_count == 2
    assert w1.kwargs["resource_version"] == "100"
    assert w2.kwargs["resource_version"] == "101"
    assert w2.stopped is True


@pytest.mark.asyncio
async def test_delete_job_phase_scoped_map_preserves_reduce(
    mocker: Any,
    mock_settings: None,
    mock_k8s_api: Dict[str, Any],
) -> None:
    """Verifies delete_job(job_id, phase='map') deletes only map queue & map K8s Jobs."""
    job_id = "12345678-1111-2222-3333-444444444444"
    mq_mock = AsyncMock()
    cm_mock = AsyncMock()
    cm_mock.__aenter__.return_value = mq_mock
    cm_mock.__aexit__.return_value = None
    mocker.patch("app.k8s.client.acquire_rabbitmq_channel", return_value=cm_mock)
    sleep_mock = mocker.patch("app.k8s.client.asyncio.sleep", new_callable=AsyncMock)

    map_job = MagicMock()
    map_job.metadata.name = "worker-map-12345678-abcde"
    map_job.metadata.labels = {"job_id": job_id, "phase": "map"}

    reduce_job = MagicMock()
    reduce_job.metadata.name = "worker-reduce-12345678-p0-fghij"
    reduce_job.metadata.labels = {"job_id": job_id, "phase": "reduce"}

    mock_k8s_api["list_jobs"].return_value = MagicMock(items=[map_job, reduce_job])

    provider = RealKubernetesProvider()
    await provider.delete_job(job_id, phase="map")

    sleep_mock.assert_not_awaited()

    mq_mock.queue_delete.assert_called_once_with(f"map_tasks_{job_id}")
    mock_k8s_api["delete_job"].assert_called_once_with(
        name="worker-map-12345678-abcde",
        namespace="test-namespace",
        propagation_policy="Background",
    )


@pytest.mark.asyncio
async def test_delete_job_cleans_per_partition_reduce_queues_safely(
    mocker: Any,
    mock_settings: None,
    mock_k8s_api: Dict[str, Any],
) -> None:
    """Verifies delete_job cleans map, legacy reduce, and per-partition reduce_tasks_{id}_{p} queues without failing on absent ones."""
    job_id = "12345678-1111-2222-3333-444444444444"
    mq_mock = AsyncMock()
    mq_mock.queue_delete.side_effect = [None, RuntimeError("NOT_FOUND"), None, None]
    cm_mock = AsyncMock()
    cm_mock.__aenter__.return_value = mq_mock
    cm_mock.__aexit__.return_value = None
    mocker.patch("app.k8s.client.acquire_rabbitmq_channel", return_value=cm_mock)
    mocker.patch("app.k8s.client.asyncio.sleep", new_callable=AsyncMock)

    map_job = MagicMock()
    map_job.metadata.name = "worker-map-12345678-abcde"
    map_job.metadata.labels = {"job_id": job_id}

    reduce_job = MagicMock()
    reduce_job.metadata.name = "worker-reduce-12345678-fghij"
    reduce_job.metadata.labels = {"job_id": job_id}

    mock_k8s_api["list_jobs"].return_value = MagicMock(items=[map_job, reduce_job])

    provider = RealKubernetesProvider()
    await provider.delete_job(job_id, num_reducers=2)

    deleted_queues = [call.args[0] for call in mq_mock.queue_delete.call_args_list]
    assert deleted_queues == [
        f"map_tasks_{job_id}",
        f"reduce_tasks_{job_id}",
        f"reduce_tasks_{job_id}_0",
        f"reduce_tasks_{job_id}_1",
    ]
    assert mock_k8s_api["delete_job"].call_count == 2


@pytest.mark.asyncio
async def test_get_active_pod_counts_by_job_batches_single_call(
    mocker: Any,
    mock_settings: None,
    mock_k8s_api: Dict[str, Any],
) -> None:
    """Verifies get_active_pod_counts_by_job issues ONE list_namespaced_pod call and groups Running/Pending pods."""
    p1 = MagicMock()
    p1.metadata.labels = {"job_id": "job-a"}
    p1.status.phase = "Running"
    p2 = MagicMock()
    p2.metadata.labels = {"job_id": "job-a"}
    p2.status.phase = "Pending"
    p3 = MagicMock()
    p3.metadata.labels = {"job_id": "job-b"}
    p3.status.phase = "Succeeded"

    mock_k8s_api["list_pods"].return_value = MagicMock(items=[p1, p2, p3])

    provider = RealKubernetesProvider()
    mocker.patch.object(k8s_client_module, "_global_k8s_provider", provider)
    counts = await get_active_pod_counts_by_job()

    mock_k8s_api["list_pods"].assert_awaited_once_with(
        namespace="test-namespace",
        label_selector="app=worker-node",
    )
    assert counts == {"job-a": 2}

    await teardown_k8s()
    mock_k8s_api["api_instance"].close.assert_awaited_once()
