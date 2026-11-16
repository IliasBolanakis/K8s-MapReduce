"""
Unit tests for the TaskEventProcessor orchestration logic.

Validates:
    - Multi-tenant S3 routing and dynamic class derivation.
    - ADR-04: Per-partition queue publication BEFORE spawning 1 Indexed Reduce Job,
      and targeted single-partition non-indexed recovery.
    - ADR-05: Atomic Map-to-Reduce capacity swap (``swap_map_to_reduce_capacity``)
      without leaking capacity or releasing ``reduce_waiting`` on starvation.
    - ADR-06: Monotonic ``attempt_id`` zombie fencing (dropping stale signals) and
      fenced Reduce staging promotion + ``_attempts/`` cleanup.
    - ADR-13: Isolated consumer channels in ``start_consumer`` and pooled channel
      leasing via ``acquire_rabbitmq_channel``.
"""

import json
import uuid
from typing import Any, Dict
from unittest.mock import AsyncMock

import pytest

from app.models.schemas import WorkerSignal
from app.services.task_consumer import TaskEventProcessor, start_consumer


@pytest.fixture
def mock_dependencies(mocker: Any) -> Dict[str, Any]:
    """
    Mocks infrastructure providers and repositories for isolated TaskEventProcessor tests.
    """
    mq_channel_mock = AsyncMock()
    mq_channel_mock.default_exchange.publish = AsyncMock()

    acq_cm = AsyncMock()
    acq_cm.__aenter__.return_value = mq_channel_mock
    acq_cm.__aexit__.return_value = None
    acquire_patch = mocker.patch(
        "app.services.task_consumer.acquire_rabbitmq_channel",
        return_value=acq_cm,
    )

    cache_repo = mocker.patch("app.services.task_consumer.CacheRepository", spec=True)
    cache_repo.get_expected_task_attempt = AsyncMock(return_value=None)
    cache_repo.cache_num_reducers = AsyncMock()
    cache_repo.mint_task_attempt = AsyncMock(return_value=1)
    cache_repo.mint_task_attempts_batch = AsyncMock(side_effect=lambda _j, tids, _r: [1] * len(tids))
    cache_repo.cleanup_job_metadata = AsyncMock()
    cache_repo.release_job_capacity = AsyncMock()
    cache_repo.evict_ess_endpoint = AsyncMock(return_value=1)

    return {
        "db_pool": mocker.patch("app.services.task_consumer.get_db_pool", new_callable=AsyncMock),
        "acquire_mq": acquire_patch,
        "mq_channel_mock": mq_channel_mock,
        "k8s_provider": mocker.patch("app.services.task_consumer.get_k8s_provider", new_callable=AsyncMock),
        "redis_client_provider": mocker.patch("app.services.task_consumer.get_redis_client", new_callable=AsyncMock),
        "job_repo": mocker.patch("app.services.task_consumer.JobRepository", spec=True),
        "cache_repo": cache_repo,
        "storage_service": mocker.patch("app.services.task_consumer.StorageService", spec=True),
    }


@pytest.fixture
def valid_job_id() -> str:
    return str(uuid.uuid4())


@pytest.fixture
def valid_user_id() -> str:
    return str(uuid.uuid4())


@pytest.fixture
def map_completion_payload(valid_job_id: str) -> Dict[str, Any]:
    return {
        "jobId": valid_job_id,
        "taskId": "map_task_0",
        "jobToken": "secure-hmac-token-123",
        "status": "COMPLETED",
        "workerBindAddress": "physical-node-01@@10.0.1.45:8080",
    }


@pytest.mark.asyncio
async def test_process_message_routing_to_map(
    mocker: Any,
    mock_dependencies: Dict[str, Any],
    map_completion_payload: Dict[str, Any],
    valid_job_id: str,
) -> None:
    """Verifies signal validation and routing when unseeded (attempt_id=0, expected=None)."""
    mock_message = mocker.MagicMock()
    mock_message.body = json.dumps(map_completion_payload).encode()
    mock_message.reject = AsyncMock()
    mock_message.process.return_value = mocker.AsyncMock()

    handler_mock = mocker.patch.object(TaskEventProcessor, "_handle_map_completion", new_callable=AsyncMock)

    await TaskEventProcessor.process_message(mock_message)

    handler_mock.assert_called_once()
    parsed_payload = handler_mock.call_args[0][0]
    assert str(parsed_payload.job_id) == valid_job_id
    assert parsed_payload.status == "COMPLETED"
    assert parsed_payload.job_token == "secure-hmac-token-123"
    assert parsed_payload.attempt_id == 0


@pytest.mark.asyncio
async def test_process_message_drops_stale_zombie_attempt_signal(
    mocker: Any,
    mock_dependencies: Dict[str, Any],
    valid_job_id: str,
) -> None:
    """Verifies ADR-06 fencing drops stale signals whose attemptId != Redis active attempt."""
    deps = mock_dependencies
    deps["cache_repo"].get_expected_task_attempt = AsyncMock(return_value=2)

    mock_message = mocker.MagicMock()
    mock_message.body = json.dumps({
        "jobId": valid_job_id,
        "taskId": "map-chunk-0",
        "jobToken": "tok",
        "status": "COMPLETED",
        "attemptId": 1,
    }).encode()
    mock_message.reject = AsyncMock()
    mock_message.process.return_value = mocker.AsyncMock()

    map_handler = mocker.patch.object(TaskEventProcessor, "_handle_map_completion", new_callable=AsyncMock)
    reclaim_mock = mocker.patch("app.services.task_consumer.forcefully_reclaim_job", new_callable=AsyncMock)

    await TaskEventProcessor.process_message(mock_message)

    map_handler.assert_not_called()
    reclaim_mock.assert_not_called()
    mock_message.reject.assert_not_called()


@pytest.mark.asyncio
async def test_process_message_allows_matching_attempt_signal(
    mocker: Any,
    mock_dependencies: Dict[str, Any],
    valid_job_id: str,
) -> None:
    """Verifies ADR-06 fencing passes signals matching Redis active attemptId."""
    deps = mock_dependencies
    deps["cache_repo"].get_expected_task_attempt = AsyncMock(return_value=2)

    mock_message = mocker.MagicMock()
    mock_message.body = json.dumps({
        "jobId": valid_job_id,
        "taskId": "0",
        "jobToken": "tok",
        "status": "COMPLETED",
        "attemptId": 2,
    }).encode()
    mock_message.reject = AsyncMock()
    mock_message.process.return_value = mocker.AsyncMock()

    reduce_handler = mocker.patch.object(TaskEventProcessor, "_handle_reduce_completion", new_callable=AsyncMock)

    await TaskEventProcessor.process_message(mock_message)

    reduce_handler.assert_awaited_once()
    assert reduce_handler.call_args[0][0].attempt_id == 2


@pytest.mark.asyncio
async def test_process_message_rejects_malformed_payload(mocker: Any) -> None:
    """Verifies Poison Pill Protection ensures malformed JSON is rejected to DLX."""
    mock_message = mocker.MagicMock()
    mock_message.body = b"invalid_json_payload"
    mock_message.reject = AsyncMock()
    mock_message.process.return_value = mocker.AsyncMock()

    await TaskEventProcessor.process_message(mock_message)
    mock_message.reject.assert_called_once_with(requeue=False)


@pytest.mark.asyncio
async def test_process_message_fails_fast_on_worker_error(
    mock_dependencies: Dict[str, Any],
    valid_job_id: str,
    mocker: Any,
) -> None:
    """Verifies Fail-Fast Protocol triggers immediate teardown on deterministic user errors."""
    mock_message = mocker.MagicMock()
    mock_message.body = json.dumps({
        "jobId": valid_job_id,
        "taskId": "map1",
        "jobToken": "token",
        "status": "FAILED",
        "error": "OutOfMemoryError",
    }).encode()
    mock_message.reject = AsyncMock()
    mock_message.process.return_value = mocker.AsyncMock()

    mock_reclaim = mocker.patch("app.services.task_consumer.forcefully_reclaim_job", new_callable=AsyncMock)

    await TaskEventProcessor.process_message(mock_message)

    mock_reclaim.assert_called_once()
    assert mock_reclaim.call_args[0][0] == valid_job_id
    mock_message.reject.assert_called_once_with(requeue=False)


@pytest.mark.asyncio
async def test_process_message_intercepts_shuffle_failure(
    mock_dependencies: Dict[str, Any],
    valid_job_id: str,
    mocker: Any,
) -> None:
    """Verifies Sentinel Interceptor triggers Lineage Recovery instead of job failure."""
    deps = mock_dependencies
    mock_message = mocker.MagicMock()
    mock_message.body = json.dumps({
        "jobId": valid_job_id,
        "taskId": "reduce1",
        "jobToken": "token",
        "status": "FAILED",
        "error": "SHUFFLE_FETCH_FAILED:map-chunk-5",
    }).encode()
    mock_message.reject = AsyncMock()
    mock_message.process.return_value = mocker.AsyncMock()

    recover_mock = mocker.patch.object(TaskEventProcessor, "_recover_lost_map_task", new_callable=AsyncMock)
    deps["cache_repo"].increment_task_failure = AsyncMock(return_value=1)
    deps["redis_client_provider"].return_value.set = AsyncMock(return_value=True)

    await TaskEventProcessor.process_message(mock_message)

    recover_mock.assert_called_once()
    assert recover_mock.call_args[0][1] == "map-chunk-5"
    mock_message.reject.assert_not_called()


@pytest.mark.asyncio
async def test_map_barrier_triggers_event_publication(
    mock_dependencies: Dict[str, Any],
    valid_job_id: str,
) -> None:
    """Validates Distributed Barrier logic triggers Reduce transition publication."""
    deps = mock_dependencies
    payload = WorkerSignal(
        jobId=valid_job_id,
        taskId="map_0",
        jobToken="secure-token-abc",
        state="COMPLETED",
        workerBindAddress="node1@@10.0.1.45:8080",
    )

    deps["cache_repo"].pop_from_recovery_waitlist = AsyncMock(return_value=[])
    deps["cache_repo"].register_map_completion_atomic = AsyncMock(return_value=True)
    deps["job_repo"].flush_map_phase = AsyncMock()

    await TaskEventProcessor._handle_map_completion(payload)

    deps["job_repo"].flush_map_phase.assert_called_once()
    deps["mq_channel_mock"].default_exchange.publish.assert_called_once()


@pytest.mark.asyncio
async def test_trigger_reduce_phase_publishes_partition_queues_then_spawns_one_indexed_job(
    mock_dependencies: Dict[str, Any],
    valid_job_id: str,
    valid_user_id: str,
) -> None:
    """
    Verifies ADR-04 normal Reduce phase:
    1. Declares & publishes per-partition lazy queues reduce_tasks_{job_id}_{p_id} BEFORE spawning pods.
    2. Spawns ONE Indexed K8s Job (replicas=num_reducers, phase='reduce', indexed=True).
    """
    deps = mock_dependencies
    num_reducers = 3
    mock_endpoints = ["node-a@@10.0.1.45:7337", "node-b@@10.0.1.46:7337"]

    call_order = []
    mq_mock = deps["mq_channel_mock"]

    async def _rec_pub(*args, **kwargs):
        call_order.append(("publish", kwargs["routing_key"]))

    async def _rec_spawn(*args, **kwargs):
        call_order.append(("spawn", kwargs))

    mq_mock.default_exchange.publish = AsyncMock(side_effect=_rec_pub)
    k8s_mock = deps["k8s_provider"].return_value
    k8s_mock.spawn_workers = AsyncMock(side_effect=_rec_spawn)
    deps["cache_repo"].get_worker_endpoints = AsyncMock(return_value=mock_endpoints)
    deps["cache_repo"].mint_task_attempts_batch = AsyncMock(return_value=[1, 1, 1])

    await TaskEventProcessor._trigger_reduce_phase(
        job_id=valid_job_id,
        user_id=valid_user_id,
        reducer_filename="WordCountReducer.class",
        reducer_class="WordCountReducer",
        num_reducers=num_reducers,
        file_format="JSON",
    )

    assert mq_mock.declare_queue.call_count == num_reducers
    assert mq_mock.default_exchange.publish.call_count == num_reducers
    k8s_mock.spawn_workers.assert_awaited_once_with(
        job_id=valid_job_id,
        replicas=num_reducers,
        phase="reduce",
        indexed=True,
    )
    # All publishes occurred BEFORE spawning the Indexed Job
    assert [c[0] for c in call_order] == ["publish", "publish", "publish", "spawn"]
    assert sorted(c[1] for c in call_order[:3]) == [
        f"reduce_tasks_{valid_job_id}_0",
        f"reduce_tasks_{valid_job_id}_1",
        f"reduce_tasks_{valid_job_id}_2",
    ]

    sent_msg = json.loads(mq_mock.default_exchange.publish.call_args_list[0][0][0].body.decode())
    assert sent_msg["bucketName"] == "results"
    assert sent_msg["objectName"] == f"{valid_user_id}/{valid_job_id}/"
    assert sent_msg["format"] == "JSON"
    assert sent_msg["attemptId"] == 1


@pytest.mark.asyncio
async def test_trigger_reduce_phase_targeted_recovery_spawns_non_indexed_job(
    mock_dependencies: Dict[str, Any],
    valid_job_id: str,
    valid_user_id: str,
) -> None:
    """Verifies targeted recovery publishes to reduce_tasks_{id}_{p} and spawns non-indexed Job."""
    deps = mock_dependencies
    mq_mock = deps["mq_channel_mock"]
    k8s_mock = deps["k8s_provider"].return_value
    k8s_mock.spawn_workers = AsyncMock()
    deps["cache_repo"].get_worker_endpoints = AsyncMock(return_value=["node-a@@10.0.1.45:7337"])
    deps["cache_repo"].mint_task_attempt = AsyncMock(return_value=2)

    await TaskEventProcessor._trigger_reduce_phase(
        job_id=valid_job_id,
        user_id=valid_user_id,
        reducer_filename="WordCountReducer.class",
        reducer_class="WordCountReducer",
        num_reducers=3,
        target_partition_id=1,
        file_format="JSON",
    )

    mq_mock.declare_queue.assert_awaited_once()
    assert mq_mock.declare_queue.call_args[0][0] == f"reduce_tasks_{valid_job_id}_1"
    assert mq_mock.default_exchange.publish.call_args.kwargs["routing_key"] == f"reduce_tasks_{valid_job_id}_1"
    sent_msg = json.loads(mq_mock.default_exchange.publish.call_args[0][0].body.decode())
    assert sent_msg["attemptId"] == 2
    k8s_mock.spawn_workers.assert_awaited_once_with(
        job_id=valid_job_id,
        replicas=1,
        phase="reduce",
        partition_id=1,
        target_node="node-a",
        indexed=False,
    )


@pytest.mark.asyncio
async def test_reduce_barrier_promotes_staged_output_and_finalizes_job(
    mock_dependencies: Dict[str, Any],
    valid_job_id: str,
    valid_user_id: str,
) -> None:
    """Verifies ADR-06 staged output promotion before barrier and _attempts cleanup on completion."""
    deps = mock_dependencies

    payload = WorkerSignal(
        jobId=valid_job_id,
        taskId="0",
        jobToken="secure-token-abc",
        state="COMPLETED",
        attemptId=2,
    )

    deps["job_repo"].get_job_metadata = AsyncMock(return_value={
        "user_id": uuid.UUID(valid_user_id),
        "total_chunks": 5,
        "num_reducers": 3,
        "reducer_filename": "Reducer.class",
    })
    deps["cache_repo"].register_reduce_completion_atomic = AsyncMock(return_value=True)
    deps["job_repo"].flush_reduce_phase = AsyncMock()
    deps["job_repo"].mark_job_completed = AsyncMock()
    deps["k8s_provider"].return_value.wait_for_job_completion = AsyncMock()
    deps["k8s_provider"].return_value.delete_job = AsyncMock()

    await TaskEventProcessor._handle_reduce_completion(payload)

    deps["storage_service"].promote_staged_reduce_output.assert_called_once_with(
        valid_user_id, valid_job_id, "0", 2
    )
    deps["job_repo"].mark_job_completed.assert_called_once()
    deps["k8s_provider"].return_value.delete_job.assert_called_once_with(valid_job_id, num_reducers=3)
    deps["storage_service"].cleanup_job_attempts_prefix.assert_called_once_with(valid_user_id, valid_job_id)
    deps["cache_repo"].release_job_capacity.assert_called_once()
    deps["cache_repo"].cleanup_job_metadata.assert_called_once()


@pytest.mark.asyncio
async def test_process_message_shuffle_fetch_failed_from_node(
    mock_dependencies: Dict[str, Any],
    valid_job_id: str,
    mocker: Any,
) -> None:
    """Verifies node-level shuffle failure resolves affected map tasks via CacheRepository and recovers each."""
    deps = mock_dependencies
    mock_message = mocker.MagicMock()
    mock_message.body = json.dumps({
        "jobId": valid_job_id,
        "taskId": "0",
        "jobToken": "token",
        "status": "FAILED",
        "error": "SHUFFLE_FETCH_FAILED_FROM_NODE:10.0.1.45:7337",
    }).encode()
    mock_message.reject = AsyncMock()
    mock_message.process.return_value = mocker.AsyncMock()

    deps["cache_repo"].get_tasks_by_endpoint = AsyncMock(return_value=["map-chunk-0", "map-chunk-2"])
    deps["cache_repo"].increment_task_failure = AsyncMock(return_value=1)
    deps["cache_repo"].add_to_recovery_waitlist = AsyncMock()
    deps["redis_client_provider"].return_value.set = AsyncMock(return_value=True)
    recover_mock = mocker.patch.object(TaskEventProcessor, "_recover_lost_map_task", new_callable=AsyncMock)

    await TaskEventProcessor.process_message(mock_message)

    deps["cache_repo"].get_tasks_by_endpoint.assert_called_once_with(
        valid_job_id, "10.0.1.45:7337", deps["redis_client_provider"].return_value
    )
    deps["cache_repo"].evict_ess_endpoint.assert_called_once_with(
        valid_job_id, "10.0.1.45:7337", deps["redis_client_provider"].return_value
    )
    assert recover_mock.call_count == 2
    mock_message.reject.assert_not_called()


@pytest.mark.asyncio
async def test_map_completion_clears_recovering_lock_and_wakes_waitlist(
    mock_dependencies: Dict[str, Any],
    valid_job_id: str,
    valid_user_id: str,
    mocker: Any,
) -> None:
    """Verifies recovery completion deletes recovering lock and wakes waiting Reducers."""
    deps = mock_dependencies
    payload = WorkerSignal(
        jobId=valid_job_id,
        taskId="map-chunk-0",
        jobToken="tok",
        state="COMPLETED",
        workerBindAddress="node2@@10.0.1.99:7337",
    )
    deps["cache_repo"].get_cached_total_chunks = AsyncMock(return_value=2)
    deps["cache_repo"].add_worker_endpoint = AsyncMock()
    deps["cache_repo"].pop_from_recovery_waitlist = AsyncMock(return_value=["0", "1"])
    deps["job_repo"].get_job_metadata = AsyncMock(return_value={
        "user_id": uuid.UUID(valid_user_id),
        "reducer_filename": "WordCountReducer.class",
        "num_reducers": 2,
        "format": "TEXT",
    })
    trigger_mock = mocker.patch.object(TaskEventProcessor, "_trigger_reduce_phase", new_callable=AsyncMock)
    redis_mock = deps["redis_client_provider"].return_value
    redis_mock.delete = AsyncMock()

    await TaskEventProcessor._handle_map_completion(payload)

    redis_mock.delete.assert_awaited_once_with(f"job:{valid_job_id}:recovering:map-chunk-0")
    assert trigger_mock.await_count == 2


@pytest.mark.asyncio
async def test_process_message_generic_failed_retries_map_and_acks(
    mock_dependencies: Dict[str, Any],
    valid_job_id: str,
    mocker: Any,
) -> None:
    """Verifies non-user-bug Map failure (<3 strikes) republishes via _recover_lost_map_task and ACKs signal."""
    deps = mock_dependencies
    mock_message = mocker.MagicMock()
    mock_message.body = json.dumps({
        "jobId": valid_job_id,
        "taskId": "map-chunk-3",
        "jobToken": "token",
        "status": "FAILED",
        "error": "SocketTimeoutException: MinIO read timed out",
    }).encode()
    mock_message.reject = AsyncMock()
    mock_message.process.return_value = mocker.AsyncMock()

    deps["cache_repo"].increment_task_failure = AsyncMock(return_value=1)
    recover_mock = mocker.patch.object(TaskEventProcessor, "_recover_lost_map_task", new_callable=AsyncMock)

    await TaskEventProcessor.process_message(mock_message)

    recover_mock.assert_called_once_with(valid_job_id, "map-chunk-3")
    mock_message.reject.assert_not_called()


@pytest.mark.asyncio
async def test_process_message_generic_failed_retries_reduce_and_acks(
    mock_dependencies: Dict[str, Any],
    valid_job_id: str,
    valid_user_id: str,
    mocker: Any,
) -> None:
    """Verifies non-user-bug Reduce failure (<3 strikes) republishes target partition and ACKs signal."""
    deps = mock_dependencies
    mock_message = mocker.MagicMock()
    mock_message.body = json.dumps({
        "jobId": valid_job_id,
        "taskId": "1",
        "jobToken": "token",
        "status": "FAILED",
        "error": "Transient container eviction",
    }).encode()
    mock_message.reject = AsyncMock()
    mock_message.process.return_value = mocker.AsyncMock()

    deps["cache_repo"].increment_task_failure = AsyncMock(return_value=1)
    deps["job_repo"].get_job_metadata = AsyncMock(return_value={
        "user_id": uuid.UUID(valid_user_id),
        "total_chunks": 4,
        "reducer_filename": "WordCountReducer.class",
        "num_reducers": 3,
        "format": "JSON",
        "status": "RUNNING",
    })
    trigger_mock = mocker.patch.object(TaskEventProcessor, "_trigger_reduce_phase", new_callable=AsyncMock)

    await TaskEventProcessor.process_message(mock_message)

    trigger_mock.assert_called_once_with(
        job_id=valid_job_id,
        user_id=valid_user_id,
        reducer_filename="WordCountReducer.class",
        reducer_class="WordCountReducer",
        num_reducers=3,
        target_partition_id=1,
        file_format="JSON",
    )
    mock_message.reject.assert_not_called()


@pytest.mark.asyncio
async def test_recover_lost_map_task_passes_combiner_format_and_redis(
    mock_dependencies: Dict[str, Any],
    valid_job_id: str,
    valid_user_id: str,
    mocker: Any,
) -> None:
    """Verifies _recover_lost_map_task forwards combiner, file_format, and redis_client."""
    deps = mock_dependencies
    deps["job_repo"].get_job_recovery_context = AsyncMock(return_value={
        "user_id": uuid.UUID(valid_user_id),
        "format": "JSON",
        "input_filename": "events.json",
        "total_chunks": 4,
        "num_reducers": 2,
        "num_mappers": 2,
        "mapper_filename": "JsonMapper.class",
        "combiner_filename": "JsonCombiner.class",
    })
    deps["storage_service"].get_client.return_value.stat_object.return_value.size = 1024
    deps["k8s_provider"].return_value.spawn_workers = AsyncMock()
    republish_mock = mocker.patch(
        "app.services.task_consumer.TaskPublisher.republish_single_map_task",
        new_callable=AsyncMock,
    )

    await TaskEventProcessor._recover_lost_map_task(valid_job_id, "map-chunk-2")

    republish_mock.assert_called_once()
    kwargs = republish_mock.call_args.kwargs
    assert kwargs["combiner_obj"] == f"{valid_user_id}/JsonCombiner.class"
    assert kwargs["combiner_class"] == "JsonCombiner"
    assert kwargs["file_format"] == "JSON"
    assert kwargs["redis_client"] is deps["redis_client_provider"].return_value


@pytest.mark.asyncio
async def test_process_orchestration_event_atomic_swap_and_starvation_backoff(
    mock_dependencies: Dict[str, Any],
    valid_job_id: str,
    valid_user_id: str,
    mocker: Any,
) -> None:
    """
    Verifies ADR-05 orchestration transition:
    1. Idempotent phase='map' cleanup runs once.
    2. Atomic swap_map_to_reduce_capacity runs without calling release_job_capacity separately.
    3. Capacity starvation delays 2s and republishes without clearing reduce_waiting.
    """
    deps = mock_dependencies
    mock_message = mocker.MagicMock()
    mock_message.body = valid_job_id.encode("utf-8")
    mock_message.reject = AsyncMock()
    mock_message.process.return_value = mocker.AsyncMock()

    deps["job_repo"].get_job_metadata = AsyncMock(return_value={
        "user_id": uuid.UUID(valid_user_id),
        "total_chunks": 4,
        "reducer_filename": "WordCountReducer.class",
        "num_reducers": 3,
        "format": "TEXT",
        "status": "RUNNING",
    })
    deps["k8s_provider"].return_value.delete_job = AsyncMock()
    deps["cache_repo"].swap_map_to_reduce_capacity = AsyncMock(return_value=0)
    deps["redis_client_provider"].return_value.set = AsyncMock(side_effect=[True, False])
    sleep_mock = mocker.patch("app.services.task_consumer.asyncio.sleep", new_callable=AsyncMock)

    await TaskEventProcessor.process_orchestration_event(mock_message)
    await TaskEventProcessor.process_orchestration_event(mock_message)

    deps["k8s_provider"].return_value.delete_job.assert_called_once_with(valid_job_id, phase="map")
    deps["cache_repo"].release_job_capacity.assert_not_called()
    assert deps["cache_repo"].swap_map_to_reduce_capacity.await_count == 2
    assert sleep_mock.call_count == 2
    sleep_mock.assert_called_with(2)
    assert deps["mq_channel_mock"].default_exchange.publish.call_count == 2
    mock_message.reject.assert_not_called()


@pytest.mark.asyncio
async def test_start_consumer_uses_two_isolated_channels(mocker: Any) -> None:
    """Verifies ADR-13 start_consumer binds job_events_queue and orchestration_events_queue to separate channels."""
    job_ch = AsyncMock()
    orch_ch = AsyncMock()
    job_q = AsyncMock()
    orch_q = AsyncMock()
    job_ch.declare_queue = AsyncMock(return_value=job_q)
    orch_ch.declare_queue = AsyncMock(return_value=orch_q)

    mocker.patch("app.services.task_consumer.get_job_events_channel", AsyncMock(return_value=job_ch))
    mocker.patch("app.services.task_consumer.get_orchestration_events_channel", AsyncMock(return_value=orch_ch))
    mocker.patch("app.services.task_consumer.settings.RABBITMQ_ORCHESTRATION_PREFETCH", 12)

    await start_consumer()

    job_ch.set_qos.assert_awaited_once_with(prefetch_count=100)
    orch_ch.set_qos.assert_awaited_once_with(prefetch_count=12)
    job_ch.declare_queue.assert_awaited_once_with("job_events_queue", durable=True)
    orch_ch.declare_queue.assert_awaited_once_with("orchestration_events_queue", durable=True)
    job_q.consume.assert_awaited_once_with(TaskEventProcessor.process_message)
    orch_q.consume.assert_awaited_once_with(TaskEventProcessor.process_orchestration_event)
