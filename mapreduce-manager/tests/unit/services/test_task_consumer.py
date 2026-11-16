"""
Unit tests for the TaskEventProcessor orchestration logic.

This suite verifies the granular routing of worker signals, the distributed
management of phase barriers via Redis Lua locks, and the triggering of
downstream infrastructure transitions (Kubernetes and RabbitMQ) within
a multi-tenant S3 architecture.

We test for:
    - **Multi-Tenant Routing:** Validates that orchestrators now use User IDs and
      explicit filenames to construct S3 paths instead of dynamic URI parsing.
    - **Dynamic Binding:** Validates that the system correctly derives execution
      classes from filenames without relying on redundant DB fields.
    - **Data Locality Scheduling:** Validates that Reduce phase orchestration spawns
      individual Kubernetes micro-jobs per partition to allow for specific node targeting.
    - **Zero-Trust Security:** Enforces the presence and validation of HMAC `job_token`s
      in task completion signaling (Pydantic v2.x compliant).
"""

import json
import uuid
from typing import Any, Dict
from unittest.mock import AsyncMock

import pytest

from app.models.schemas import WorkerSignal
from app.services.task_consumer import TaskEventProcessor


# --- FIXTURES ---

@pytest.fixture
def mock_dependencies(mocker: Any) -> Dict[str, Any]:
    """
    Mocks all infrastructure providers and repositories required for orchestration tests.
    """
    return {
        "db_pool": mocker.patch("app.services.task_consumer.get_db_pool", new_callable=AsyncMock),
        "mq_channel": mocker.patch("app.services.task_consumer.get_rabbitmq_channel", new_callable=AsyncMock),
        "k8s_provider": mocker.patch("app.services.task_consumer.get_k8s_provider", new_callable=AsyncMock),
        "redis_client_provider": mocker.patch("app.services.task_consumer.get_redis_client", new_callable=AsyncMock),
        "job_repo": mocker.patch("app.services.task_consumer.JobRepository", spec=True),
        "cache_repo": mocker.patch("app.services.task_consumer.CacheRepository", spec=True),
        "storage_service": mocker.patch("app.services.task_consumer.StorageService", spec=True)
    }


@pytest.fixture
def valid_job_id() -> str:
    """Provides a valid UUID string to satisfy PostgreSQL and Pydantic strict typing."""
    return str(uuid.uuid4())


@pytest.fixture
def valid_user_id() -> str:
    """Provides a valid User UUID string for multi-tenant path testing."""
    return str(uuid.uuid4())


@pytest.fixture
def map_completion_payload(valid_job_id: str) -> Dict[str, str]:
    """Provides a standard Map completion signal, updated for Pydantic v2 naming."""
    return {
        "jobId": valid_job_id,
        "taskId": "map_task_0",
        "jobToken": "secure-hmac-token-123",
        "status": "COMPLETED",
        "workerBindAddress": "physical-node-01@@10.0.1.45:8080"
    }


# --- TESTS ---

@pytest.mark.asyncio
async def test_process_message_routing_to_map(
    mocker: Any,
    mock_dependencies: Dict[str, Any],
    map_completion_payload: Dict[str, str],
    valid_job_id: str
) -> None:
    """Verifies that security tokens are parsed using Pydantic v2.x snake_case attributes."""
    mock_message = mocker.MagicMock()
    mock_message.body = json.dumps(map_completion_payload).encode()
    mock_message.reject = AsyncMock()

    cms = mocker.AsyncMock()
    mock_message.process.return_value = cms
    cms.__aenter__.return_value = None
    cms.__aexit__.return_value = None

    handler_mock = mocker.patch.object(TaskEventProcessor, "_handle_map_completion", new_callable=AsyncMock)

    await TaskEventProcessor.process_message(mock_message)

    handler_mock.assert_called_once()
    parsed_payload = handler_mock.call_args[0][0]

    assert str(parsed_payload.job_id) == valid_job_id
    assert parsed_payload.status == "COMPLETED"
    assert parsed_payload.job_token == "secure-hmac-token-123"


@pytest.mark.asyncio
async def test_process_message_rejects_malformed_payload(mocker: Any) -> None:
    """Verifies Poison Pill Protection ensures malformed JSON is moved to DLX."""
    mock_message = mocker.MagicMock()
    mock_message.body = b"invalid_json_payload"
    mock_message.reject = AsyncMock()

    cms = mocker.AsyncMock()
    mock_message.process.return_value = cms

    await TaskEventProcessor.process_message(mock_message)
    mock_message.reject.assert_called_once_with(requeue=False)


@pytest.mark.asyncio
async def test_process_message_fails_fast_on_worker_error(
        mock_dependencies: Dict[str, Any],
        valid_job_id: str,
        mocker: Any
) -> None:
    """Verifies Fail-Fast Protocol triggers immediate infrastructure teardown on deterministic errors."""
    deps = mock_dependencies
    mock_message = mocker.MagicMock()
    mock_message.body = json.dumps({
        "jobId": valid_job_id,
        "taskId": "map1",
        "jobToken": "token",
        "status": "FAILED",
        "error": "OutOfMemoryError"
    }).encode()
    mock_message.reject = AsyncMock()

    cms = mocker.AsyncMock()
    mock_message.process.return_value = cms

    # MOCK the centralized lifecycle method that handles the teardown
    mock_reclaim = mocker.patch("app.services.task_consumer.forcefully_reclaim_job", new_callable=AsyncMock)

    await TaskEventProcessor.process_message(mock_message)

    # ASSERT the centralized teardown was triggered with the correct job_id
    mock_reclaim.assert_called_once()
    assert mock_reclaim.call_args[0][0] == valid_job_id

    # ASSERT the message is rejected without being requeued
    mock_message.reject.assert_called_once_with(requeue=False)


@pytest.mark.asyncio
async def test_process_message_intercepts_shuffle_failure(
        mock_dependencies: Dict[str, Any],
        valid_job_id: str,
        mocker: Any
) -> None:
    """Verifies Sentinel Interceptor triggers Lineage Recovery instead of job failure."""
    deps = mock_dependencies
    mock_message = mocker.MagicMock()
    mock_message.body = json.dumps({
        "jobId": valid_job_id,
        "taskId": "reduce1",
        "jobToken": "token",
        "status": "FAILED",
        "error": "SHUFFLE_FETCH_FAILED:map-chunk-5"
    }).encode()
    mock_message.reject = AsyncMock()

    cms = mocker.AsyncMock()
    mock_message.process.return_value = cms

    recover_mock = mocker.patch.object(TaskEventProcessor, "_recover_lost_map_task", new_callable=AsyncMock)

    deps["cache_repo"].increment_task_failure = AsyncMock(return_value=1)

    redis_mock = deps["redis_client_provider"].return_value
    redis_mock.set = AsyncMock(return_value=True)

    await TaskEventProcessor.process_message(mock_message)

    recover_mock.assert_called_once()
    assert recover_mock.call_args[0][1] == "map-chunk-5"
    mock_message.reject.assert_not_called()


@pytest.mark.asyncio
async def test_map_barrier_triggers_event_publication(mock_dependencies: Dict[str, Any], valid_job_id: str) -> None:
    """Validates Distributed Barrier logic triggers the transition to the Reduce phase."""
    deps = mock_dependencies

    # FIX: Use WorkerSignal instead of TaskCompletedRequest
    payload = WorkerSignal(
        jobId=valid_job_id,
        taskId="map_0",
        jobToken="secure-token-abc",
        state="COMPLETED",
        workerBindAddress="node1@@10.0.1.45:8080"
    )

    deps["cache_repo"].pop_from_recovery_waitlist = AsyncMock(return_value=[])
    deps["cache_repo"].register_map_completion_atomic = AsyncMock(return_value=True)
    deps["job_repo"].flush_map_phase = AsyncMock()

    mq_mock = deps["mq_channel"].return_value
    mq_mock.default_exchange.publish = AsyncMock()

    await TaskEventProcessor._handle_map_completion(payload)

    deps["job_repo"].flush_map_phase.assert_called_once()
    mq_mock.default_exchange.publish.assert_called_once()


@pytest.mark.asyncio
async def test_trigger_reduce_phase_multi_tenant_logic(
    mock_dependencies: Dict[str, Any],
    valid_job_id: str,
    valid_user_id: str
) -> None:
    """Verifies multi-tenant bucket construction and iterative pod spawning."""
    deps = mock_dependencies
    num_reducers = 2
    mock_endpoints = ["node-a@@10.0.1.45:8080", "node-b@@10.0.1.46:8080"]

    mq_mock = deps["mq_channel"].return_value
    mq_mock.default_exchange.publish = AsyncMock()
    k8s_mock = deps["k8s_provider"].return_value
    k8s_mock.spawn_workers = AsyncMock()

    deps["cache_repo"].get_worker_endpoints = AsyncMock(return_value=mock_endpoints)

    # Act: Trigger using multi-tenant specific parameters, simulating the derived class name
    await TaskEventProcessor._trigger_reduce_phase(
        job_id=valid_job_id,
        user_id=valid_user_id,
        reducer_filename="WordCountReducer.class",
        reducer_class="WordCountReducer",
        num_reducers=num_reducers
    )

    # Assert: Verify infrastructure was called for each partition
    assert mq_mock.default_exchange.publish.call_count == num_reducers
    assert k8s_mock.spawn_workers.call_count == num_reducers

    # Verify the TaskMessage uses the hardcoded 'results' bucket per multi-tenant spec
    sent_msg_json = mq_mock.default_exchange.publish.call_args[0][0].body.decode()
    sent_msg = json.loads(sent_msg_json)
    assert sent_msg["bucketName"] == "results"
    assert sent_msg["objectName"] == f"{valid_user_id}/{valid_job_id}/"


@pytest.mark.asyncio
async def test_reduce_barrier_finalizes_job(
    mock_dependencies: Dict[str, Any],
    valid_job_id: str,
    valid_user_id: str
) -> None:
    """Validates final Reduce barrier triggers job completion and capacity release."""
    deps = mock_dependencies

    # FIX: Use WorkerSignal instead of TaskCompletedRequest
    payload = WorkerSignal(
        jobId=valid_job_id,
        taskId="0",
        jobToken="secure-token-abc",
        state="COMPLETED"
    )

    # Mock metadata with the new user_id field, omitting legacy _class_name fields
    deps["job_repo"].get_job_metadata = AsyncMock(return_value={
        "user_id": uuid.UUID(valid_user_id),
        "total_chunks": 5,
        "num_reducers": 3,
        "reducer_filename": "Reducer.class"
    })

    deps["cache_repo"].register_reduce_completion_atomic = AsyncMock(return_value=True)
    deps["job_repo"].mark_job_completed = AsyncMock()

    await TaskEventProcessor._handle_reduce_completion(payload)

    deps["job_repo"].mark_job_completed.assert_called_once()
    deps["k8s_provider"].return_value.delete_job.assert_called_once()
    deps["cache_repo"].release_job_capacity.assert_called_once()


@pytest.mark.asyncio
async def test_process_message_shuffle_fetch_failed_from_node(
    mock_dependencies: Dict[str, Any],
    valid_job_id: str,
    mocker: Any
) -> None:
    """Verifies node-level shuffle failure resolves affected map tasks via CacheRepository and recovers each."""
    deps = mock_dependencies
    mock_message = mocker.MagicMock()
    mock_message.body = json.dumps({
        "jobId": valid_job_id,
        "taskId": "0",
        "jobToken": "token",
        "status": "FAILED",
        "error": "SHUFFLE_FETCH_FAILED_FROM_NODE:10.0.1.45:7337"
    }).encode()
    mock_message.reject = AsyncMock()

    cms = mocker.AsyncMock()
    mock_message.process.return_value = cms

    deps["cache_repo"].get_tasks_by_endpoint = AsyncMock(return_value=["map-chunk-0", "map-chunk-2"])
    deps["cache_repo"].increment_task_failure = AsyncMock(return_value=1)
    deps["cache_repo"].add_to_recovery_waitlist = AsyncMock()
    deps["redis_client_provider"].return_value.set = AsyncMock(return_value=True)
    recover_mock = mocker.patch.object(TaskEventProcessor, "_recover_lost_map_task", new_callable=AsyncMock)

    await TaskEventProcessor.process_message(mock_message)

    deps["cache_repo"].get_tasks_by_endpoint.assert_called_once_with(
        valid_job_id, "10.0.1.45:7337", deps["redis_client_provider"].return_value
    )
    assert recover_mock.call_count == 2
    assert recover_mock.call_args_list[0][0] == (valid_job_id, "map-chunk-0")
    assert recover_mock.call_args_list[1][0] == (valid_job_id, "map-chunk-2")
    mock_message.reject.assert_not_called()


@pytest.mark.asyncio
async def test_process_message_generic_failed_retries_map_and_acks(
    mock_dependencies: Dict[str, Any],
    valid_job_id: str,
    mocker: Any
) -> None:
    """Verifies non-user-bug Map failure (<3 strikes) republishes via _recover_lost_map_task and ACKs signal."""
    deps = mock_dependencies
    mock_message = mocker.MagicMock()
    mock_message.body = json.dumps({
        "jobId": valid_job_id,
        "taskId": "map-chunk-3",
        "jobToken": "token",
        "status": "FAILED",
        "error": "SocketTimeoutException: MinIO read timed out"
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
    mocker: Any
) -> None:
    """Verifies non-user-bug Reduce failure (<3 strikes) republishes target partition and ACKs signal."""
    deps = mock_dependencies
    mock_message = mocker.MagicMock()
    mock_message.body = json.dumps({
        "jobId": valid_job_id,
        "taskId": "1",
        "jobToken": "token",
        "status": "FAILED",
        "error": "Transient container eviction"
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
        file_format="JSON"
    )
    mock_message.reject.assert_not_called()


@pytest.mark.asyncio
async def test_recover_lost_map_task_passes_combiner_and_format(
    mock_dependencies: Dict[str, Any],
    valid_job_id: str,
    valid_user_id: str,
    mocker: Any
) -> None:
    """Verifies _recover_lost_map_task forwards combiner_obj, combiner_class, and file_format."""
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
        new_callable=AsyncMock
    )

    await TaskEventProcessor._recover_lost_map_task(valid_job_id, "map-chunk-2")

    republish_mock.assert_called_once()
    kwargs = republish_mock.call_args.kwargs
    assert kwargs["combiner_obj"] == f"{valid_user_id}/JsonCombiner.class"
    assert kwargs["combiner_class"] == "JsonCombiner"
    assert kwargs["file_format"] == "JSON"


@pytest.mark.asyncio
async def test_process_orchestration_event_phase1_cleanup_idempotent_and_backoff(
    mock_dependencies: Dict[str, Any],
    valid_job_id: str,
    valid_user_id: str,
    mocker: Any
) -> None:
    """Verifies phase-1 cleanup runs once with phase='map' and capacity starvation delays + republishes."""
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
    deps["cache_repo"].release_job_capacity = AsyncMock()
    deps["cache_repo"].reserve_cluster_capacity = AsyncMock(return_value=1)  # Starved (< 3)
    deps["redis_client_provider"].return_value.set = AsyncMock(side_effect=[True, False])
    deps["mq_channel"].return_value.default_exchange.publish = AsyncMock()
    sleep_mock = mocker.patch("app.services.task_consumer.asyncio.sleep", new_callable=AsyncMock)

    await TaskEventProcessor.process_orchestration_event(mock_message)
    await TaskEventProcessor.process_orchestration_event(mock_message)

    deps["k8s_provider"].return_value.delete_job.assert_called_once_with(valid_job_id, phase="map")
    assert sleep_mock.call_count == 2
    sleep_mock.assert_called_with(2)
    assert deps["mq_channel"].return_value.default_exchange.publish.call_count == 2
    mock_message.reject.assert_not_called()


@pytest.mark.asyncio
async def test_trigger_reduce_phase_serializes_format(
    mock_dependencies: Dict[str, Any],
    valid_job_id: str,
    valid_user_id: str
) -> None:
    """Verifies _trigger_reduce_phase includes format in serialized TaskMessage."""
    deps = mock_dependencies
    mq_mock = deps["mq_channel"].return_value
    mq_mock.default_exchange.publish = AsyncMock()
    deps["k8s_provider"].return_value.spawn_workers = AsyncMock()
    deps["cache_repo"].get_worker_endpoints = AsyncMock(return_value=["node-a@@10.0.1.45:7337"])

    await TaskEventProcessor._trigger_reduce_phase(
        job_id=valid_job_id,
        user_id=valid_user_id,
        reducer_filename="WordCountReducer.class",
        reducer_class="WordCountReducer",
        num_reducers=1,
        file_format="JSON"
    )

    sent_msg = json.loads(mq_mock.default_exchange.publish.call_args[0][0].body.decode())
    assert sent_msg["format"] == "JSON"