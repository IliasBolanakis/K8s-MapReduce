"""
Unit tests verifying schedule rollback safety, RabbitMQ pool/channel isolation,
structured JSON logging context, O(1) TaskPublisher splits, and TaskMessage contracts.
"""

import json
import logging
import uuid
from unittest.mock import AsyncMock, MagicMock

import aio_pika
import pytest
from fastapi import HTTPException

from app.api.routes import handle_schedule_job
from app.core import rabbitmq as rabbitmq_module
from app.core.logger import (
    JobContextFilter,
    attempt_id_var,
    job_id_var,
    task_id_var,
)
from app.models.enums import TaskType
from app.models.schemas import ScheduleJobRequest, TaskMessage
from app.services.task_publisher import TaskPublisher


def _make_schedule_request(file_format: str = "JSON") -> ScheduleJobRequest:
    return ScheduleJobRequest(
        jobId=uuid.uuid4(),
        format=file_format,
        inputFilename="data.json",
        fileSize=1024,
        mapperFilename="Mapper.class",
        reducerFilename="Reducer.class",
    )


@pytest.mark.asyncio
async def test_schedule_503_marks_submitted_job_failed(mocker) -> None:
    """Verifies capacity saturation (503) marks the SUBMITTED DB row FAILED."""
    req = _make_schedule_request()
    user_id = str(uuid.uuid4())
    db_pool = AsyncMock()
    mq_channel = AsyncMock()
    k8s_provider = AsyncMock()
    redis_client = AsyncMock()

    mocker.patch("app.api.routes.CacheRepository.try_acquire_idempotency_lock", AsyncMock(return_value=(True, None)))
    mocker.patch("app.api.routes.CacheRepository.release_idempotency_lock", AsyncMock())
    mocker.patch("app.api.routes.JobRepository.create_job", AsyncMock())
    mark_failed = mocker.patch("app.api.routes.JobRepository.mark_job_failed", AsyncMock())
    mocker.patch("app.api.routes.CacheRepository.reserve_cluster_capacity", AsyncMock(return_value=0))

    with pytest.raises(HTTPException) as exc_info:
        await handle_schedule_job(
            payload=req,
            idempotency_key="idem-503",
            current_user_id=user_id,
            db_pool=db_pool,
            mq_channel=mq_channel,
            k8s_provider=k8s_provider,
            redis_client=redis_client,
        )

    assert exc_info.value.status_code == 503
    mark_failed.assert_awaited_once_with(str(req.job_id), db_pool)


@pytest.mark.asyncio
async def test_schedule_transition_404_releases_redis_capacity(mocker) -> None:
    """Verifies a 404/exception in transition_job_to_running releases reserved Redis capacity."""
    req = _make_schedule_request()
    user_id = str(uuid.uuid4())
    db_pool = AsyncMock()
    mq_channel = AsyncMock()
    k8s_provider = AsyncMock()
    redis_client = AsyncMock()

    mocker.patch("app.api.routes.CacheRepository.try_acquire_idempotency_lock", AsyncMock(return_value=(True, None)))
    mocker.patch("app.api.routes.CacheRepository.release_idempotency_lock", AsyncMock())
    mocker.patch("app.api.routes.JobRepository.create_job", AsyncMock())
    mocker.patch("app.api.routes.CacheRepository.reserve_cluster_capacity", AsyncMock(return_value=2))
    release_cap = mocker.patch("app.api.routes.CacheRepository.release_job_capacity", AsyncMock())
    mark_failed = mocker.patch("app.api.routes.JobRepository.mark_job_failed", AsyncMock())
    mocker.patch(
        "app.api.routes.JobRepository.transition_job_to_running",
        AsyncMock(side_effect=HTTPException(status_code=404, detail="Not found")),
    )

    with pytest.raises(HTTPException) as exc_info:
        await handle_schedule_job(
            payload=req,
            idempotency_key="idem-404",
            current_user_id=user_id,
            db_pool=db_pool,
            mq_channel=mq_channel,
            k8s_provider=k8s_provider,
            redis_client=redis_client,
        )

    assert exc_info.value.status_code == 404
    release_cap.assert_awaited_once_with(str(req.job_id), redis_client)
    mark_failed.assert_awaited_once_with(str(req.job_id), db_pool)


@pytest.mark.asyncio
async def test_schedule_passes_file_format_and_redis_to_publish_map_tasks(mocker) -> None:
    """Verifies /internal/schedule caches num_reducers and forwards format & redis_client to publish_map_tasks."""
    req = _make_schedule_request(file_format="JSON")
    user_id = str(uuid.uuid4())
    db_pool = AsyncMock()
    mq_channel = AsyncMock()
    k8s_provider = AsyncMock()
    redis_client = AsyncMock()

    mocker.patch("app.api.routes.CacheRepository.try_acquire_idempotency_lock", AsyncMock(return_value=(True, None)))
    mocker.patch("app.api.routes.CacheRepository.cache_idempotent_response", AsyncMock())
    cache_reducers = mocker.patch("app.api.routes.CacheRepository.cache_num_reducers", AsyncMock())
    mocker.patch("app.api.routes.JobRepository.create_job", AsyncMock())
    mocker.patch("app.api.routes.CacheRepository.reserve_cluster_capacity", AsyncMock(return_value=1))
    mocker.patch("app.api.routes.JobRepository.transition_job_to_running", AsyncMock())
    publish_mock = mocker.patch("app.api.routes.TaskPublisher.publish_map_tasks", AsyncMock())

    resp = await handle_schedule_job(
        payload=req,
        idempotency_key="idem-ok",
        current_user_id=user_id,
        db_pool=db_pool,
        mq_channel=mq_channel,
        k8s_provider=k8s_provider,
        redis_client=redis_client,
    )

    assert resp["status"] == "SUCCESS"
    cache_reducers.assert_awaited_once()
    assert publish_mock.call_args.kwargs["file_format"] == "JSON"
    assert publish_mock.call_args.kwargs["redis_client"] is redis_client


@pytest.mark.asyncio
async def test_setup_rabbitmq_binds_dlq_isolates_consumers_and_pools_channels(mocker) -> None:
    """Verifies ADR-13 setup creates 2 isolated consumer channels, a bounded pool, and binds DLQ."""
    mocker.patch("app.core.rabbitmq.settings.RABBITMQ_CHANNEL_POOL_SIZE", 4)
    dlx_mock = MagicMock()
    dlq_mock = AsyncMock()

    created_channels = []

    async def _make_channel():
        ch = AsyncMock()
        ch.is_closed = False
        ch.declare_exchange = AsyncMock(return_value=dlx_mock)
        ch.declare_queue = AsyncMock(return_value=dlq_mock)
        created_channels.append(ch)
        return ch

    conn_mock = AsyncMock()
    conn_mock.is_closed = False
    conn_mock.channel = AsyncMock(side_effect=_make_channel)
    mocker.patch("app.core.rabbitmq.aio_pika.connect_robust", AsyncMock(return_value=conn_mock))

    await rabbitmq_module.setup_rabbitmq()

    # 2 consumer channels + 4 pool channels = 6 total channels opened
    assert len(created_channels) == 6
    job_ch = await rabbitmq_module.get_job_events_channel()
    orch_ch = await rabbitmq_module.get_orchestration_events_channel()
    assert job_ch is not orch_ch

    created_channels[2].declare_exchange.assert_awaited_once_with(
        "dead_letter_exchange",
        aio_pika.ExchangeType.FANOUT,
        durable=True,
    )
    dlq_mock.bind.assert_awaited_once_with(dlx_mock)

    await rabbitmq_module.teardown_rabbitmq()


@pytest.mark.asyncio
async def test_rabbitmq_channel_pool_replaces_closed_channels_on_lease_and_return(mocker) -> None:
    """Verifies ADR-13 pool automatically replaces closed channels on lease and return."""
    mocker.patch("app.core.rabbitmq.settings.RABBITMQ_CHANNEL_POOL_SIZE", 2)
    dlx_mock = MagicMock()
    dlq_mock = AsyncMock()

    channels = []

    async def _make_channel():
        ch = AsyncMock()
        ch.is_closed = False
        ch.declare_exchange = AsyncMock(return_value=dlx_mock)
        ch.declare_queue = AsyncMock(return_value=dlq_mock)
        channels.append(ch)
        return ch

    conn_mock = AsyncMock()
    conn_mock.is_closed = False
    conn_mock.channel = AsyncMock(side_effect=_make_channel)
    mocker.patch("app.core.rabbitmq.aio_pika.connect_robust", AsyncMock(return_value=conn_mock))

    await rabbitmq_module.setup_rabbitmq()
    # Pool channels are indices 2 and 3; simulate index 2 died before lease
    channels[2].is_closed = True

    async with rabbitmq_module.acquire_rabbitmq_channel() as leased:
        # Replacement channel (index 4) should be opened and leased
        assert leased is channels[4]
        assert leased.is_closed is False
        # Simulate channel dying while in use before return
        leased.is_closed = True

    # On return, index 5 should have been opened to keep pool healthy
    assert len(channels) == 6
    assert channels[5].is_closed is False

    await rabbitmq_module.teardown_rabbitmq()


def test_task_message_format_and_attempt_id_serialization() -> None:
    """Verifies TaskMessage defaults format='TEXT', attemptId=0 and serializes camelCase attemptId."""
    msg = TaskMessage(
        task_id="map-chunk-0",
        job_id="job-1",
        job_token="tok",
        task_type=TaskType.MAP,
        bucket_name="data",
        object_name="u/f.txt",
        byte_offset=0,
        byte_length=10,
        num_reducers=1,
        user_code_bucket="code",
        user_code_object="u/M.class",
        class_name="M",
        attempt_id=3,
    )
    assert msg.format == "TEXT"
    dumped = json.loads(msg.model_dump_json(by_alias=True))
    assert dumped["format"] == "TEXT"
    assert dumped["attemptId"] == 3


@pytest.mark.asyncio
async def test_task_publisher_propagates_format_attempt_id_and_o1_split(mocker) -> None:
    """Verifies publish_map_tasks and O(1) republish_single_map_task mint attempts and propagate format."""
    mq_channel = AsyncMock()
    mq_channel.default_exchange.publish = AsyncMock()
    redis_client = AsyncMock()
    pipe_mock = MagicMock()
    pipe_mock.__aenter__ = AsyncMock(return_value=pipe_mock)
    pipe_mock.__aexit__ = AsyncMock(return_value=None)
    pipe_mock.execute = AsyncMock(return_value=[1, True])
    redis_client.pipeline = MagicMock(return_value=pipe_mock)

    mocker.patch("app.services.task_publisher.orchestrator.settings.CHUNK_SIZE_BYTES", 60)

    await TaskPublisher.publish_map_tasks(
        job_id="job-1",
        total_chunks=2,
        file_size=100,
        s3_input_uri="s3://data/user/file.json",
        mapper_bucket="code",
        mapper_obj="user/M.class",
        mapper_class="M",
        num_reducers=1,
        mq_channel=mq_channel,
        file_format="JSON",
        redis_client=redis_client,
    )
    published_map = json.loads(mq_channel.default_exchange.publish.call_args_list[0][0][0].body.decode())
    assert published_map["format"] == "JSON"
    assert published_map["attemptId"] == 1
    assert published_map["byteOffset"] == 0
    assert published_map["byteLength"] == 60

    mq_channel.default_exchange.publish.reset_mock()
    pipe_mock.execute.return_value = [2, True]

    await TaskPublisher.republish_single_map_task(
        job_id="job-1",
        task_id="map-chunk-1",
        total_chunks=2,
        file_size=100,
        s3_input_uri="s3://data/user/file.json",
        mapper_bucket="code",
        mapper_obj="user/M.class",
        mapper_class="M",
        num_reducers=1,
        mq_channel=mq_channel,
        file_format="JSON",
        redis_client=redis_client,
    )
    republished_map = json.loads(mq_channel.default_exchange.publish.call_args[0][0].body.decode())
    assert republished_map["format"] == "JSON"
    assert republished_map["attemptId"] == 2
    assert republished_map["byteOffset"] == 60
    assert republished_map["byteLength"] == 40


def test_job_context_filter_injects_job_task_and_attempt_ids() -> None:
    """Verifies JobContextFilter populates job_id, task_id, and attempt_id from ContextVars."""
    t1 = job_id_var.set("job-ctx-1")
    t2 = task_id_var.set("map-chunk-7")
    t3 = attempt_id_var.set(4)
    try:
        record = logging.LogRecord("test", logging.INFO, __file__, 1, "hello", (), None)
        assert JobContextFilter().filter(record) is True
        assert record.job_id == "job-ctx-1"  # type: ignore[attr-defined]
        assert record.task_id == "map-chunk-7"  # type: ignore[attr-defined]
        assert record.attempt_id == 4  # type: ignore[attr-defined]
    finally:
        attempt_id_var.reset(t3)
        task_id_var.reset(t2)
        job_id_var.reset(t1)
