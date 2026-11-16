"""
Unit tests verifying schedule rollback safety, RabbitMQ DLQ binding, and TaskMessage.format propagation.
"""

import json
import uuid
from unittest.mock import AsyncMock, MagicMock

import aio_pika
import pytest
from fastapi import HTTPException

from app.api.routes import handle_schedule_job
from app.core import rabbitmq as rabbitmq_module
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
async def test_schedule_passes_file_format_to_publish_map_tasks(mocker) -> None:
    """Verifies /internal/schedule forwards payload.format to TaskPublisher.publish_map_tasks."""
    req = _make_schedule_request(file_format="JSON")
    user_id = str(uuid.uuid4())
    db_pool = AsyncMock()
    mq_channel = AsyncMock()
    k8s_provider = AsyncMock()
    redis_client = AsyncMock()

    mocker.patch("app.api.routes.CacheRepository.try_acquire_idempotency_lock", AsyncMock(return_value=(True, None)))
    mocker.patch("app.api.routes.CacheRepository.cache_idempotent_response", AsyncMock())
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
    assert publish_mock.call_args.kwargs["file_format"] == "JSON"


@pytest.mark.asyncio
async def test_setup_rabbitmq_binds_dlq_to_fanout_dlx(mocker) -> None:
    """Verifies dead_letter_queue is bound to a FANOUT dead_letter_exchange."""
    dlx_mock = MagicMock()
    dlq_mock = AsyncMock()
    channel_mock = AsyncMock()
    channel_mock.declare_exchange = AsyncMock(return_value=dlx_mock)
    channel_mock.declare_queue = AsyncMock(return_value=dlq_mock)

    conn_mock = AsyncMock()
    conn_mock.channel = AsyncMock(return_value=channel_mock)
    mocker.patch("app.core.rabbitmq.aio_pika.connect_robust", AsyncMock(return_value=conn_mock))

    await rabbitmq_module.setup_rabbitmq()

    channel_mock.declare_exchange.assert_awaited_once_with(
        "dead_letter_exchange",
        aio_pika.ExchangeType.FANOUT,
        durable=True,
    )
    dlq_mock.bind.assert_awaited_once_with(dlx_mock)


def test_task_message_format_default_and_serialization() -> None:
    """Verifies TaskMessage defaults format to TEXT and serializes explicit format."""
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
    )
    assert msg.format == "TEXT"
    assert json.loads(msg.model_dump_json(by_alias=True))["format"] == "TEXT"


@pytest.mark.asyncio
async def test_task_publisher_propagates_file_format(mocker) -> None:
    """Verifies publish_map_tasks and republish_single_map_task include format in TaskMessage."""
    mq_channel = AsyncMock()
    mq_channel.default_exchange.publish = AsyncMock()
    mocker.patch("app.services.task_publisher.orchestrator.generate_chunk_splits", return_value=iter([(0, 100)]))

    await TaskPublisher.publish_map_tasks(
        job_id="job-1",
        total_chunks=1,
        file_size=100,
        s3_input_uri="s3://data/user/file.json",
        mapper_bucket="code",
        mapper_obj="user/M.class",
        mapper_class="M",
        num_reducers=1,
        mq_channel=mq_channel,
        file_format="JSON",
    )
    published_map = json.loads(mq_channel.default_exchange.publish.call_args[0][0].body.decode())
    assert published_map["format"] == "JSON"

    mq_channel.default_exchange.publish.reset_mock()
    mocker.patch("app.services.task_publisher.orchestrator.generate_chunk_splits", return_value=iter([(0, 100)]))

    await TaskPublisher.republish_single_map_task(
        job_id="job-1",
        task_id="map-chunk-0",
        total_chunks=1,
        file_size=100,
        s3_input_uri="s3://data/user/file.json",
        mapper_bucket="code",
        mapper_obj="user/M.class",
        mapper_class="M",
        num_reducers=1,
        mq_channel=mq_channel,
        file_format="JSON",
    )
    republished_map = json.loads(mq_channel.default_exchange.publish.call_args[0][0].body.decode())
    assert republished_map["format"] == "JSON"
