"""
Asynchronous worker event consumer module for MapReduce orchestration.

This module provides the core event-driven logic for tracking MapReduce job
progress and orchestrating phase transitions. It consumes completion/failure
signals from RabbitMQ on isolated consumer channels and manages execution
barriers between the Map and Reduce phases.

Architecture Upgrades:
    - **ADR-04 (Indexed Reduce Jobs & Per-Partition Queues):** Publishes each
      Reduce partition to ``reduce_tasks_{job_id}_{p_id}`` before spawning a
      single Kubernetes ``Indexed`` Batch Job, while retaining targeted
      non-indexed single-partition recovery jobs with ``PARTITION_ID`` injection.
    - **ADR-05 (Atomic Capacity Swap):** Replaces split release/reserve calls
      with ``CacheRepository.swap_map_to_reduce_capacity`` and the
      ``cluster:reduce_waiting`` watermark.
    - **ADR-06 (Fenced Attempt IDs & Output Promotion):** Validates incoming
      ``WorkerSignal.attempt_id`` against Redis ``job:{id}:attempts`` to drop
      stale zombie signals, and promotes ``_attempts/part_{p}_attempt_{a}.staging``
      Reduce outputs atomically before advancing the final Reduce barrier.
    - **ADR-13 (AMQP Channel Isolation & Pool):** Isolates ``job_events_queue``
      and ``orchestration_events_queue`` onto dedicated consumer channels and
      leases publisher channels via ``acquire_rabbitmq_channel()``.
"""

import asyncio
import hashlib
import hmac
import logging
from typing import List, Optional

import aio_pika
from aio_pika.abc import AbstractIncomingMessage
from pydantic import ValidationError

from app.core.config import settings
from app.core.database import get_db_pool
from app.core.logger import attempt_id_var, job_id_var, task_id_var
from app.core.rabbitmq import (
    acquire_rabbitmq_channel,
    get_job_events_channel,
    get_orchestration_events_channel,
)
from app.core.redis import get_redis_client
from app.k8s.client import get_k8s_provider
from app.models.enums import TaskType
from app.models.schemas import TaskMessage, WorkerSignal
from app.repositories.cache_repository import CacheRepository
from app.repositories.job_repository import JobRepository
from app.services.lifecycle import forcefully_reclaim_job
from app.services.storage import StorageService
from app.services.task_publisher import TaskPublisher

logger = logging.getLogger(__name__)


class TaskEventProcessor:
    """
    Handles the ingestion and routing of execution signals from distributed workers.

    Provides asynchronous class methods to process RabbitMQ telemetry and
    orchestration messages, interfacing with Kubernetes, PostgreSQL, Redis,
    and MinIO to advance the global job state machine safely.
    """

    @classmethod
    async def _ping_db_throttled(cls, job_id_str: str) -> None:
        """
        Throttles PostgreSQL activity updates using a Redis debounce lock.

        Employs a 30-second distributed lock so high-throughput worker heartbeats
        and task completions keep the Watchdog satisfied without saturating DB IOPS.

        :param job_id_str: Target job UUID string.
        :type job_id_str: str
        """
        redis_client = await get_redis_client()
        lock_key = f"job:{job_id_str}:db_ping_throttle"

        if await redis_client.set(lock_key, "1", nx=True, ex=30):
            db_pool = await get_db_pool()
            await JobRepository.ping_job_activity(job_id_str, db_pool)

    @classmethod
    async def process_message(cls, message: AbstractIncomingMessage) -> None:
        """
        Main entry point for processing worker execution signals on ``job_events_queue``.

        Validates the JSON DTO, enforces ``attempt_id`` zombie fencing (ADR-06),
        routes ``IN_PROGRESS`` heartbeats, executes lineage recovery or bounded
        retries on ``FAILED`` signals, and advances Map/Reduce completion barriers.

        :param message: Incoming AMQP message from ``job_events_queue``.
        :type message: AbstractIncomingMessage
        """
        async with message.process(ignore_processed=True):
            try:
                raw_data = message.body.decode("utf-8")
                payload = WorkerSignal.model_validate_json(raw_data)
            except (UnicodeDecodeError, ValidationError) as e:
                logger.error("Failed to decode worker completion message: %s", str(e))
                await message.reject(requeue=False)
                return

            job_id_str = str(payload.job_id)
            job_tok = job_id_var.set(job_id_str)
            task_tok = task_id_var.set(payload.task_id)
            attempt_tok = attempt_id_var.set(payload.attempt_id)

            try:
                logger.info(
                    "Received signal for task %s (Status: %s, attempt_id=%d)",
                    payload.task_id,
                    payload.status,
                    payload.attempt_id,
                )

                redis_client = await get_redis_client()

                # --- ADR-06: Stale Zombie Signal Fencing ---
                expected_attempt = await CacheRepository.get_expected_task_attempt(
                    job_id_str, payload.task_id, redis_client
                )
                if expected_attempt is not None and expected_attempt != payload.attempt_id:
                    logger.warning(
                        "Fencing: Dropping stale zombie signal for task %s (attempt_id=%d, expected_attempt_id=%d).",
                        payload.task_id,
                        payload.attempt_id,
                        expected_attempt,
                        extra={
                            "job_id": job_id_str,
                            "task_id": payload.task_id,
                            "attempt_id": payload.attempt_id,
                            "expected_attempt_id": expected_attempt,
                        },
                    )
                    return

                if expected_attempt is None and payload.attempt_id != 0:
                    logger.warning(
                        "Fencing: Dropping stale signal for cleaned/untracked task %s "
                        "(attempt_id=%d, expected_attempt_id=None).",
                        payload.task_id,
                        payload.attempt_id,
                        extra={
                            "job_id": job_id_str,
                            "task_id": payload.task_id,
                            "attempt_id": payload.attempt_id,
                            "expected_attempt_id": None,
                        },
                    )
                    return

                # --- Long-Running Task Heartbeats ---
                if payload.status == "IN_PROGRESS":
                    await cls._ping_db_throttled(job_id_str)
                    return

                # --- Error Handling & Hybrid Retry Routing ---
                if payload.status != "COMPLETED":
                    # 1. Lineage Recovery (Surgical Shuffle Healing)
                    if payload.error and (
                        payload.error.startswith("SHUFFLE_FETCH_FAILED:")
                        or payload.error.startswith("SHUFFLE_FETCH_FAILED_FROM_NODE:")
                    ):
                        if payload.error.startswith("SHUFFLE_FETCH_FAILED_FROM_NODE:"):
                            failed_endpoint = payload.error.split(":", 1)[1]
                            lost_task_ids = await CacheRepository.get_tasks_by_endpoint(
                                job_id_str, failed_endpoint, redis_client
                            )
                            await CacheRepository.evict_ess_endpoint(
                                job_id_str, failed_endpoint, redis_client
                            )
                            logger.warning(
                                "Node-level Shuffle Failure from '%s'. Affected map tasks: %s",
                                failed_endpoint,
                                lost_task_ids,
                            )
                        else:
                            lost_task_id = payload.error.split(":", 1)[1]
                            lost_task_ids = [lost_task_id] if lost_task_id else []

                        if lost_task_ids:
                            for lost_task_id in lost_task_ids:
                                logger.warning("Reactive Lineage Recovery Triggered for '%s'.", lost_task_id)

                                recovery_strikes = await CacheRepository.increment_task_failure(
                                    job_id_str, f"recovery_attempts_{lost_task_id}", redis_client
                                )

                                if recovery_strikes >= 3:
                                    logger.error(
                                        "Poison Pill Triggered: Lineage Recovery failed 3 times for %s.",
                                        lost_task_id,
                                    )
                                    db_pool = await get_db_pool()
                                    k8s_provider = await get_k8s_provider()
                                    await forcefully_reclaim_job(job_id_str, db_pool, k8s_provider, redis_client)
                                    await message.reject(requeue=False)
                                    return

                                await CacheRepository.add_to_recovery_waitlist(
                                    job_id_str, lost_task_id, payload.task_id, redis_client
                                )

                                recovery_lock_key = f"job:{job_id_str}:recovering:{lost_task_id}"
                                if await redis_client.set(recovery_lock_key, "LOCKED", nx=True, ex=300):
                                    await cls._recover_lost_map_task(job_id_str, lost_task_id)
                                else:
                                    logger.debug(
                                        "Recovery already in progress for %s. Skipping duplicate spawn.",
                                        lost_task_id,
                                    )

                            return

                        logger.warning(
                            "No map tasks found for shuffle failure '%s' in job %s; falling back to task retry.",
                            payload.error,
                            job_id_str,
                        )

                    # 2. Smart Retry Loop: Detect Deterministic vs. Transient Errors
                    deterministic_errors = [
                        "NullPointerException",
                        "ClassCastException",
                        "OutOfMemoryError",
                        "Exception in User Code",
                    ]
                    is_user_bug = any(err in (payload.error or "") for err in deterministic_errors)

                    if is_user_bug:
                        logger.error("Fail-Fast Triggered: Deterministic user error. Skipping retries.")
                        strikes = 3
                    else:
                        strikes = await CacheRepository.increment_task_failure(
                            job_id_str, payload.task_id, redis_client
                        )

                    if strikes < 3:
                        logger.warning(
                            "Task %s failed (Strike %d/3). Error: %s. Provisioning replacement compute.",
                            payload.task_id,
                            strikes,
                            payload.error,
                        )
                        if "map" in payload.task_id.lower():
                            await cls._recover_lost_map_task(job_id_str, payload.task_id)
                        elif payload.task_id.isdigit():
                            db_pool = await get_db_pool()
                            metadata = await JobRepository.get_job_metadata(job_id_str, db_pool)
                            if not metadata:
                                logger.error("Task retry aborted: Job metadata not found for %s", job_id_str)
                                return
                            derived_reducer_class = metadata["reducer_filename"].rsplit(".class", 1)[0]
                            await cls._trigger_reduce_phase(
                                job_id=job_id_str,
                                user_id=str(metadata["user_id"]),
                                reducer_filename=metadata["reducer_filename"],
                                reducer_class=derived_reducer_class,
                                num_reducers=metadata["num_reducers"],
                                target_partition_id=int(payload.task_id),
                                file_format=metadata.get("format", "TEXT"),
                            )
                        else:
                            k8s_provider = await get_k8s_provider()
                            await k8s_provider.spawn_workers(job_id=job_id_str, replicas=1, phase="reduce")
                        return

                    # 3. Poison Pill Teardown (Strike 3 or User Code Bug)
                    logger.error(
                        "Poison Pill Triggered. Task %s failed terminally. Error: %s",
                        payload.task_id,
                        payload.error,
                    )
                    db_pool = await get_db_pool()
                    k8s_provider = await get_k8s_provider()
                    await forcefully_reclaim_job(job_id_str, db_pool, k8s_provider, redis_client)
                    await message.reject(requeue=False)
                    return

                # --- Standard Completion Pipeline ---
                await cls._ping_db_throttled(job_id_str)

                if "map" in payload.task_id.lower():
                    await cls._handle_map_completion(payload)
                elif "reduce" in payload.task_id.lower() or payload.task_id.isdigit():
                    await cls._handle_reduce_completion(payload)
                else:
                    logger.error("Unable to route signal: unknown task_id format '%s'", payload.task_id)
            finally:
                attempt_id_var.reset(attempt_tok)
                task_id_var.reset(task_tok)
                job_id_var.reset(job_tok)

    @classmethod
    async def _recover_lost_map_task(cls, job_id: str, lost_task_id: str) -> None:
        """
        Reconstructs execution metadata to re-run a specific lost Map task.

        :param job_id: UUID of the parent MapReduce job.
        :type job_id: str
        :param lost_task_id: Identifier of the Map chunk to recompute (e.g., ``map-chunk-5``).
        :type lost_task_id: str
        """
        logger.info("Initiating Lineage Recovery sequence for %s.", lost_task_id)
        db_pool = await get_db_pool()
        k8s_provider = await get_k8s_provider()
        redis_client = await get_redis_client()

        record = await JobRepository.get_job_recovery_context(job_id, db_pool)
        if not record:
            logger.error("Lineage Recovery failed: Job context not found for %s", job_id)
            return

        try:
            user_id_str = str(record["user_id"])

            data_bucket = "data"
            data_obj = f"{user_id_str}/{record['input_filename']}"
            minio_client = StorageService.get_client()
            file_size = minio_client.stat_object(data_bucket, data_obj).size

            mapper_bucket = "code"
            mapper_obj = f"{user_id_str}/{record['mapper_filename']}"
            constructed_input_uri = f"s3://{data_bucket}/{data_obj}"

            derived_mapper_class = record["mapper_filename"].rsplit(".class", 1)[0]
            combiner_filename = record.get("combiner_filename")
            combiner_obj = f"{user_id_str}/{combiner_filename}" if combiner_filename else None
            derived_combiner_class = combiner_filename.rsplit(".class", 1)[0] if combiner_filename else None
            file_format = record.get("format") or "TEXT"

            logger.info("Re-provisioning 1 Map worker for lineage recovery.")
            await k8s_provider.spawn_workers(
                job_id=job_id,
                replicas=1,
                phase="map",
            )

            async with acquire_rabbitmq_channel() as mq_channel:
                await TaskPublisher.republish_single_map_task(
                    job_id=job_id,
                    task_id=lost_task_id,
                    total_chunks=record["total_chunks"],
                    file_size=file_size,  # type: ignore[arg-type]
                    s3_input_uri=constructed_input_uri,
                    mapper_bucket=mapper_bucket,
                    mapper_obj=mapper_obj,
                    mapper_class=derived_mapper_class,
                    num_reducers=record["num_reducers"],
                    mq_channel=mq_channel,
                    combiner_obj=combiner_obj,
                    combiner_class=derived_combiner_class,
                    file_format=file_format,
                    redis_client=redis_client,
                )
        except Exception as e:
            logger.error("Failed to execute lineage recovery: %s", str(e))
            await forcefully_reclaim_job(job_id, db_pool, k8s_provider, redis_client)

    @classmethod
    async def process_orchestration_event(cls, message: AbstractIncomingMessage) -> None:
        """
        Processes internal transition events on ``orchestration_events_queue``.

        Tears down Map-phase Kubernetes resources idempotently and executes an
        atomic Map-to-Reduce capacity swap via ``CacheRepository.swap_map_to_reduce_capacity``
        (ADR-05). If cluster capacity is temporarily saturated, registers the job's
        demand in ``cluster:reduce_waiting`` and retries cleanly with a 2s backoff.

        :param message: Incoming AMQP message containing the job UUID.
        :type message: AbstractIncomingMessage
        """
        async with message.process(ignore_processed=True):
            try:
                job_id = message.body.decode("utf-8")
            except Exception:
                await message.reject(requeue=False)
                return

            job_tok = job_id_var.set(job_id)
            task_tok = task_id_var.set("-")
            attempt_tok = attempt_id_var.set(0)

            try:
                db_pool = await get_db_pool()
                metadata = await JobRepository.get_job_metadata(job_id, db_pool)

                if not metadata:
                    await message.reject(requeue=False)
                    return

                if metadata.get("status") in ("FAILED", "COMPLETED"):
                    logger.info(
                        "Skipping orchestration transition for terminal job %s (%s).",
                        job_id,
                        metadata.get("status"),
                    )
                    return

                k8s_provider = await get_k8s_provider()
                redis_client = await get_redis_client()

                # Step 1: Idempotent Map-phase physical cleanup (preserves reduce queues)
                cleanup_lock_key = f"job:{job_id}:map_cleanup_done"
                if await redis_client.set(cleanup_lock_key, "1", nx=True, ex=86400):
                    logger.info("Map phase complete. Actively tearing down Map pods before capacity swap.")
                    await k8s_provider.delete_job(job_id, phase="map")
                else:
                    logger.debug("Map phase cleanup already performed for job %s. Skipping duplicate teardown.", job_id)

                # Step 2: Atomic Map -> Reduce capacity swap with reduce_waiting watermark (ADR-05)
                target_reducers = metadata["num_reducers"]
                granted_capacity = await CacheRepository.swap_map_to_reduce_capacity(
                    job_id=job_id,
                    target_reducers=target_reducers,
                    max_capacity=settings.MAX_TOTAL_WORKERS,
                    redis_client=redis_client,
                )

                if granted_capacity < target_reducers:
                    logger.warning(
                        "Capacity Starvation: Could not reserve %d Reduce slots for job %s "
                        "(registered in reduce_waiting). Delaying transition retry.",
                        target_reducers,
                        job_id,
                    )
                    await asyncio.sleep(2)
                    async with acquire_rabbitmq_channel() as mq_channel:
                        await mq_channel.default_exchange.publish(
                            aio_pika.Message(
                                body=job_id.encode("utf-8"),
                                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                            ),
                            routing_key="orchestration_events_queue",
                        )
                    return

                derived_reducer_class = metadata["reducer_filename"].rsplit(".class", 1)[0]

                await cls._trigger_reduce_phase(
                    job_id=job_id,
                    user_id=str(metadata["user_id"]),
                    reducer_filename=metadata["reducer_filename"],
                    reducer_class=derived_reducer_class,
                    num_reducers=target_reducers,
                    file_format=metadata.get("format", "TEXT"),
                )
            finally:
                attempt_id_var.reset(attempt_tok)
                task_id_var.reset(task_tok)
                job_id_var.reset(job_tok)

    @classmethod
    async def _handle_map_completion(cls, payload: WorkerSignal) -> None:
        """
        Manages the synchronization barrier for the Map phase.

        Records worker endpoints into Redis SET + reverse node index (ADR-12),
        wakes any Reducers waiting on lineage recovery, and triggers the Reduce
        phase orchestration event once the Map barrier is breached.

        :param payload: Validated Map task completion signal.
        :type payload: WorkerSignal
        """
        redis_client = await get_redis_client()
        db_pool = await get_db_pool()
        job_id_str = str(payload.job_id)

        total = await CacheRepository.get_cached_total_chunks(job_id_str, redis_client)
        if total is None:
            metadata = await JobRepository.get_job_metadata(job_id_str, db_pool)
            if not metadata:
                return
            total = metadata["total_chunks"]
            await CacheRepository.cache_total_chunks(job_id_str, total, redis_client)

        if payload.worker_bind_address:
            await CacheRepository.add_worker_endpoint(
                job_id=job_id_str,
                task_id=payload.task_id,
                endpoint=payload.worker_bind_address,
                redis_client=redis_client,
            )

        waiting_reducer_ids: List[str] = await CacheRepository.pop_from_recovery_waitlist(
            job_id_str, payload.task_id, redis_client
        )

        if waiting_reducer_ids:
            await redis_client.delete(f"job:{job_id_str}:recovering:{payload.task_id}")
            logger.info(
                "Lineage Recovery successful for %s. Waking up Reducers: %s",
                payload.task_id,
                waiting_reducer_ids,
            )
            metadata = await JobRepository.get_job_metadata(job_id_str, db_pool)
            if not metadata:
                return

            derived_reducer_class = metadata["reducer_filename"].rsplit(".class", 1)[0]

            for reducer_id in waiting_reducer_ids:
                await cls._trigger_reduce_phase(
                    job_id=job_id_str,
                    user_id=str(metadata["user_id"]),
                    reducer_filename=metadata["reducer_filename"],
                    reducer_class=derived_reducer_class,
                    num_reducers=metadata["num_reducers"],
                    target_partition_id=int(reducer_id),
                    file_format=metadata.get("format", "TEXT"),
                )
            return

        is_barrier_breached = await CacheRepository.register_map_completion_atomic(
            job_id_str, payload.task_id, total, redis_client
        )

        if is_barrier_breached:
            transition_lock_key = f"job:{job_id_str}:reduce_transition_fired"
            if not await redis_client.set(transition_lock_key, "1", nx=True, ex=86400):
                return

            logger.info("MAP BARRIER REACHED (Atomic Lock Acquired). Publishing transition event.")
            await JobRepository.flush_map_phase(job_id_str, total, db_pool)
            async with acquire_rabbitmq_channel() as mq_channel:
                await mq_channel.default_exchange.publish(
                    aio_pika.Message(
                        body=job_id_str.encode("utf-8"),
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    ),
                    routing_key="orchestration_events_queue",
                )

    @classmethod
    async def _trigger_reduce_phase(
        cls,
        job_id: str,
        user_id: str,
        reducer_filename: str,
        reducer_class: str,
        num_reducers: int,
        target_partition_id: Optional[int] = None,
        file_format: str = "TEXT",
    ) -> None:
        """
        Orchestrates Reduce phase queue publishing and Kubernetes Job provisioning (ADR-04 & ADR-06).

        For a normal Reduce phase (``target_partition_id is None``):
            1. Mints monotonic ``attempt_id``s for partitions ``0..num_reducers-1``.
            2. Declares & publishes each partition's ``TaskMessage`` to its deterministic
               lazy queue ``reduce_tasks_{job_id}_{p_id}`` **before** spawning pods.
            3. Spawns **1** Kubernetes ``Indexed`` Batch Job (`replicas=num_reducers`,
               `indexed=True`).

        For targeted single-partition recovery (``target_partition_id is not None``):
            1. Mints a new monotonic ``attempt_id`` for ``str(target_partition_id)``.
            2. Publishes the recovery ``TaskMessage`` to ``reduce_tasks_{job_id}_{p_id}``.
            3. Spawns **1** non-indexed Job (`replicas=1, partition_id=p_id, indexed=False`)
               with data-locality affinity.

        :param job_id: Parent job UUID.
        :type job_id: str
        :param user_id: Tenant UUID owning input/code/results prefixes.
        :type user_id: str
        :param reducer_filename: Compiled Reducer ``.class`` filename.
        :type reducer_filename: str
        :param reducer_class: Derived Java Reducer class name.
        :type reducer_class: str
        :param num_reducers: Total number of Reduce partitions.
        :type num_reducers: int
        :param target_partition_id: Specific partition ID for recovery, or None for full phase.
        :type target_partition_id: Optional[int]
        :param file_format: Input data format propagated on ``TaskMessage``.
        :type file_format: str
        """
        k8s_provider = await get_k8s_provider()
        redis_client = await get_redis_client()

        raw_endpoints = await CacheRepository.get_worker_endpoints(job_id, redis_client)

        unique_ess_endpoints = list(set(raw_endpoints))
        available_nodes: List[str] = []
        clean_grpc_endpoints: List[str] = []

        for ep in unique_ess_endpoints:
            if "@@" in ep:
                node_name, ip_port = ep.split("@@", 1)
                available_nodes.append(node_name)
                clean_grpc_endpoints.append(ip_port)
            else:
                clean_grpc_endpoints.append(ep)

        logger.info("Extracted %d physical Node Hostnames for Data Locality Scheduling.", len(available_nodes))

        code_bucket = "code"
        code_obj = f"{user_id}/{reducer_filename}"
        class_name = reducer_class if reducer_class else "WordCountReducer"

        secure_job_token = hmac.new(
            getattr(settings, "ESS_SECRET_KEY", "dev-insecure-shared-secret").encode("utf-8"),
            job_id.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        queue_args = {
            "x-dead-letter-exchange": "dead_letter_exchange",
            "x-queue-mode": "lazy",
            "x-expires": 86400000,
        }

        try:
            await CacheRepository.cache_num_reducers(job_id, num_reducers, redis_client)

            if target_partition_id is not None:
                p_id = target_partition_id
                target_node = available_nodes[p_id % len(available_nodes)] if available_nodes else None
                attempt_id = await CacheRepository.mint_task_attempt(job_id, str(p_id), redis_client)
                partition_queue = f"reduce_tasks_{job_id}_{p_id}"

                reduce_msg = TaskMessage(
                    task_id=str(p_id),
                    job_id=job_id,
                    job_token=secure_job_token,
                    task_type=TaskType.REDUCE,
                    format=file_format,
                    bucket_name="results",
                    object_name=f"{user_id}/{job_id}/",
                    byte_offset=0,
                    byte_length=0,
                    num_reducers=num_reducers,
                    user_code_bucket=code_bucket,
                    user_code_object=code_obj,
                    class_name=class_name,
                    worker_endpoints=clean_grpc_endpoints,
                    attempt_id=attempt_id,
                )

                async with acquire_rabbitmq_channel() as mq_channel:
                    await mq_channel.declare_queue(partition_queue, durable=True, arguments=queue_args)
                    await mq_channel.default_exchange.publish(
                        aio_pika.Message(
                            body=reduce_msg.model_dump_json(by_alias=True).encode(),
                            delivery_mode=aio_pika.DeliveryMode.NOT_PERSISTENT,
                        ),
                        routing_key=partition_queue,
                    )

                await k8s_provider.spawn_workers(
                    job_id=job_id,
                    replicas=1,
                    phase="reduce",
                    partition_id=p_id,
                    target_node=target_node,
                    indexed=False,
                )
                logger.info(
                    "Successfully provisioned targeted Reducer partition %d (attempt_id=%d) for recovery.",
                    p_id,
                    attempt_id,
                )
            else:
                partition_ids = [str(p_id) for p_id in range(num_reducers)]
                attempt_ids = await CacheRepository.mint_task_attempts_batch(
                    job_id, partition_ids, redis_client
                )

                async def _declare_and_publish_partition(p_id: int, attempt_id: int) -> None:
                    partition_queue = f"reduce_tasks_{job_id}_{p_id}"
                    reduce_msg = TaskMessage(
                        task_id=str(p_id),
                        job_id=job_id,
                        job_token=secure_job_token,
                        task_type=TaskType.REDUCE,
                        format=file_format,
                        bucket_name="results",
                        object_name=f"{user_id}/{job_id}/",
                        byte_offset=0,
                        byte_length=0,
                        num_reducers=num_reducers,
                        user_code_bucket=code_bucket,
                        user_code_object=code_obj,
                        class_name=class_name,
                        worker_endpoints=clean_grpc_endpoints,
                        attempt_id=attempt_id,
                    )
                    msg_bytes = reduce_msg.model_dump_json(by_alias=True).encode()
                    async with acquire_rabbitmq_channel() as mq_channel:
                        await mq_channel.declare_queue(partition_queue, durable=True, arguments=queue_args)
                        await mq_channel.default_exchange.publish(
                            aio_pika.Message(
                                body=msg_bytes,
                                delivery_mode=aio_pika.DeliveryMode.NOT_PERSISTENT,
                            ),
                            routing_key=partition_queue,
                        )

                await asyncio.gather(
                    *[
                        _declare_and_publish_partition(p_id, attempt_id)
                        for p_id, attempt_id in zip(range(num_reducers), attempt_ids)
                    ]
                )

                await k8s_provider.spawn_workers(
                    job_id=job_id,
                    replicas=num_reducers,
                    phase="reduce",
                    indexed=True,
                )
                logger.info(
                    "Successfully published %d partition queues and spawned 1 Indexed Reduce Job.",
                    num_reducers,
                )

        except Exception as publish_error:
            logger.error("Saga Triggered: Reduce provisioning failed. Tearing down pods.")
            db_pool = await get_db_pool()
            await forcefully_reclaim_job(job_id, db_pool, k8s_provider, redis_client)
            raise publish_error

    @classmethod
    async def _handle_reduce_completion(cls, payload: WorkerSignal) -> None:
        """
        Manages the final synchronization barrier and fenced output promotion for Reduce (ADR-06).

        Promotes ``{user_id}/{job_id}/_attempts/part_{task_id}_attempt_{attempt_id}.staging``
        to ``result_part_{task_id}.txt`` when ``attempt_id > 0`` prior to advancing the
        atomic Reduce barrier. When all partitions finish, cleans up K8s Jobs, AMQP queues,
        leftover ``_attempts/`` staging objects, Redis capacity permits, and job metadata.

        :param payload: Validated Reduce task completion signal.
        :type payload: WorkerSignal
        """
        redis_client = await get_redis_client()
        db_pool = await get_db_pool()
        k8s_provider = await get_k8s_provider()
        job_id_str = str(payload.job_id)

        metadata = await JobRepository.get_job_metadata(job_id_str, db_pool)
        if not metadata:
            return
        user_id_str = str(metadata["user_id"])
        target_reducers = metadata["num_reducers"]

        # ADR-06: Promote fenced staging output to canonical result file before barrier advancement
        if payload.attempt_id > 0:
            await asyncio.to_thread(
                StorageService.promote_staged_reduce_output,
                user_id_str,
                job_id_str,
                payload.task_id,
                payload.attempt_id,
            )

        is_barrier_breached = await CacheRepository.register_reduce_completion_atomic(
            job_id_str, payload.task_id, target_reducers, redis_client
        )

        if is_barrier_breached:
            completion_lock_key = f"job:{job_id_str}:reduce_completion_fired"
            if not await redis_client.set(completion_lock_key, "1", nx=True, ex=86400):
                logger.debug("Reduce completion barrier already breached. Skipping duplicate teardown.")
                return

            logger.info("FINAL BARRIER REACHED. MapReduce job execution successful.")
            await JobRepository.flush_reduce_phase(job_id_str, target_reducers, db_pool)
            await JobRepository.mark_job_completed(job_id_str, db_pool)

            try:
                await k8s_provider.wait_for_job_completion(job_id_str)
                await k8s_provider.delete_job(job_id_str, num_reducers=target_reducers)
                try:
                    await asyncio.to_thread(
                        StorageService.cleanup_job_attempts_prefix,
                        user_id_str,
                        job_id_str,
                    )
                except Exception as s3_cleanup_err:
                    logger.warning(
                        "Non-fatal cleanup error removing _attempts prefix for job %s: %s",
                        job_id_str,
                        str(s3_cleanup_err),
                    )
            finally:
                await CacheRepository.release_job_capacity(job_id_str, redis_client)
                await CacheRepository.cleanup_job_metadata(job_id_str, redis_client)


async def start_consumer() -> None:
    """
    Bootstraps RabbitMQ event listeners on isolated AMQP channels (ADR-13).

    Establishes independent consumption for:
        1. ``job_events_queue`` on its dedicated channel (``prefetch_count=100``).
        2. ``orchestration_events_queue`` on its dedicated channel
           (``prefetch_count=settings.RABBITMQ_ORCHESTRATION_PREFETCH``).
    """
    logger.info("Starting Consumers on isolated channels ('job_events_queue' & 'orchestration_events_queue')...")

    job_events_channel = await get_job_events_channel()
    await job_events_channel.set_qos(prefetch_count=100)
    events_queue = await job_events_channel.declare_queue("job_events_queue", durable=True)
    await events_queue.consume(TaskEventProcessor.process_message)

    orch_channel = await get_orchestration_events_channel()
    await orch_channel.set_qos(prefetch_count=settings.RABBITMQ_ORCHESTRATION_PREFETCH)
    orch_queue = await orch_channel.declare_queue("orchestration_events_queue", durable=True)
    await orch_queue.consume(TaskEventProcessor.process_orchestration_event)

    logger.info("Consumers are now active on isolated channels and awaiting payloads.")
