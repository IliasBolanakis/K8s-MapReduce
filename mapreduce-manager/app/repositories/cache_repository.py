"""
Repository module for high-throughput distributed caching operations.

This module acts as the Data Access Layer (DAL) for Redis. It abstracts
raw Redis commands into domain-specific operations, primarily focusing on
the atomic, idempotent operations required to track phase barriers (Map/Reduce)
without incurring database lock contention.

With the introduction of the Peer-to-Peer Shuffle architecture, this module
also acts as the dynamic Directory Service, aggregating network routes
from active Map workers to construct the Reduce phase routing table.
"""

import json
import logging
from typing import List, Optional, cast, Awaitable, Any, Dict

from redis.asyncio import Redis

# Initialize the module-level logger for cache persistence events
logger = logging.getLogger(__name__)


class CacheRepository:
    """
    Data access layer for in-memory distributed tracking and synchronization.

    Utilizes Redis Hashes and Sets (:code:`HSET`, :code:`HVALS`, :code:`SADD`, :code:`SCARD`)
    to map job IDs to their completed tasks and active network locations.

    Phase transitions are guarded by atomic Lua scripts to prevent distributed
    race conditions during concurrent worker callbacks.
    """

    @staticmethod
    async def try_acquire_idempotency_lock(
            idempotency_key: str,
            ttl: int,
            redis_client: Redis
    ) -> tuple[bool, Optional[Dict[str, Any]]]:
        """
        Attempts to atomically acquire an operational lock for an idempotency key.

        If the lock is successfully acquired, returns (True, None).
        If the key already exists (meaning it is actively processing or has cached a response),
        returns (False, cached_data_dict).

        :param idempotency_key: The client-provided unique retry identifier.
        :type idempotency_key: str
        :param ttl: Time-to-live expiration limit in seconds.
        :type ttl: int
        :param redis_client: The active asynchronous Redis client.
        :type redis_client: Redis
        :return: A tuple containing a success flag and any cached response data.
        :rtype: tuple[bool, Optional[Dict[str, Any]]]
        """
        key = f"idempotency:schedule:{idempotency_key}"

        # Atomically set key if Not Exists (NX) to establish lock
        acquired = await redis_client.set(
            key,
            json.dumps({"status": "PROCESSING"}),
            nx=True,
            ex=ttl
        )
        if acquired:
            return True, None

        # Lock acquisition failed; retrieve existing state/cached value
        raw_val = await redis_client.get(key)
        if raw_val:
            decoded = raw_val.decode('utf-8') if isinstance(raw_val, bytes) else raw_val
            return False, json.loads(decoded)

        return False, None

    @staticmethod
    async def cache_idempotent_response(
            idempotency_key: str,
            response_payload: Dict[str, Any],
            ttl: int,
            redis_client: Redis
    ) -> None:
        """
        Caches the final successful orchestration payload for a given idempotency key.

        :param idempotency_key: The client-provided unique retry identifier.
        :type idempotency_key: str
        :param response_payload: The dictionary response to cache for subsequent calls.
        :type response_payload: Dict[str, Any]
        :param ttl: Time-to-live expiration cache limit in seconds.
        :type ttl: int
        :param redis_client: The active asynchronous Redis client.
        :type redis_client: Redis
        """
        key = f"idempotency:schedule:{idempotency_key}"
        await redis_client.set(key, json.dumps(response_payload), ex=ttl)

    @staticmethod
    async def release_idempotency_lock(idempotency_key: str, redis_client: Redis) -> None:
        """
        Removes the idempotency lock in the event of a non-idempotent system exception.

        :param idempotency_key: The client-provided unique retry identifier.
        :type idempotency_key: str
        :param redis_client: The active asynchronous Redis client.
        :type redis_client: Redis
        """
        key = f"idempotency:schedule:{idempotency_key}"
        await redis_client.delete(key)

    @staticmethod
    def _normalize_endpoint(endpoint: str) -> str:
        """
        Normalizes a worker endpoint string by stripping any ``<node_name>@@`` prefix.

        :param endpoint: Raw endpoint string (e.g., ``node1@@10.0.1.45:7337`` or ``10.0.1.45:7337``).
        :type endpoint: str
        :return: Canonical ``IP:Port`` endpoint identifier.
        :rtype: str
        """
        return endpoint.split("@@", 1)[1] if "@@" in endpoint else endpoint

    @staticmethod
    def _decode(value: Any) -> str:
        """Decodes bytes values returned by Redis into UTF-8 strings."""
        return value.decode("utf-8") if isinstance(value, bytes) else str(value)

    @staticmethod
    async def reserve_cluster_capacity(
            job_id: str,
            requested_mappers: int,
            num_reducers: int,
            max_capacity: int,
            redis_client: Redis
    ) -> int:
        """
        Atomically reserves cluster capacity for incoming Map phase workloads.

        Respects the ``cluster:reduce_waiting`` watermark (ADR-05) so incoming
        Map jobs cannot starve Reducers that are waiting to transition out of
        an active Map phase: ``available = max_capacity - active_workers - reduce_waiting``.

        :param job_id: Unique job identifier claiming capacity.
        :type job_id: str
        :param requested_mappers: Desired parallel Map worker count.
        :type requested_mappers: int
        :param num_reducers: Planned Reduce partition count (retained for signature parity).
        :type num_reducers: int
        :param max_capacity: Global cluster ceiling (``MAX_TOTAL_WORKERS``).
        :type max_capacity: int
        :param redis_client: Active asynchronous Redis client.
        :type redis_client: Redis
        :return: Granted Map worker slot count (``0`` if cluster is saturated or reserved for waiting Reducers).
        :rtype: int
        """
        script = """
        local hash_key = KEYS[1]
        local waiting_key = KEYS[2]
        local job_id = ARGV[1]
        local requested_map = tonumber(ARGV[2])
        local max_capacity = tonumber(ARGV[4])

        -- Calculate active reservations across the cluster
        local all_vals = redis.call('HVALS', hash_key)
        local current_total = 0
        for i, v in ipairs(all_vals) do
            current_total = current_total + tonumber(v)
        end

        local reduce_waiting = tonumber(redis.call('GET', waiting_key) or '0') or 0
        if reduce_waiting < 0 then
            reduce_waiting = 0
        end

        local available = max_capacity - current_total - reduce_waiting

        if available < 1 then
            return 0
        end

        local granted_map = requested_map
        if requested_map > available then
            granted_map = available
        end

        redis.call('HINCRBY', hash_key, job_id, granted_map)

        return granted_map
        """
        key = "cluster:global_capacity_semaphore"
        waiting_key = "cluster:reduce_waiting"

        result = await cast(
            Awaitable[Any],
            redis_client.eval(script, 2, key, waiting_key, job_id, requested_mappers, num_reducers, max_capacity)
        )

        return int(result)

    @staticmethod
    async def swap_map_to_reduce_capacity(
            job_id: str,
            target_reducers: int,
            max_capacity: int,
            redis_client: Redis
    ) -> int:
        """
        Atomically transitions a job's cluster reservation from Map slots to Reduce slots (ADR-05).

        Releases the job's held Map slots in ``cluster:global_capacity_semaphore`` and either:
        - Immediately claims ``target_reducers`` active slots (clearing any prior
          ``job:{job_id}:reduce_waiting_slots`` contribution from ``cluster:reduce_waiting``), OR
        - Registers ``target_reducers`` in ``cluster:reduce_waiting`` and
          ``job:{job_id}:reduce_waiting_slots`` without holding partial active slots.

        :param job_id: Unique identifier of the transitioning job.
        :type job_id: str
        :param target_reducers: Exact number of Reduce slots required (1:1 with partitions).
        :type target_reducers: int
        :param max_capacity: Global cluster ceiling (``MAX_TOTAL_WORKERS``).
        :type max_capacity: int
        :param redis_client: Active asynchronous Redis client.
        :type redis_client: Redis
        :return: ``target_reducers`` if immediately claimed, or ``0`` if registered in ``reduce_waiting``.
        :rtype: int
        """
        script = """
        local hash_key = KEYS[1]
        local waiting_key = KEYS[2]
        local job_waiting_key = KEYS[3]
        local job_id = ARGV[1]
        local target_reducers = tonumber(ARGV[2])
        local max_capacity = tonumber(ARGV[3])

        -- Atomically release this job's Map (or previous) active slots
        redis.call('HDEL', hash_key, job_id)

        -- Remove this job's own prior waiting contribution so retries do not self-block
        local own_waiting = tonumber(redis.call('GET', job_waiting_key) or '0') or 0
        local total_waiting = tonumber(redis.call('GET', waiting_key) or '0') or 0
        local other_waiting = total_waiting - own_waiting
        if other_waiting < 0 then
            other_waiting = 0
        end

        -- Sum active slots held by all other jobs
        local all_vals = redis.call('HVALS', hash_key)
        local other_active = 0
        for i, v in ipairs(all_vals) do
            other_active = other_active + tonumber(v)
        end

        local available = max_capacity - other_active
        if available >= target_reducers then
            redis.call('HSET', hash_key, job_id, target_reducers)
            redis.call('DEL', job_waiting_key)
            if other_waiting == 0 then
                redis.call('DEL', waiting_key)
            else
                redis.call('SET', waiting_key, other_waiting)
            end
            return target_reducers
        else
            redis.call('SET', job_waiting_key, target_reducers, 'EX', 86400)
            redis.call('SET', waiting_key, other_waiting + target_reducers)
            return 0
        end
        """
        hash_key = "cluster:global_capacity_semaphore"
        waiting_key = "cluster:reduce_waiting"
        job_waiting_key = f"job:{job_id}:reduce_waiting_slots"

        result = await cast(
            Awaitable[Any],
            redis_client.eval(
                script,
                3,
                hash_key,
                waiting_key,
                job_waiting_key,
                job_id,
                target_reducers,
                max_capacity,
            ),
        )
        granted = int(result)
        if granted > 0:
            logger.info(
                "Atomic capacity swap granted %d Reduce slots for job %s.",
                granted,
                job_id,
            )
        else:
            logger.warning(
                "Atomic capacity swap deferred for job %s; registered %d slots in cluster:reduce_waiting.",
                job_id,
                target_reducers,
            )
        return granted

    @staticmethod
    async def release_job_capacity(job_id: str, redis_client: Redis) -> None:
        """
        Atomically releases both active cluster slots and any ``reduce_waiting``
        watermark contribution held by a specific job.

        :param job_id: Unique job identifier to release.
        :type job_id: str
        :param redis_client: Active asynchronous Redis client.
        :type redis_client: Redis
        """
        script = """
        local hash_key = KEYS[1]
        local waiting_key = KEYS[2]
        local job_waiting_key = KEYS[3]
        local job_id = ARGV[1]

        redis.call('HDEL', hash_key, job_id)

        local job_waiting = tonumber(redis.call('GET', job_waiting_key) or '0') or 0
        if job_waiting > 0 then
            local total_waiting = tonumber(redis.call('GET', waiting_key) or '0') or 0
            local new_waiting = total_waiting - job_waiting
            if new_waiting <= 0 then
                redis.call('DEL', waiting_key)
            else
                redis.call('SET', waiting_key, new_waiting)
            end
        end
        redis.call('DEL', job_waiting_key)
        return 1
        """
        key = "cluster:global_capacity_semaphore"
        waiting_key = "cluster:reduce_waiting"
        job_waiting_key = f"job:{job_id}:reduce_waiting_slots"
        await cast(
            Awaitable[Any],
            redis_client.eval(script, 3, key, waiting_key, job_waiting_key, job_id),
        )

    @staticmethod
    async def get_reduce_waiting(redis_client: Redis) -> int:
        """
        Returns the current global ``cluster:reduce_waiting`` watermark value.

        :param redis_client: Active asynchronous Redis client.
        :type redis_client: Redis
        :return: Number of slots currently reserved in the Reduce waiting watermark.
        :rtype: int
        """
        val = await cast(Awaitable[Any], redis_client.get("cluster:reduce_waiting"))
        return max(0, int(CacheRepository._decode(val))) if val is not None else 0

    @staticmethod
    async def register_map_completion_atomic(
            job_id: str,
            task_id: str,
            target_total: int,
            redis_client: Redis
    ) -> bool:
        """
        Idempotently records a Map task and atomically evaluates the phase barrier.
        """
        script = """
            local set_key = KEYS[1]
            local task_id = ARGV[1]
            local target_total = tonumber(ARGV[2])

            -- Capture whether this specific call added a new task
            local added = redis.call('SADD', set_key, task_id)
            redis.call('EXPIRE', set_key, 86400)

            local current_count = redis.call('SCARD', set_key)

            -- The barrier is ONLY breached if THIS specific task tipped the scale
            if current_count == target_total and added == 1 then
                return 1
            else
                return 0
            end
            """
        key = f"job:{job_id}:map_completed_set"
        result = await cast(Awaitable[Any], redis_client.eval(script, 1, key, task_id, target_total))
        return result == 1

    @staticmethod
    async def add_worker_endpoint(job_id: str, task_id: str, endpoint: str, redis_client: Redis) -> None:
        """
        Registers a Map worker's ESS network endpoint and per-node reverse task index (ADR-12).

        Stores ``endpoint`` (e.g., ``node@@IP:Port``) in ``SET job:{job_id}:ess_nodes``
        for O(K) routing-table construction, indexes ``task_id`` under
        ``SET job:{job_id}:node_tasks:{norm_ep}`` for O(M/K) node-failure lookups,
        and maintains ``HASH job:{job_id}:task_endpoint`` so task recomputation on a
        new healthy node removes stale reverse-index entries from the previous node.

        :param job_id: Parent job identifier.
        :type job_id: str
        :param task_id: Completed Map task identifier (e.g., ``map-chunk-0``).
        :type task_id: str
        :param endpoint: Worker ESS bind address (``node@@IP:Port`` or ``IP:Port``).
        :type endpoint: str
        :param redis_client: Active asynchronous Redis client.
        :type redis_client: Redis
        """
        if not endpoint:
            return

        ess_key = f"job:{job_id}:ess_nodes"
        norm_ep = CacheRepository._normalize_endpoint(endpoint)
        node_tasks_key = f"job:{job_id}:node_tasks:{norm_ep}"
        task_ep_key = f"job:{job_id}:task_endpoint"

        old_raw = await cast(Awaitable[Any], redis_client.hget(task_ep_key, task_id))
        old_norm_ep = CacheRepository._decode(old_raw) if old_raw is not None else None

        async with redis_client.pipeline() as pipe:
            if old_norm_ep and old_norm_ep != norm_ep:
                pipe.srem(f"job:{job_id}:node_tasks:{old_norm_ep}", task_id)
            pipe.sadd(ess_key, endpoint)
            pipe.expire(ess_key, 86400)  # type: ignore
            pipe.sadd(node_tasks_key, task_id)
            pipe.expire(node_tasks_key, 86400)  # type: ignore
            pipe.hset(task_ep_key, task_id, norm_ep)
            pipe.expire(task_ep_key, 86400)  # type: ignore
            await pipe.execute()

    @staticmethod
    async def evict_ess_endpoint(
            job_id: str,
            failed_endpoint: str,
            redis_client: Redis
    ) -> int:
        """
        Evicts a dead ESS endpoint from ``SET job:{job_id}:ess_nodes`` (ADR-12).

        Matches both bare ``IP:Port`` and ``<node_name>@@IP:Port`` entries whose
        normalized ``IP:Port`` equals ``_normalize_endpoint(failed_endpoint)``.
        Intentionally preserves ``job:{job_id}:node_tasks:{norm_ep}`` during
        in-flight recovery so concurrent sibling Reducers failing on the same dead
        node still resolve affected Map tasks and join ``recovery_waitlist``.

        :param job_id: Parent job identifier.
        :type job_id: str
        :param failed_endpoint: Failed ESS endpoint reported by a Reducer.
        :type failed_endpoint: str
        :param redis_client: Active asynchronous Redis client.
        :type redis_client: Redis
        :return: Number of matching entries removed from ``job:{job_id}:ess_nodes``.
        :rtype: int
        """
        if not failed_endpoint:
            return 0

        target_norm = CacheRepository._normalize_endpoint(failed_endpoint)
        endpoints = await CacheRepository.get_worker_endpoints(job_id, redis_client)
        to_remove = [
            ep for ep in endpoints if CacheRepository._normalize_endpoint(ep) == target_norm
        ]
        if not to_remove:
            return 0

        ess_key = f"job:{job_id}:ess_nodes"
        removed = await cast(Awaitable[Any], redis_client.srem(ess_key, *to_remove))
        logger.warning(
            "Evicted dead ESS endpoint(s) %s from %s for job %s.",
            to_remove,
            ess_key,
            job_id,
        )
        return int(removed)

    @staticmethod
    async def get_worker_endpoints(job_id: str, redis_client: Redis) -> List[str]:
        """
        Retrieves the deduplicated set of ESS endpoints for a job in O(K) time (ADR-12).

        :param job_id: Parent job identifier.
        :type job_id: str
        :param redis_client: Active asynchronous Redis client.
        :type redis_client: Redis
        :return: List of ESS endpoint strings (``node@@IP:Port``).
        :rtype: List[str]
        """
        ess_key = f"job:{job_id}:ess_nodes"
        endpoints = await cast(Awaitable[Any], redis_client.smembers(ess_key))

        if not endpoints:
            return []

        return [CacheRepository._decode(endpoint) for endpoint in endpoints]

    @staticmethod
    async def get_tasks_by_endpoint(job_id: str, endpoint: str, redis_client: Redis) -> List[str]:
        """
        Retrieves all Map task IDs hosted on a failed ESS endpoint in O(M/K) time (ADR-12).

        Normalizes ``endpoint`` identically to :meth:`add_worker_endpoint` so both
        ``node@@IP:Port`` and bare ``IP:Port`` inputs resolve cleanly.

        :param job_id: Parent job identifier.
        :type job_id: str
        :param endpoint: Failed ESS endpoint identifier.
        :type endpoint: str
        :param redis_client: Active asynchronous Redis client.
        :type redis_client: Redis
        :return: List of affected Map task IDs.
        :rtype: List[str]
        """
        if not endpoint:
            return []

        norm_ep = CacheRepository._normalize_endpoint(endpoint)
        node_tasks_key = f"job:{job_id}:node_tasks:{norm_ep}"
        tasks = await cast(Awaitable[Any], redis_client.smembers(node_tasks_key))
        if not tasks:
            return []

        return [CacheRepository._decode(task_id) for task_id in tasks]

    @staticmethod
    async def register_reduce_completion_atomic(
            job_id: str,
            task_id: str,
            target_total: int,
            redis_client: Redis
    ) -> bool:
        """
        Idempotently records a Reduce task and evaluates the final phase barrier.
        """
        script = """
            local set_key = KEYS[1]
            local task_id = ARGV[1]
            local target_total = tonumber(ARGV[2])

            -- Capture whether this specific call added a new task
            local added = redis.call('SADD', set_key, task_id)
            redis.call('EXPIRE', set_key, 86400)

            local current_count = redis.call('SCARD', set_key)

            -- The barrier is ONLY breached if THIS specific task tipped the scale
            if current_count == target_total and added == 1 then
                return 1
            else
                return 0
            end
            """
        key = f"job:{job_id}:reduce_completed_set"
        result = await cast(Awaitable[Any], redis_client.eval(script, 1, key, task_id, target_total))
        return result == 1

    @staticmethod
    async def get_cached_total_chunks(job_id: str, redis_client: Redis) -> Optional[int]:
        """
        Retrieves the cached 'total_chunks' boundary for a job to reduce DB load.
        """
        key = f"job:{job_id}:progress_meta"
        val = await cast(Awaitable[Any], redis_client.hget(key, "total_chunks"))

        if val is None:
            return None

        decoded_val = val.decode('utf-8') if isinstance(val, bytes) else val
        return int(decoded_val)

    @staticmethod
    async def cache_total_chunks(job_id: str, total_chunks: int, redis_client: Redis) -> None:
        """
        Caches the static 'total_chunks' boundary to prevent database hits.
        """
        key = f"job:{job_id}:progress_meta"

        async with redis_client.pipeline() as pipe:
            pipe.hset(key, "total_chunks", str(total_chunks))
            pipe.expire(key, 86400)  # type: ignore
            await pipe.execute()

    @staticmethod
    async def add_to_recovery_waitlist(
            job_id: str,
            lost_map_task_id: str,
            failed_reduce_task_id: str,
            redis_client: Redis
    ) -> None:
        """
        Registers a failed Reducer into the recovery waitlist.
        """
        key = f"job:{job_id}:recovery_waitlist:{lost_map_task_id}"

        async with redis_client.pipeline() as pipe:
            pipe.sadd(key, failed_reduce_task_id)
            pipe.expire(key, 86400)  # type: ignore
            await pipe.execute()

    @staticmethod
    async def pop_from_recovery_waitlist(
            job_id: str,
            completed_map_task_id: str,
            redis_client: Redis
    ) -> List[str]:
        """
        Atomically retrieves and removes waiting Reducers from the waitlist.
        """
        script = """
            local key = KEYS[1]
            local members = redis.call('SMEMBERS', key)
            if #members > 0 then
                redis.call('DEL', key)
            end
            return members
            """
        key = f"job:{job_id}:recovery_waitlist:{completed_map_task_id}"

        failed_reducers = await cast(Awaitable[Any], redis_client.eval(script, 1, key))

        if failed_reducers:
            return [r.decode('utf-8') if isinstance(r, bytes) else r for r in failed_reducers]

        return []

    @staticmethod
    async def increment_task_failure(
            job_id: str,
            task_id: str,
            redis_client: Redis
    ) -> int:
        """
        Atomically increments and returns the failure count for a specific task.
        """
        key = f"job:{job_id}:task_failures"

        async with redis_client.pipeline() as pipe:
            pipe.hincrby(key, task_id, 1)
            pipe.expire(key, 86400)  # type: ignore
            results = await pipe.execute()

        return int(results[0])

    @staticmethod
    async def get_active_job_reservations(redis_client: Redis) -> List[str]:
        """
        Retrieves a list of Job IDs that currently have capacity reserved.

        FIXED BUG: Migrated from scanning legacy standalone keyspace variables
        (redis.keys("job:*:reserved_capacity")) to reading hash fields directly
        from the active cluster-wide multi-tenant semaphore data structure.
        """
        key = "cluster:global_capacity_semaphore"
        fields = await cast(Awaitable[Any], redis_client.hkeys(key))

        if not fields:
            return []

        return [
            f.decode('utf-8') if isinstance(f, bytes) else f
            for f in fields
        ]

    @staticmethod
    async def mint_task_attempt(job_id: str, task_id: str, redis_client: Redis) -> int:
        """
        Atomically mints and returns the next monotonic attempt ID for a task (ADR-06).

        :param job_id: Parent job identifier.
        :type job_id: str
        :param task_id: Target task or partition identifier.
        :type task_id: str
        :param redis_client: Active asynchronous Redis client.
        :type redis_client: Redis
        :return: Newly minted monotonic attempt number (>= 1).
        :rtype: int
        """
        key = f"job:{job_id}:attempts"
        async with redis_client.pipeline() as pipe:
            pipe.hincrby(key, task_id, 1)
            pipe.expire(key, 86400)  # type: ignore
            results = await pipe.execute()
        return int(results[0])

    @staticmethod
    async def mint_task_attempts_batch(
            job_id: str,
            task_ids: List[str],
            redis_client: Redis
    ) -> List[int]:
        """
        Pipelined monotonic attempt minting for a batch of tasks (ADR-06).

        :param job_id: Parent job identifier.
        :type job_id: str
        :param task_ids: Ordered list of task identifiers to mint attempts for.
        :type task_ids: List[str]
        :param redis_client: Active asynchronous Redis client.
        :type redis_client: Redis
        :return: Attempt numbers aligned positionally with ``task_ids``.
        :rtype: List[int]
        """
        if not task_ids:
            return []

        key = f"job:{job_id}:attempts"
        async with redis_client.pipeline() as pipe:
            for tid in task_ids:
                pipe.hincrby(key, tid, 1)
            pipe.expire(key, 86400)  # type: ignore
            results = await pipe.execute()

        return [int(val) for val in results[:len(task_ids)]]

    @staticmethod
    async def get_expected_task_attempt(
            job_id: str,
            task_id: str,
            redis_client: Redis
    ) -> Optional[int]:
        """
        Retrieves the active fenced attempt ID for a task from Redis (ADR-06).

        :param job_id: Parent job identifier.
        :type job_id: str
        :param task_id: Task or partition identifier.
        :type task_id: str
        :param redis_client: Active asynchronous Redis client.
        :type redis_client: Redis
        :return: Active attempt number if present, or None if unrecorded.
        :rtype: Optional[int]
        """
        key = f"job:{job_id}:attempts"
        val = await cast(Awaitable[Any], redis_client.hget(key, task_id))
        if val is None:
            return None
        return int(CacheRepository._decode(val))

    @staticmethod
    async def cache_num_reducers(job_id: str, num_reducers: int, redis_client: Redis) -> None:
        """
        Caches ``num_reducers`` in ``job:{job_id}:progress_meta`` for queue teardown discovery.

        :param job_id: Parent job identifier.
        :type job_id: str
        :param num_reducers: Configured reducer partition count.
        :type num_reducers: int
        :param redis_client: Active asynchronous Redis client.
        :type redis_client: Redis
        """
        key = f"job:{job_id}:progress_meta"
        async with redis_client.pipeline() as pipe:
            pipe.hset(key, "num_reducers", str(num_reducers))
            pipe.expire(key, 86400)  # type: ignore
            await pipe.execute()

    @staticmethod
    async def get_cached_num_reducers(job_id: str, redis_client: Redis) -> Optional[int]:
        """
        Retrieves cached ``num_reducers`` from ``job:{job_id}:progress_meta`` if present.

        :param job_id: Parent job identifier.
        :type job_id: str
        :param redis_client: Active asynchronous Redis client.
        :type redis_client: Redis
        :return: Cached reducer count or None.
        :rtype: Optional[int]
        """
        key = f"job:{job_id}:progress_meta"
        val = await cast(Awaitable[Any], redis_client.hget(key, "num_reducers"))
        if val is None:
            return None
        return int(CacheRepository._decode(val))

    @staticmethod
    async def get_reduce_partition_ids_from_attempts(job_id: str, redis_client: Redis) -> List[int]:
        """
        Returns numeric partition IDs recorded in ``job:{job_id}:attempts``.

        :param job_id: Parent job identifier.
        :type job_id: str
        :param redis_client: Active asynchronous Redis client.
        :type redis_client: Redis
        :return: Sorted list of integer partition IDs.
        :rtype: List[int]
        """
        key = f"job:{job_id}:attempts"
        fields = await cast(Awaitable[Any], redis_client.hkeys(key))
        if not fields:
            return []
        partitions: List[int] = []
        for field in fields:
            name = CacheRepository._decode(field)
            if name.isdigit():
                partitions.append(int(name))
        return sorted(partitions)

    @staticmethod
    async def cleanup_job_metadata(job_id: str, redis_client: Redis) -> None:
        """
        Purges ephemeral per-job Redis keys (attempts hash, ESS endpoint set, task_endpoint map, and reverse node indexes).

        :param job_id: Parent job identifier being torn down.
        :type job_id: str
        :param redis_client: Active asynchronous Redis client.
        :type redis_client: Redis
        """
        ess_key = f"job:{job_id}:ess_nodes"
        attempts_key = f"job:{job_id}:attempts"
        task_ep_key = f"job:{job_id}:task_endpoint"
        endpoints = await CacheRepository.get_worker_endpoints(job_id, redis_client)
        tracked_eps = await cast(Awaitable[Any], redis_client.hvals(task_ep_key))

        keys_to_delete = [ess_key, attempts_key, task_ep_key]
        seen_norm: set[str] = set()
        for ep in endpoints:
            norm_ep = CacheRepository._normalize_endpoint(ep)
            if norm_ep not in seen_norm:
                seen_norm.add(norm_ep)
                keys_to_delete.append(f"job:{job_id}:node_tasks:{norm_ep}")
        if tracked_eps:
            for raw_ep in tracked_eps:
                norm_ep = CacheRepository._normalize_endpoint(CacheRepository._decode(raw_ep))
                if norm_ep not in seen_norm:
                    seen_norm.add(norm_ep)
                    keys_to_delete.append(f"job:{job_id}:node_tasks:{norm_ep}")

        if keys_to_delete:
            await cast(Awaitable[Any], redis_client.delete(*keys_to_delete))