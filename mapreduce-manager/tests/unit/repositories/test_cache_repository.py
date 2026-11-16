"""
Unit tests for the distributed Cache Repository.

This suite utilizes asynchronous mocking to verify atomic Lua-based tracking
operations, ADR-05 capacity swaps with the ``reduce_waiting`` watermark,
ADR-06 monotonic attempt minting, and ADR-12 Redis SET endpoint + reverse-index
lookups without requiring a physical Redis connection.
"""

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.repositories.cache_repository import CacheRepository


@pytest.fixture
def mock_redis() -> AsyncMock:
    """
    Provides a mocked asynchronous Redis client with async pipeline context support.
    """
    redis_mock = AsyncMock()
    pipe_mock = MagicMock()
    pipe_mock.__aenter__ = AsyncMock(return_value=pipe_mock)
    pipe_mock.__aexit__ = AsyncMock(return_value=None)
    pipe_mock.execute = AsyncMock()
    redis_mock.pipeline = MagicMock(return_value=pipe_mock)
    return redis_mock


@pytest.mark.asyncio
async def test_register_map_completion_atomic(mock_redis: AsyncMock) -> None:
    """Verifies atomic Lua-based tracking for the Map phase barrier."""
    mock_redis.eval.return_value = 1
    job_id = "test-job-123"
    task_id = "map-chunk-0"
    target_total = 5

    result = await CacheRepository.register_map_completion_atomic(
        job_id, task_id, target_total, mock_redis
    )

    assert result is True
    mock_redis.eval.assert_called_once()
    args = mock_redis.eval.call_args[0]
    assert args[1] == 1
    assert args[2] == f"job:{job_id}:map_completed_set"
    assert args[3] == task_id
    assert args[4] == target_total


@pytest.mark.asyncio
async def test_reserve_cluster_capacity_passes_reduce_waiting_key(mock_redis: AsyncMock) -> None:
    """Verifies reserve_cluster_capacity evaluates Lua with semaphore and reduce_waiting keys (ADR-05)."""
    mock_redis.eval.return_value = 4
    granted = await CacheRepository.reserve_cluster_capacity(
        job_id="job-1",
        requested_mappers=4,
        num_reducers=2,
        max_capacity=10,
        redis_client=mock_redis,
    )

    assert granted == 4
    args = mock_redis.eval.call_args[0]
    assert args[1] == 2
    assert args[2] == "cluster:global_capacity_semaphore"
    assert args[3] == "cluster:reduce_waiting"
    assert "reduce_waiting" in args[0]


@pytest.mark.asyncio
async def test_swap_map_to_reduce_capacity_granted_and_deferred(mock_redis: AsyncMock) -> None:
    """Verifies swap_map_to_reduce_capacity handles both immediate grant and deferred watermark (ADR-05)."""
    mock_redis.eval.side_effect = [6, 0]

    granted = await CacheRepository.swap_map_to_reduce_capacity("job-1", 6, 10, mock_redis)
    assert granted == 6

    deferred = await CacheRepository.swap_map_to_reduce_capacity("job-2", 8, 10, mock_redis)
    assert deferred == 0

    args = mock_redis.eval.call_args[0]
    assert args[1] == 3
    assert args[2] == "cluster:global_capacity_semaphore"
    assert args[3] == "cluster:reduce_waiting"
    assert args[4] == "job:job-2:reduce_waiting_slots"


@pytest.mark.asyncio
async def test_release_job_capacity_clears_semaphore_and_waiting_watermark(mock_redis: AsyncMock) -> None:
    """Verifies release_job_capacity runs atomic Lua cleanup across semaphore and reduce_waiting."""
    mock_redis.eval.return_value = 1
    await CacheRepository.release_job_capacity("job-1", mock_redis)

    mock_redis.eval.assert_called_once()
    args = mock_redis.eval.call_args[0]
    assert args[1] == 3
    assert args[2] == "cluster:global_capacity_semaphore"
    assert args[3] == "cluster:reduce_waiting"
    assert args[4] == "job:job-1:reduce_waiting_slots"
    assert args[5] == "job-1"


@pytest.mark.asyncio
async def test_add_worker_endpoint_updates_set_and_reverse_index(mock_redis: AsyncMock) -> None:
    """
    Verifies ADR-12: worker endpoints populate SET job:{id}:ess_nodes,
    reverse index SET job:{id}:node_tasks:{norm_ep}, and task_endpoint hash,
    and remove stale node_tasks entries when a task moves to a new node.
    """
    job_id = "test-job-123"
    task_id = "map-chunk-0"
    endpoint = "worker-node-1@@10.0.1.45:7337"
    mock_redis.hget.return_value = None

    await CacheRepository.add_worker_endpoint(job_id, task_id, endpoint, mock_redis)

    pipe = mock_redis.pipeline.return_value
    pipe.sadd.assert_any_call(f"job:{job_id}:ess_nodes", endpoint)
    pipe.sadd.assert_any_call(f"job:{job_id}:node_tasks:10.0.1.45:7337", task_id)
    pipe.hset.assert_called_once_with(f"job:{job_id}:task_endpoint", task_id, "10.0.1.45:7337")
    pipe.expire.assert_any_call(f"job:{job_id}:ess_nodes", 86400)
    pipe.expire.assert_any_call(f"job:{job_id}:node_tasks:10.0.1.45:7337", 86400)
    pipe.expire.assert_any_call(f"job:{job_id}:task_endpoint", 86400)
    pipe.execute.assert_awaited_once()

    # Simulate task moving to a new healthy node
    pipe.reset_mock()
    mock_redis.hget.return_value = b"10.0.1.45:7337"
    await CacheRepository.add_worker_endpoint(job_id, task_id, "worker-node-2@@10.0.1.99:7337", mock_redis)
    pipe.srem.assert_called_once_with(f"job:{job_id}:node_tasks:10.0.1.45:7337", task_id)
    pipe.sadd.assert_any_call(f"job:{job_id}:node_tasks:10.0.1.99:7337", task_id)


@pytest.mark.asyncio
async def test_evict_ess_endpoint_removes_matching_entries(mock_redis: AsyncMock) -> None:
    """Verifies evict_ess_endpoint removes both prefixed and bare matching entries from ess_nodes."""
    job_id = "test-job-123"
    mock_redis.smembers.return_value = {
        b"node1@@10.0.1.45:7337",
        b"10.0.1.45:7337",
        b"node2@@10.0.1.46:7337",
    }
    mock_redis.srem.return_value = 2

    removed = await CacheRepository.evict_ess_endpoint(job_id, "10.0.1.45:7337", mock_redis)
    assert removed == 2
    srem_args = mock_redis.srem.call_args[0]
    assert srem_args[0] == f"job:{job_id}:ess_nodes"
    assert set(srem_args[1:]) == {"node1@@10.0.1.45:7337", "10.0.1.45:7337"}


@pytest.mark.asyncio
async def test_get_worker_endpoints_uses_smembers(mock_redis: AsyncMock) -> None:
    """Verifies ADR-12 O(K) endpoint retrieval via SMEMBERS job:{id}:ess_nodes."""
    job_id = "test-job-123"
    mock_redis.smembers.return_value = {b"node1@@10.0.1.45:7337", b"node2@@10.0.1.46:7337"}

    result = await CacheRepository.get_worker_endpoints(job_id, mock_redis)

    assert sorted(result) == ["node1@@10.0.1.45:7337", "node2@@10.0.1.46:7337"]
    mock_redis.smembers.assert_called_once_with(f"job:{job_id}:ess_nodes")


@pytest.mark.asyncio
async def test_get_tasks_by_endpoint_normalizes_prefix(mock_redis: AsyncMock) -> None:
    """Verifies ADR-12 reverse lookup normalizes both bare IP:Port and node@@IP:Port."""
    job_id = "test-job-123"
    mock_redis.smembers.return_value = {b"map-chunk-0", b"map-chunk-2"}

    result = await CacheRepository.get_tasks_by_endpoint(job_id, "worker-node-1@@10.0.1.45:7337", mock_redis)

    assert sorted(result) == ["map-chunk-0", "map-chunk-2"]
    mock_redis.smembers.assert_called_once_with(f"job:{job_id}:node_tasks:10.0.1.45:7337")


@pytest.mark.asyncio
async def test_mint_task_attempts_batch_and_lookup(mock_redis: AsyncMock) -> None:
    """Verifies ADR-06 pipelined attempt minting and HGET verification."""
    pipe = mock_redis.pipeline.return_value
    pipe.execute.return_value = [1, 1, True]

    attempts = await CacheRepository.mint_task_attempts_batch(
        "job-1", ["map-chunk-0", "map-chunk-1"], mock_redis
    )
    assert attempts == [1, 1]
    pipe.hincrby.assert_any_call("job:job-1:attempts", "map-chunk-0", 1)
    pipe.hincrby.assert_any_call("job:job-1:attempts", "map-chunk-1", 1)
    pipe.expire.assert_called_once_with("job:job-1:attempts", 86400)

    mock_redis.hget.return_value = b"2"
    expected = await CacheRepository.get_expected_task_attempt("job-1", "map-chunk-0", mock_redis)
    assert expected == 2


@pytest.mark.asyncio
async def test_cleanup_job_metadata_deletes_ess_nodes_reverse_indexes_and_attempts(mock_redis: AsyncMock) -> None:
    """Verifies teardown deletes ess_nodes, task_endpoint, all node_tasks reverse indexes, and attempts hash."""
    job_id = "job-cleanup"
    mock_redis.smembers.return_value = {b"node1@@10.0.1.1:7337", b"10.0.1.2:7337"}
    mock_redis.hvals.return_value = [b"10.0.1.3:7337"]

    await CacheRepository.cleanup_job_metadata(job_id, mock_redis)

    deleted_keys = set(mock_redis.delete.call_args[0])
    assert deleted_keys == {
        f"job:{job_id}:ess_nodes",
        f"job:{job_id}:attempts",
        f"job:{job_id}:task_endpoint",
        f"job:{job_id}:node_tasks:10.0.1.1:7337",
        f"job:{job_id}:node_tasks:10.0.1.2:7337",
        f"job:{job_id}:node_tasks:10.0.1.3:7337",
    }


@pytest.mark.asyncio
async def test_register_reduce_completion_atomic(mock_redis: AsyncMock) -> None:
    """Verifies atomic Lua-based tracking for the Reduce phase barrier."""
    mock_redis.eval.return_value = 0
    result = await CacheRepository.register_reduce_completion_atomic(
        "test-job-123", "1", 3, mock_redis
    )
    assert result is False
    mock_redis.eval.assert_called_once()


@pytest.mark.asyncio
async def test_get_cached_total_chunks_from_meta_hash(mock_redis: AsyncMock) -> None:
    """Verifies retrieval of cached total_chunks from progress_meta hash."""
    mock_redis.hget.return_value = "10"
    result = await CacheRepository.get_cached_total_chunks("test-job-123", mock_redis)
    assert result == 10
    mock_redis.hget.assert_called_once_with("job:test-job-123:progress_meta", "total_chunks")


@pytest.mark.asyncio
async def test_get_cached_total_chunks_missing(mock_redis: AsyncMock) -> None:
    """Verifies cache miss returns None for progress_meta lookups."""
    mock_redis.hget.return_value = None
    assert await CacheRepository.get_cached_total_chunks("test-job-123", mock_redis) is None
