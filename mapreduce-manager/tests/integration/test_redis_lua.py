"""
Integration tests for CacheRepository Lua scripts, ADR-05 capacity swaps,
ADR-06 attempt minting, and ADR-12 Redis SET endpoint + reverse-index lookups
executed against a real Redis Testcontainer.
"""

import pytest
import pytest_asyncio
import redis.asyncio as aioredis
from testcontainers.redis import RedisContainer

from app.repositories.cache_repository import CacheRepository


@pytest.fixture(scope="module")
def redis_container():
    """Spins up an ephemeral Redis Docker container for Lua script verification."""
    with RedisContainer("redis:7-alpine") as container:
        yield container


@pytest_asyncio.fixture(scope="function")
async def redis_client(redis_container):
    """Provides a clean, flushed async Redis client per test."""
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    client = aioredis.from_url(f"redis://{host}:{port}/0", decode_responses=True)
    await client.flushdb()
    yield client
    await client.aclose()


@pytest.mark.asyncio
async def test_atomic_capacity_swap_and_reduce_waiting_prevents_map_starvation(redis_client) -> None:
    """
    Verifies ADR-05 end-to-end against real Redis Lua execution:
    1. Job A holds 4 Map slots, Job B holds 6 Map slots (max_capacity=10).
    2. Job A finishes Map and swaps for 6 Reduce slots -> only 4 available (6 held by B) -> deferred,
       Job A's 4 Map slots released, cluster:reduce_waiting == 6.
    3. Incoming Job C requests 4 Map slots: available = 10 - 6 (B) - 6 (reduce_waiting) < 1 -> DENIED!
    4. Job B finishes/releases -> Job A retries swap -> immediately claims 6 Reduce slots, reduce_waiting == 0.
    5. Job A completes and releases -> active_workers == 0 and reduce_waiting == 0.
    """
    max_cap = 10
    assert await CacheRepository.reserve_cluster_capacity("job-a", 4, 6, max_cap, redis_client) == 4
    assert await CacheRepository.reserve_cluster_capacity("job-b", 6, 2, max_cap, redis_client) == 6

    # Job A swaps Map(4) -> Reduce(6) while Job B holds 6 slots -> cannot claim 6 immediately
    granted = await CacheRepository.swap_map_to_reduce_capacity("job-a", 6, max_cap, redis_client)
    assert granted == 0
    assert await CacheRepository.get_reduce_waiting(redis_client) == 6
    assert await CacheRepository.get_active_job_reservations(redis_client) == ["job-b"]

    # Incoming Job C cannot steal slots reserved for waiting Reducers
    assert await CacheRepository.reserve_cluster_capacity("job-c", 4, 1, max_cap, redis_client) == 0

    # Job B releases its 4 slots; Job A retries swap and claims all 6 Reduce slots
    await CacheRepository.release_job_capacity("job-b", redis_client)
    granted_retry = await CacheRepository.swap_map_to_reduce_capacity("job-a", 6, max_cap, redis_client)
    assert granted_retry == 6
    assert await CacheRepository.get_reduce_waiting(redis_client) == 0

    # Teardown clears everything cleanly
    await CacheRepository.release_job_capacity("job-a", redis_client)
    assert await CacheRepository.get_reduce_waiting(redis_client) == 0
    assert await CacheRepository.get_active_job_reservations(redis_client) == []


@pytest.mark.asyncio
async def test_release_job_capacity_clears_reduce_waiting_on_failure(redis_client) -> None:
    """Verifies forcefully_reclaim_job/release_job_capacity clears reduce_waiting if a waiting job fails."""
    max_cap = 10
    assert await CacheRepository.reserve_cluster_capacity("job-b", 6, 2, max_cap, redis_client) == 6
    assert await CacheRepository.reserve_cluster_capacity("job-a", 4, 6, max_cap, redis_client) == 4

    assert await CacheRepository.swap_map_to_reduce_capacity("job-a", 6, max_cap, redis_client) == 0
    assert await CacheRepository.get_reduce_waiting(redis_client) == 6

    # Job A fails while waiting in reduce_waiting
    await CacheRepository.release_job_capacity("job-a", redis_client)
    assert await CacheRepository.get_reduce_waiting(redis_client) == 0


@pytest.mark.asyncio
async def test_adr12_set_endpoints_reverse_index_and_adr06_attempts_cleanup(redis_client) -> None:
    """Verifies real Redis SET endpoints, O(M/K) reverse lookups, attempt minting, and cleanup."""
    job_id = "job-real-redis"
    await CacheRepository.add_worker_endpoint(job_id, "map-chunk-0", "node1@@10.0.0.1:7337", redis_client)
    await CacheRepository.add_worker_endpoint(job_id, "map-chunk-1", "node1@@10.0.0.1:7337", redis_client)
    await CacheRepository.add_worker_endpoint(job_id, "map-chunk-2", "node2@@10.0.0.2:7337", redis_client)

    eps = sorted(await CacheRepository.get_worker_endpoints(job_id, redis_client))
    assert eps == ["node1@@10.0.0.1:7337", "node2@@10.0.0.2:7337"]

    tasks_node1 = sorted(await CacheRepository.get_tasks_by_endpoint(job_id, "10.0.0.1:7337", redis_client))
    assert tasks_node1 == ["map-chunk-0", "map-chunk-1"]

    attempts = await CacheRepository.mint_task_attempts_batch(job_id, ["map-chunk-0", "map-chunk-1"], redis_client)
    assert attempts == [1, 1]
    assert await CacheRepository.mint_task_attempt(job_id, "map-chunk-0", redis_client) == 2
    assert await CacheRepository.get_expected_task_attempt(job_id, "map-chunk-0", redis_client) == 2

    await CacheRepository.cleanup_job_metadata(job_id, redis_client)
    assert await CacheRepository.get_worker_endpoints(job_id, redis_client) == []
    assert await CacheRepository.get_tasks_by_endpoint(job_id, "10.0.0.1:7337", redis_client) == []
    assert await CacheRepository.get_expected_task_attempt(job_id, "map-chunk-0", redis_client) is None


@pytest.mark.asyncio
async def test_adr12_dead_ess_eviction_and_sibling_waitlist_recovery(redis_client) -> None:
    """
    Verifies dead ESS endpoint eviction on SHUFFLE_FETCH_FAILED_FROM_NODE while
    preserving node_tasks reverse lookup for concurrent sibling Reducers and
    removing stale node_tasks membership once the task is recomputed on a healthy node.
    """
    job_id = "job-evict-test"
    await CacheRepository.add_worker_endpoint(job_id, "map-chunk-0", "dead-node@@10.0.0.1:7337", redis_client)
    await CacheRepository.add_worker_endpoint(job_id, "map-chunk-1", "healthy-node@@10.0.0.2:7337", redis_client)

    # First Reducer reports dead node -> resolves tasks & evicts dead endpoint from ess_nodes
    tasks_first = await CacheRepository.get_tasks_by_endpoint(job_id, "10.0.0.1:7337", redis_client)
    assert tasks_first == ["map-chunk-0"]
    evicted = await CacheRepository.evict_ess_endpoint(job_id, "10.0.0.1:7337", redis_client)
    assert evicted == 1
    assert await CacheRepository.get_worker_endpoints(job_id, redis_client) == ["healthy-node@@10.0.0.2:7337"]

    # Concurrent sibling Reducer failing on the same dead node still resolves affected map tasks
    tasks_sibling = await CacheRepository.get_tasks_by_endpoint(job_id, "10.0.0.1:7337", redis_client)
    assert tasks_sibling == ["map-chunk-0"]

    # Recomputed Map task finishes on a healthy node -> moves out of dead node_tasks into healthy node_tasks
    await CacheRepository.add_worker_endpoint(job_id, "map-chunk-0", "healthy-node-2@@10.0.0.3:7337", redis_client)
    assert await CacheRepository.get_tasks_by_endpoint(job_id, "10.0.0.1:7337", redis_client) == []
    assert await CacheRepository.get_tasks_by_endpoint(job_id, "10.0.0.3:7337", redis_client) == ["map-chunk-0"]
    assert sorted(await CacheRepository.get_worker_endpoints(job_id, redis_client)) == [
        "healthy-node-2@@10.0.0.3:7337",
        "healthy-node@@10.0.0.2:7337",
    ]

    await CacheRepository.cleanup_job_metadata(job_id, redis_client)
    assert await redis_client.exists(f"job:{job_id}:task_endpoint") == 0
    assert await redis_client.exists(f"job:{job_id}:node_tasks:10.0.0.3:7337") == 0
