"""
Unit tests for the batched Kubernetes Reconciler service (ADR-11).
"""

from typing import Any
from unittest.mock import AsyncMock

import pytest

from app.services.reconciler import run_reconciliation_audit


@pytest.mark.asyncio
async def test_reconciler_batches_pod_lookup_and_reclaims_only_phantom_jobs(mocker: Any) -> None:
    """
    Verifies run_reconciliation_audit executes ONE batched pod lookup across
    multiple active jobs and only reclaims phantom jobs past the 60s grace window.
    """
    redis_mock = AsyncMock()
    k8s_mock = AsyncMock()
    db_mock = AsyncMock()

    mocker.patch("app.services.reconciler.get_redis_client", AsyncMock(return_value=redis_mock))
    mocker.patch("app.services.reconciler.get_k8s_provider", AsyncMock(return_value=k8s_mock))
    mocker.patch("app.services.reconciler.get_db_pool", AsyncMock(return_value=db_mock))
    mocker.patch(
        "app.services.reconciler.CacheRepository.get_active_job_reservations",
        AsyncMock(return_value=["healthy-job", "young-phantom", "stale-phantom"]),
    )
    batch_lookup = mocker.patch(
        "app.services.reconciler.client.get_active_pod_counts_by_job",
        AsyncMock(return_value={"healthy-job": 3}),
    )
    mocker.patch(
        "app.services.reconciler.JobRepository.get_job_elapsed_time",
        AsyncMock(side_effect=[15.0, 120.0]),
    )
    reclaim_mock = mocker.patch(
        "app.services.reconciler.forcefully_reclaim_job",
        AsyncMock(),
    )

    await run_reconciliation_audit()

    batch_lookup.assert_awaited_once()
    reclaim_mock.assert_awaited_once_with("stale-phantom", db_mock, k8s_mock, redis_mock)
