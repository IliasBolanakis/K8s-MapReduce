"""
Unit tests for StorageService fenced Reduce staging promotion/cleanup (ADR-06)
and centralized lifecycle reclamation (ADR-05/ADR-06/ADR-12).
"""

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from minio.error import S3Error

from app.services.lifecycle import forcefully_reclaim_job
from app.services.storage import StorageService


def test_promote_staged_reduce_output_copies_and_deletes_staging(mocker: Any) -> None:
    """Verifies attempt_id > 0 copies .staging -> canonical result_part_<p>.txt and removes staging."""
    minio_mock = MagicMock()
    mocker.patch("app.services.storage.StorageService.get_client", return_value=minio_mock)

    StorageService.promote_staged_reduce_output("user-1", "job-1", "2", 3)

    minio_mock.copy_object.assert_called_once()
    copy_args = minio_mock.copy_object.call_args[0]
    assert copy_args[0] == "results"
    assert copy_args[1] == "user-1/job-1/result_part_2.txt"
    assert copy_args[2].bucket_name == "results"
    assert copy_args[2].object_name == "user-1/job-1/_attempts/part_2_attempt_3.staging"
    minio_mock.remove_object.assert_called_once_with(
        "results", "user-1/job-1/_attempts/part_2_attempt_3.staging"
    )


def test_promote_staged_reduce_output_handles_idempotent_redelivery(mocker: Any) -> None:
    """Verifies NoSuchKey on staging succeeds idempotently if canonical output already exists."""
    minio_mock = MagicMock()
    minio_mock.copy_object.side_effect = S3Error(
        code="NoSuchKey",
        message="Not found",
        resource="/results/staging",
        request_id="req",
        host_id="host",
        response=MagicMock(),
    )
    minio_mock.stat_object.return_value = MagicMock(size=42)
    mocker.patch("app.services.storage.StorageService.get_client", return_value=minio_mock)

    StorageService.promote_staged_reduce_output("user-1", "job-1", "0", 1)

    minio_mock.stat_object.assert_called_once_with("results", "user-1/job-1/result_part_0.txt")
    minio_mock.remove_object.assert_not_called()


def test_promote_staged_reduce_output_noop_for_attempt_zero(mocker: Any) -> None:
    """Verifies attempt_id == 0 skips staging promotion for backward compatibility."""
    get_client = mocker.patch("app.services.storage.StorageService.get_client")
    StorageService.promote_staged_reduce_output("user-1", "job-1", "0", 0)
    get_client.assert_not_called()


def test_cleanup_job_attempts_prefix_removes_all_leftovers(mocker: Any) -> None:
    """Verifies cleanup_job_attempts_prefix removes all objects under {user_id}/{job_id}/_attempts/."""
    obj1 = MagicMock(object_name="user-1/job-1/_attempts/part_0_attempt_1.staging")
    obj2 = MagicMock(object_name="user-1/job-1/_attempts/part_1_attempt_2.staging")
    minio_mock = MagicMock()
    minio_mock.list_objects.return_value = [obj1, obj2]
    mocker.patch("app.services.storage.StorageService.get_client", return_value=minio_mock)

    removed = StorageService.cleanup_job_attempts_prefix("user-1", "job-1")

    assert removed == 2
    minio_mock.list_objects.assert_called_once_with(
        "results", prefix="user-1/job-1/_attempts/", recursive=True
    )
    assert minio_mock.remove_object.call_count == 2


@pytest.mark.asyncio
async def test_forcefully_reclaim_job_cleans_k8s_redis_capacity_metadata_and_db(mocker: Any) -> None:
    """Verifies forcefully_reclaim_job cleans K8s, Redis capacity + waiting watermark + metadata, and marks DB FAILED."""
    db_pool = AsyncMock()
    k8s_provider = AsyncMock()
    redis_client = AsyncMock()

    rel_cap = mocker.patch("app.services.lifecycle.CacheRepository.release_job_capacity", AsyncMock())
    clean_meta = mocker.patch("app.services.lifecycle.CacheRepository.cleanup_job_metadata", AsyncMock())
    mark_failed = mocker.patch("app.services.lifecycle.JobRepository.mark_job_failed", AsyncMock())

    await forcefully_reclaim_job("job-xyz", db_pool, k8s_provider, redis_client)

    k8s_provider.delete_job.assert_awaited_once_with("job-xyz")
    rel_cap.assert_awaited_once_with("job-xyz", redis_client)
    clean_meta.assert_awaited_once_with("job-xyz", redis_client)
    mark_failed.assert_awaited_once_with("job-xyz", db_pool)
