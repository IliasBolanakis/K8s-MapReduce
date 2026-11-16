"""
Service module for object storage operations.

This module abstracts interactions with MinIO/S3 compatible storage layers,
providing high-level utilities for secure client initialization and
multi-tenant file resolution.

This service operates strictly as a data access layer, as execution
classes are derived dynamically by the Orchestrator directly from validated
filenames. This ensures a leaner, faster, and more secure storage boundary.
"""

import logging
from minio import Minio
from minio.commonconfig import CopySource
from minio.error import S3Error
from app.core.config import settings

# Initialize module-level logger for storage events
logger = logging.getLogger(__name__)


class StorageService:
    """
    Encapsulates S3/MinIO operations and client initialization.

    This service provides a centralized interface for interacting with
    S3-compatible storage. It handles the low-level MinIO client configuration
    necessary for the Orchestrator to resolve physical file sizes, calculate
    byte offsets, promote fenced Reduce staging outputs, and clean up staging prefixes.
    """

    @staticmethod
    def get_client() -> Minio:
        """
        Initializes and returns a configured MinIO client.

        Uses the global application settings for endpoint, credentials,
        and security protocols to establish a secure connection to the
        storage cluster.

        Returns:
            Minio: An authenticated MinIO client instance ready for I/O operations.
        """
        return Minio(
            endpoint=settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=settings.MINIO_SECURE
        )

    @staticmethod
    def promote_staged_reduce_output(
        user_id: str,
        job_id: str,
        task_id: str,
        attempt_id: int,
    ) -> None:
        """
        Atomically promotes a fenced Reduce staging object to its canonical output key (ADR-06).

        Copies ``{user_id}/{job_id}/_attempts/part_{task_id}_attempt_{attempt_id}.staging``
        to ``{user_id}/{job_id}/result_part_{task_id}.txt`` inside the ``results`` bucket
        and deletes the staging object. Gracefully handles idempotent redeliveries if the
        canonical output already exists.

        :param user_id: Tenant UUID owning the output prefix.
        :type user_id: str
        :param job_id: MapReduce job UUID.
        :type job_id: str
        :param task_id: Reduce partition identifier.
        :type task_id: str
        :param attempt_id: Winning fenced attempt identifier (> 0).
        :type attempt_id: int
        """
        if attempt_id <= 0:
            return

        client = StorageService.get_client()
        bucket = "results"
        staging_key = f"{user_id}/{job_id}/_attempts/part_{task_id}_attempt_{attempt_id}.staging"
        canonical_key = f"{user_id}/{job_id}/result_part_{task_id}.txt"

        try:
            client.copy_object(bucket, canonical_key, CopySource(bucket, staging_key))
            client.remove_object(bucket, staging_key)
            logger.info(
                "Promoted fenced Reduce staging output '%s' -> '%s'.",
                staging_key,
                canonical_key,
                extra={"job_id": job_id, "task_id": task_id, "attempt_id": attempt_id},
            )
        except S3Error as err:
            if err.code == "NoSuchKey":
                try:
                    client.stat_object(bucket, canonical_key)
                    logger.debug(
                        "Staging key '%s' missing but canonical '%s' already present.",
                        staging_key,
                        canonical_key,
                    )
                    return
                except S3Error:
                    pass
            raise

    @staticmethod
    def cleanup_job_attempts_prefix(user_id: str, job_id: str) -> int:
        """
        Removes any leftover objects under ``{user_id}/{job_id}/_attempts/`` in ``results``.

        :param user_id: Tenant UUID owning the job prefix.
        :type user_id: str
        :param job_id: MapReduce job UUID.
        :type job_id: str
        :return: Count of deleted staging objects.
        :rtype: int
        """
        client = StorageService.get_client()
        bucket = "results"
        prefix = f"{user_id}/{job_id}/_attempts/"
        removed = 0
        for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
            obj_name = getattr(obj, "object_name", None)
            if obj_name:
                client.remove_object(bucket, obj_name)
                removed += 1
        return removed