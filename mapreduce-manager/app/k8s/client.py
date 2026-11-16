"""
Kubernetes cluster orchestration module.

This module provides a high-level abstraction layer for managing distributed worker
resources within a Kubernetes cluster. It leverages the ``kubernetes-asyncio``
library for non-blocking API interactions and implements a Strategy pattern
via the ``KubernetesProvider`` interface to support seamless local mocking.

Architecture Upgrades:
    - **ADR-04 (Indexed Reduce Jobs & Partition Queues):** Supports Kubernetes
      ``completionMode: Indexed`` Batch Jobs with ``backoffLimitPerIndex: 3``
      and ``restartPolicy: Never`` for normal Reduce phases, while preserving
      ``backoffLimit: 4`` and ``restartPolicy: OnFailure`` for Map and targeted
      single-partition recovery Jobs. Cleans up ``reduce_tasks_{job_id}_{p_id}``
      queues safely.
    - **ADR-11 (Pooled ApiClient & Async Job Watch):** Shares one lifecycle-managed
      ``client.ApiClient()`` across all operations on ``RealKubernetesProvider``,
      replaces 5s polling in :meth:`wait_for_job_completion` with an event-driven
      ``kubernetes_asyncio.watch.Watch`` (handling ``410 Gone`` relists), and
      batches Reconciler pod checks into a single cluster-wide pod list call.
"""

import asyncio
import logging
import random
import string
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Optional, Set
from urllib.parse import urlparse

import aiofiles
import yaml
from jinja2 import Template, TemplateError
from kubernetes_asyncio import client, config, watch
from kubernetes_asyncio.client.exceptions import ApiException

from app.core.config import settings
from app.core.rabbitmq import acquire_rabbitmq_channel
from app.core.redis import get_redis_client
from app.repositories.cache_repository import CacheRepository

# Initialize the module-level logger for cluster events
logger = logging.getLogger(__name__)


class KubernetesProvider(ABC):
    """
    Abstract base class defining the contract for Kubernetes resource providers.

    Defines the essential lifecycle operations for managing worker pods across
    different deployment environments (e.g., local, cloud, or mock).
    """

    @abstractmethod
    async def spawn_workers(
        self,
        job_id: str,
        replicas: int,
        phase: str = "map",
        partition_id: Optional[int] = None,
        target_node: Optional[str] = None,
        indexed: bool = False,
    ) -> None:
        """
        Provision worker pods for a specific execution phase.

        Args:
            job_id: The unique identifier for the MapReduce job.
            replicas: The number of worker units (or Indexed completions) requested.
            phase: The current execution phase (e.g., ``'map'`` or ``'reduce'``).
            partition_id: Optional partition identifier for targeted recovery jobs.
            target_node: Physical node hostname hint for Data Locality Affinity.
            indexed: Whether to submit a Kubernetes ``Indexed`` Batch Job.
        """
        pass

    @abstractmethod
    async def wait_for_job_completion(self, job_id: str) -> None:
        """
        Block asynchronously until all workers for a specific job have finished.

        Args:
            job_id: The job identifier to monitor.
        """
        pass

    @abstractmethod
    async def delete_job(
        self,
        job_id: str,
        phase: Optional[str] = None,
        num_reducers: Optional[int] = None,
    ) -> None:
        """
        Clean up Kubernetes resources and message queues associated with a specific job.

        Args:
            job_id: The job identifier whose resources should be purged.
            phase: Optional execution phase (``'map'`` or ``'reduce'``) to scope deletion.
            num_reducers: Optional partition count for deterministic Reduce queue cleanup.
        """
        pass

    async def close(self) -> None:
        """
        Releases any pooled HTTP/client resources held by the provider.
        """
        return None


class RealKubernetesProvider(KubernetesProvider):
    """
    Production-grade Kubernetes provider communicating with the Batch V1 API.

    Shares one lifecycle-managed ``kubernetes_asyncio.client.ApiClient`` across
    all operations and uses async ``Watch`` streams for job completion monitoring.
    """

    def __init__(self) -> None:
        # In-memory template cache to prevent disk I/O bottlenecks during concurrent spawns
        self._worker_template_cache: Optional[str] = None
        # Lifecycle-managed pooled ApiClient (initialized lazily after config load)
        self._api_client: Optional[client.ApiClient] = None

    def _get_api_client(self) -> client.ApiClient:
        """
        Lazily initializes and returns the shared ``kubernetes_asyncio.client.ApiClient``.

        :return: Shared async Kubernetes API client.
        :rtype: client.ApiClient
        """
        if self._api_client is None:
            self._api_client = client.ApiClient()
        return self._api_client

    async def close(self) -> None:
        """
        Closes the shared ``ApiClient`` HTTP session pool during shutdown.
        """
        if self._api_client is not None:
            logger.info("Closing pooled Kubernetes ApiClient...")
            await self._api_client.close()
            self._api_client = None

    async def spawn_workers(
        self,
        job_id: str,
        replicas: int,
        phase: str = "map",
        partition_id: Optional[int] = None,
        target_node: Optional[str] = None,
        indexed: bool = False,
    ) -> None:
        """
        Renders a YAML manifest and submits a Batch V1 Job to the Kubernetes API.

        For ``indexed=True`` Reduce jobs, renders ``completionMode: Indexed``,
        ``completions: replicas``, ``parallelism: min(replicas, MAX_TOTAL_WORKERS)``,
        ``backoffLimitPerIndex: 3``, omits top-level ``backoffLimit``, and sets
        ``restartPolicy: Never``. For Map and targeted recovery jobs, sets
        ``backoffLimit: 4`` and ``restartPolicy: OnFailure``.
        """
        actual_replicas = min(replicas, settings.MAX_TOTAL_WORKERS)
        completions = replicas if indexed else actual_replicas

        if replicas > actual_replicas:
            logger.info(
                "Requested %d %s workers, capping parallelism at MAX_TOTAL_WORKERS (%d).",
                replicas,
                phase,
                actual_replicas,
            )

        node_msg = f" pinned to {target_node}" if target_node else ""
        logger.info(
            "Requesting creation of %d %s worker pods%s (indexed=%s, completions=%d).",
            actual_replicas,
            phase,
            node_msg,
            indexed,
            completions,
        )

        # Generate unique suffix to avoid job name collisions in K8s
        random_suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=5))
        safe_job_id = str(job_id)[:8]

        if partition_id is not None:
            k8s_job_name = f"worker-{phase}-{safe_job_id}-p{partition_id}-{random_suffix}"
        else:
            k8s_job_name = f"worker-{phase}-{safe_job_id}-{random_suffix}"

        try:
            # --- CACHE CHECK (O(1) Memory Access) ---
            if self._worker_template_cache is None:
                template_path = Path(__file__).parent / "templates" / "worker-template.yaml"
                async with aiofiles.open(template_path, "r") as file:
                    self._worker_template_cache = await file.read()

            template_str = self._worker_template_cache

            # Resolve internal networking for Docker/Kubernetes environments
            mq_host = urlparse(settings.RABBITMQ_URL).hostname or "rabbitmq"
            minio_endpoint = settings.MINIO_ENDPOINT

            if mq_host in ("localhost", "127.0.0.1"):
                mq_host = "host.docker.internal"
            if minio_endpoint.startswith("localhost") or minio_endpoint.startswith("127.0.0.1"):
                minio_endpoint = (
                    minio_endpoint.replace("localhost", "host.docker.internal")
                    .replace("127.0.0.1", "host.docker.internal")
                )

            # Render manifest using Jinja2 for dynamic configuration injection
            jinja_template = Template(template_str)  # type: ignore
            manifest_str = jinja_template.render(
                JOB_NAME=k8s_job_name,
                REPLICAS=actual_replicas,
                COMPLETIONS=completions,
                INDEXED=indexed,
                PARTITION_ID=partition_id,
                JOB_ID=str(job_id),
                PHASE=phase,
                K8S_NAMESPACE=settings.K8S_NAMESPACE,
                RABBITMQ_HOST=mq_host,
                MINIO_ENDPOINT=minio_endpoint,
                MINIO_ACCESS_KEY=settings.MINIO_ACCESS_KEY,
                MINIO_SECRET_KEY=settings.MINIO_SECRET_KEY,
                TARGET_NODE=target_node,
            )

            job_dict: Dict[str, Any] = yaml.safe_load(manifest_str)

            batch_v1 = client.BatchV1Api(self._get_api_client())
            await batch_v1.create_namespaced_job(
                namespace=settings.K8S_NAMESPACE,
                body=job_dict,  # type: ignore[arg-type]
            )
            logger.info("Successfully submitted K8s Job: %s", k8s_job_name)

        except ApiException as e:
            logger.error("Kubernetes API rejected manifest for %s: %s", k8s_job_name, e.body)
            raise
        except (FileNotFoundError, yaml.YAMLError, TemplateError) as e:
            logger.error("Configuration or Template error in %s worker spawn: %s", phase, str(e))
            raise RuntimeError(f"Failed to render or load worker manifest: {e}") from e
        except Exception as e:
            logger.error("Unexpected error spawning workers: %s", str(e))
            raise

    @staticmethod
    def _is_k8s_job_terminal(k8s_job: Any) -> bool:
        """
        Determines whether a Kubernetes Batch Job has reached a terminal state.

        :param k8s_job: Kubernetes V1Job object.
        :type k8s_job: Any
        :return: True if succeeded >= expected completions or Job has Complete/Failed condition.
        :rtype: bool
        """
        spec = getattr(k8s_job, "spec", None)
        status = getattr(k8s_job, "status", None)
        expected_completions = (getattr(spec, "completions", None) or 1) if spec else 1
        current_successes = (getattr(status, "succeeded", None) or 0) if status else 0

        if current_successes >= expected_completions:
            return True

        conditions = (getattr(status, "conditions", None) or []) if status else []
        for cond in conditions:
            if getattr(cond, "type", None) in ("Complete", "Failed") and getattr(cond, "status", None) == "True":
                return True
        return False

    async def wait_for_job_completion(self, job_id: str) -> None:
        """
        Watches Kubernetes Batch V1 Job events until all Jobs for ``job_id`` exit (ADR-11).

        Performs an initial ``list_namespaced_job`` to capture ``metadata.resource_version``
        and short-circuit if already terminal, then opens an async ``watch.Watch`` stream.
        Handles ``410 Gone`` ``ApiException`` by resetting ``resource_version`` and relisting.

        :param job_id: The job ID used in the label selector.
        :type job_id: str
        """
        safe_job_id = str(job_id)[:8]
        logger.info("Starting async K8s Watch for Job completion (ID: %s)...", safe_job_id)

        label_selector = f"job_id={job_id}"
        timeout_seconds = settings.GLOBAL_JOB_TIMEOUT
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_seconds

        batch_v1 = client.BatchV1Api(self._get_api_client())
        resource_version: Optional[str] = None

        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                logger.error("Timeout reached watching job %s after %ss.", safe_job_id, timeout_seconds)
                return

            try:
                job_list = await batch_v1.list_namespaced_job(
                    namespace=settings.K8S_NAMESPACE,
                    label_selector=label_selector,
                )
                list_meta = getattr(job_list, "metadata", None)
                resource_version = getattr(list_meta, "resource_version", None) if list_meta else None

                jobs_by_name: Dict[str, Any] = {}
                for k8s_job in getattr(job_list, "items", None) or []:
                    meta = getattr(k8s_job, "metadata", None)
                    name = getattr(meta, "name", None) or str(id(k8s_job))
                    jobs_by_name[name] = k8s_job

                if jobs_by_name and all(self._is_k8s_job_terminal(j) for j in jobs_by_name.values()):
                    logger.info("All worker pods for job %s exited successfully.", safe_job_id)
                    return

                watch_timeout = max(1, int(min(remaining, 300)))
                stream_kwargs: Dict[str, Any] = {
                    "namespace": settings.K8S_NAMESPACE,
                    "label_selector": label_selector,
                    "timeout_seconds": watch_timeout,
                }
                if resource_version:
                    stream_kwargs["resource_version"] = resource_version

                async with watch.Watch() as w:
                    async for event in w.stream(batch_v1.list_namespaced_job, **stream_kwargs):
                        event_type = event.get("type")
                        obj = event.get("object")
                        if obj is None:
                            continue

                        meta = getattr(obj, "metadata", None)
                        name = getattr(meta, "name", None) or str(id(obj))
                        if meta and getattr(meta, "resource_version", None):
                            resource_version = meta.resource_version

                        if event_type == "DELETED":
                            jobs_by_name.pop(name, None)
                            continue

                        jobs_by_name[name] = obj
                        if jobs_by_name and all(self._is_k8s_job_terminal(j) for j in jobs_by_name.values()):
                            logger.info("All worker pods for job %s exited successfully.", safe_job_id)
                            w.stop()
                            return

                        if deadline - loop.time() <= 0:
                            w.stop()
                            break

            except ApiException as e:
                if getattr(e, "status", None) == 410:
                    logger.warning(
                        "K8s Watch 410 Gone for job %s (resource_version=%s); resetting and relisting.",
                        safe_job_id,
                        resource_version,
                    )
                    resource_version = None
                    continue
                logger.warning("Transient K8s API error watching job %s: %s", safe_job_id, getattr(e, "body", str(e)))
                await asyncio.sleep(min(2.0, max(0.0, deadline - loop.time())))
            except Exception as e:
                logger.error("Unexpected error during K8s watch for job %s: %s", safe_job_id, str(e))
                raise

    @staticmethod
    async def _safe_delete_queue(mq_channel: Any, queue_name: str) -> None:
        """
        Deletes a RabbitMQ queue safely, ignoring errors if the queue is already absent.

        :param mq_channel: Active AMQP channel.
        :type mq_channel: Any
        :param queue_name: Name of the queue to delete.
        :type queue_name: str
        """
        try:
            await mq_channel.queue_delete(queue_name)
        except Exception as e:
            logger.debug("Queue '%s' deletion skipped/already absent: %s", queue_name, str(e))

    async def _resolve_reduce_partition_ids(
        self,
        job_id: str,
        num_reducers: Optional[int],
    ) -> Set[int]:
        """
        Resolves Reduce partition IDs to delete per-partition AMQP queues cleanly.

        :param job_id: MapReduce job identifier.
        :type job_id: str
        :param num_reducers: Optional explicit reducer count.
        :type num_reducers: Optional[int]
        :return: Set of partition indices ``0..N-1`` to delete.
        :rtype: Set[int]
        """
        partition_ids: Set[int] = set()
        if num_reducers is not None and num_reducers > 0:
            partition_ids.update(range(num_reducers))
            return partition_ids

        try:
            redis_client = await get_redis_client()
            cached_reducers = await CacheRepository.get_cached_num_reducers(job_id, redis_client)
            if cached_reducers is not None and cached_reducers > 0:
                partition_ids.update(range(cached_reducers))
            attempt_partitions = await CacheRepository.get_reduce_partition_ids_from_attempts(job_id, redis_client)
            partition_ids.update(attempt_partitions)
        except Exception as e:
            logger.debug("Unable to resolve reduce partition IDs from Redis for job %s: %s", job_id, str(e))

        return partition_ids

    async def delete_job(
        self,
        job_id: str,
        phase: Optional[str] = None,
        num_reducers: Optional[int] = None,
    ) -> None:
        """
        Purges Kubernetes Job resources and dynamic message queues
        (including per-partition ``reduce_tasks_{job_id}_{p_id}`` queues)
        identified by ``job_id`` (optionally scoped to a specific phase).

        :param job_id: The job ID used to find resources for deletion.
        :type job_id: str
        :param phase: Optional execution phase (``'map'`` or ``'reduce'``) to limit cleanup.
        :type phase: Optional[str]
        :param num_reducers: Optional partition count for deterministic Reduce queue cleanup.
        :type num_reducers: Optional[int]
        """
        logger.info("Initiating resource cleanup for job %s (phase=%s).", job_id, phase or "all")

        safe_job_id = str(job_id)[:8]

        # Clean up dynamic RabbitMQ queues associated with the job/phase
        try:
            partition_ids: Set[int] = set()
            if phase is None or phase == "reduce":
                partition_ids = await self._resolve_reduce_partition_ids(job_id, num_reducers)

            async with acquire_rabbitmq_channel() as mq_channel:
                if phase is None or phase == "map":
                    await self._safe_delete_queue(mq_channel, f"map_tasks_{job_id}")
                if phase is None or phase == "reduce":
                    await self._safe_delete_queue(mq_channel, f"reduce_tasks_{job_id}")
                    for p_id in sorted(partition_ids):
                        await self._safe_delete_queue(mq_channel, f"reduce_tasks_{job_id}_{p_id}")

            logger.info(
                "Successfully deleted dynamic RabbitMQ queues (phase=%s, partitions=%d) for job %s.",
                phase or "all",
                len(partition_ids),
                safe_job_id,
            )
        except Exception as e:
            logger.error("Failed to delete RabbitMQ queues for job %s: %s", safe_job_id, str(e))

        batch_v1 = client.BatchV1Api(self._get_api_client())
        try:
            job_list = await batch_v1.list_namespaced_job(
                namespace=settings.K8S_NAMESPACE,
                label_selector=f"job_id={job_id}",
            )

            for k8s_job in job_list.items:
                if not k8s_job.metadata or not k8s_job.metadata.name:
                    continue

                if phase is not None:
                    labels = k8s_job.metadata.labels or {}
                    job_phase = labels.get("phase")
                    if job_phase is not None:
                        if job_phase != phase:
                            continue
                    elif not k8s_job.metadata.name.startswith(f"worker-{phase}-"):
                        continue

                await batch_v1.delete_namespaced_job(
                    name=k8s_job.metadata.name,
                    namespace=settings.K8S_NAMESPACE,
                    propagation_policy="Background",
                )
                logger.info("Deleted K8s Job resource: %s", k8s_job.metadata.name)

        except ApiException as e:
            logger.error("Failed to clean up Kubernetes resources for job %s: %s", job_id, e.body)


class MockKubernetesProvider(KubernetesProvider):
    """
    Simulation provider for offline development and local unit testing.

    Mocks all Kubernetes API interactions while maintaining standard logging
    and asynchronous delays to mimic real cluster behavior.
    """

    async def spawn_workers(
        self,
        job_id: str,
        replicas: int,
        phase: str = "map",
        partition_id: Optional[int] = None,
        target_node: Optional[str] = None,
        indexed: bool = False,
    ) -> None:
        """Logs the simulation of worker pod creation."""
        actual_replicas = min(replicas, settings.MAX_TOTAL_WORKERS)
        part_info = f" (Partition {partition_id})" if partition_id is not None else ""
        node_info = f" [Pinned to: {target_node}]" if target_node else ""
        indexed_info = " [Indexed]" if indexed else ""
        logger.info(
            "[MOCK] Spawning %d %s workers for job %s%s%s%s",
            actual_replicas,
            phase,
            job_id,
            part_info,
            node_info,
            indexed_info,
        )

    async def wait_for_job_completion(self, job_id: str) -> None:
        """Simulates an asynchronous wait period for job completion."""
        logger.info("[MOCK] Waiting for job %s completion...", job_id)
        await asyncio.sleep(1)

    async def delete_job(
        self,
        job_id: str,
        phase: Optional[str] = None,
        num_reducers: Optional[int] = None,
    ) -> None:
        """Logs the simulation of resource and queue cleanup."""
        logger.info(
            "[MOCK] Cleaning up K8s resources and MQ queues for job %s (phase=%s, num_reducers=%s)",
            job_id,
            phase or "all",
            num_reducers,
        )


# Global singleton instance holder for the provider
_global_k8s_provider: Optional[KubernetesProvider] = None


async def setup_k8s() -> None:
    """
    Analyzes the environment and initializes the appropriate Kubernetes provider.

    Discovery sequence:
        1. Checks if ``MOCK_K8S`` setting is active.
        2. Attempts to load in-cluster configuration (ServiceAccount).
        3. Attempts to load local kubeconfig (Dev environment).
        4. Falls back to Mock if no cluster is reachable.
    """
    global _global_k8s_provider

    if settings.MOCK_K8S:
        logger.info("Initializing MockKubernetesProvider (MOCK_K8S=True)")
        _global_k8s_provider = MockKubernetesProvider()
        return

    try:
        config.load_incluster_config()
        logger.info("Detected In-Cluster configuration. Using RealKubernetesProvider.")
        _global_k8s_provider = RealKubernetesProvider()
    except (config.ConfigException, AttributeError):
        try:
            await config.load_kube_config()
            logger.info("Detected Local Kubeconfig. Using RealKubernetesProvider.")
            _global_k8s_provider = RealKubernetesProvider()
        except (config.ConfigException, OSError):
            logger.warning("No cluster connectivity detected. Falling back to Mock.")
            _global_k8s_provider = MockKubernetesProvider()


async def teardown_k8s() -> None:
    """
    Gracefully closes the active Kubernetes provider's pooled ``ApiClient`` on shutdown.
    """
    global _global_k8s_provider
    if _global_k8s_provider is not None:
        await _global_k8s_provider.close()
        _global_k8s_provider = None


async def get_k8s_provider() -> KubernetesProvider:
    """
    Retrieves the active Kubernetes provider instance.

    Returns:
        KubernetesProvider: The initialized global provider.

    Raises:
        RuntimeError: If setup_k8s() has not been called prior to this request.
    """
    if _global_k8s_provider is None:
        raise RuntimeError("Kubernetes provider is not initialized.")
    return _global_k8s_provider


async def get_active_pod_counts_by_job() -> Dict[str, int]:
    """
    Queries Kubernetes once for all worker pods (``app=worker-node``) and returns
    the active (``Running``/``Pending``) pod count grouped by ``labels["job_id"]`` (ADR-11).

    Reuses the shared ``ApiClient`` from ``RealKubernetesProvider`` when active.

    :return: Mapping of ``job_id`` string to active pod count.
    :rtype: Dict[str, int]
    """
    active_phases = {"Running", "Pending"}
    counts: Dict[str, int] = {}

    async def _list_with_client(api_client: client.ApiClient) -> Any:
        core_v1 = client.CoreV1Api(api_client)
        return await core_v1.list_namespaced_pod(
            namespace=settings.K8S_NAMESPACE,
            label_selector="app=worker-node",
        )

    if isinstance(_global_k8s_provider, RealKubernetesProvider):
        pods = await _list_with_client(_global_k8s_provider._get_api_client())
    else:
        async with client.ApiClient() as api_client:
            pods = await _list_with_client(api_client)

    for pod in getattr(pods, "items", None) or []:
        metadata = getattr(pod, "metadata", None)
        labels = (getattr(metadata, "labels", None) or {}) if metadata else {}
        status = getattr(pod, "status", None)
        pod_phase = getattr(status, "phase", None) if status else None
        pod_job_id = labels.get("job_id")
        if pod_job_id and pod_phase in active_phases:
            counts[pod_job_id] = counts.get(pod_job_id, 0) + 1

    return counts


async def get_active_pod_count(job_id: str) -> int:
    """
    Queries the Kubernetes API for the live count of active pods belonging to a job.

    :param job_id: Target job identifier.
    :type job_id: str
    :return: Number of ``Running`` or ``Pending`` pods for ``job_id``.
    :rtype: int
    """
    label_selector = f"job_id={job_id}"
    active_phases = {"Running", "Pending"}

    async def _list_job_pods(api_client: client.ApiClient) -> Any:
        core_v1 = client.CoreV1Api(api_client)
        return await core_v1.list_namespaced_pod(
            namespace=settings.K8S_NAMESPACE,
            label_selector=label_selector,
        )

    if isinstance(_global_k8s_provider, RealKubernetesProvider):
        pods = await _list_job_pods(_global_k8s_provider._get_api_client())
    else:
        async with client.ApiClient() as api_client:
            pods = await _list_job_pods(api_client)

    return len([p for p in pods.items if p.status.phase in active_phases])
