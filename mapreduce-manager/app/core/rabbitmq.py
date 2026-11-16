"""
Message broker lifecycle and dependency injection module.

This module manages the asynchronous connection, isolated consumer channels,
and bounded publisher/RPC channel pool for RabbitMQ using `aio-pika`.

Architecture (ADR-13):
    - **Consumer Channel Isolation:** `job_events_queue` (high-throughput worker
      signals) and `orchestration_events_queue` (phase transitions and capacity
      retries) each run on dedicated AMQP channels with independent QoS prefetch
      limits so slow orchestration retries cannot starve worker signal ingestion.
    - **Bounded Publisher/RPC Pool:** Outbound publishing and queue management
      operations lease channels from a bounded `asyncio.Queue` pool (default 8)
      via :func:`acquire_rabbitmq_channel`, automatically replacing closed
      channels on lease or return.
    - **Dead Letter Exchange (DLX) Topology:** Declares durable `dead_letter_exchange`
      and `dead_letter_queue` bindings and dynamic lazy job queues.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, List, Optional

import aio_pika
import aio_pika.abc
from app.core.config import settings

# Initialize module-level logger for broker events
logger = logging.getLogger(__name__)

# Private module-level variables to maintain connection, consumer channels, and pool state.
_global_mq_connection: Optional[aio_pika.abc.AbstractRobustConnection] = None
_global_mq_channel: Optional[aio_pika.abc.AbstractRobustChannel] = None
_job_events_channel: Optional[aio_pika.abc.AbstractRobustChannel] = None
_orchestration_events_channel: Optional[aio_pika.abc.AbstractRobustChannel] = None
_channel_pool: Optional[asyncio.Queue[aio_pika.abc.AbstractRobustChannel]] = None
_pool_channels: List[aio_pika.abc.AbstractRobustChannel] = []
_rr_index: int = 0


def _is_closed(resource: Any) -> bool:
    """
    Safely determines whether an aio-pika connection or channel is closed.

    Checks strictly against ``True`` so unconfigured mock objects in unit tests
    are not misclassified as closed resources.

    :param resource: An aio-pika connection or channel instance.
    :type resource: Any
    :return: True if the resource reports ``is_closed is True``; otherwise False.
    :rtype: bool
    """
    return getattr(resource, "is_closed", False) is True


def _replace_pool_channel(
    old_channel: aio_pika.abc.AbstractRobustChannel,
    new_channel: aio_pika.abc.AbstractRobustChannel,
) -> None:
    """
    Updates tracked channel references after replacing a closed pool channel.

    :param old_channel: The closed channel being retired.
    :type old_channel: aio_pika.abc.AbstractRobustChannel
    :param new_channel: The newly opened replacement channel.
    :type new_channel: aio_pika.abc.AbstractRobustChannel
    """
    global _global_mq_channel
    for idx, ch in enumerate(_pool_channels):
        if ch is old_channel:
            _pool_channels[idx] = new_channel
            break
    else:
        _pool_channels.append(new_channel)

    if _global_mq_channel is old_channel:
        _global_mq_channel = new_channel


async def setup_rabbitmq() -> None:
    """
    Establishes robust broker connections, isolated consumer channels,
    a bounded publisher/RPC channel pool, and the DLX topology.

    :return: None
    :rtype: None
    :raises Exception: If the initial connection to the RabbitMQ broker fails
        or if exchange/queue declaration is rejected by the server.
    """
    global _global_mq_connection, _global_mq_channel
    global _job_events_channel, _orchestration_events_channel
    global _channel_pool, _pool_channels, _rr_index

    await teardown_rabbitmq()

    logger.info("Connecting to RabbitMQ broker...")
    try:
        # connect_robust handles automatic heartbeats and connection recovery.
        connection = await aio_pika.connect_robust(settings.RABBITMQ_URL)
        _global_mq_connection = connection

        # Dedicated isolated consumer channels (ADR-13)
        _job_events_channel = await connection.channel()
        _orchestration_events_channel = await connection.channel()

        # Bounded publisher/RPC channel pool
        pool_size = max(1, settings.RABBITMQ_CHANNEL_POOL_SIZE)
        pool: asyncio.Queue[aio_pika.abc.AbstractRobustChannel] = asyncio.Queue(maxsize=pool_size)
        _pool_channels = []
        for _ in range(pool_size):
            ch = await connection.channel()
            _pool_channels.append(ch)
            await pool.put(ch)

        _channel_pool = pool
        _rr_index = 0
        setup_channel = _pool_channels[0]
        _global_mq_channel = setup_channel

        # --- DLX TOPOLOGY SETUP ---
        logger.info("Declaring Dead Letter Exchange (DLX) topology...")

        # 1. Declare the DLX (Fanout Exchange) so dead-lettered messages from dynamic queues route to DLQ
        dlx_exchange = await setup_channel.declare_exchange(
            "dead_letter_exchange",
            aio_pika.ExchangeType.FANOUT,
            durable=True,
        )

        # 2. Configure autonomous cleanup for the quarantine zone
        # Messages older than 7 days will be purged, and the queue is capped at 10k messages.
        dlq_args: Dict[str, Any] = {
            "x-message-ttl": 604800000,  # 7 days in milliseconds
            "x-max-length": 10000,       # Safety cap to prevent memory exhaustion
        }

        # 3. Declare the actual Dead Letter Queue (DLQ) and bind it to the DLX
        dlq = await setup_channel.declare_queue(
            "dead_letter_queue",
            durable=True,
            arguments=dlq_args,
        )
        await dlq.bind(dlx_exchange)

        logger.info(
            "RabbitMQ connection established: 2 isolated consumer channels, %d-channel pool, DLX ready.",
            pool_size,
        )
    except Exception as e:
        logger.error("Failed to initialize RabbitMQ: %s", str(e))
        await teardown_rabbitmq()
        raise


async def declare_job_queues(channel: aio_pika.abc.AbstractRobustChannel, job_id: str) -> None:
    """
    Dynamically declares isolated task queues for a specific MapReduce job.

    :param channel: The active RabbitMQ channel to declare queues on.
    :type channel: aio_pika.abc.AbstractRobustChannel
    :param job_id: The unique identifier for the job, used as a suffix.
    :type job_id: str
    :return: None
    :rtype: None
    """
    queue_args = {
        "x-dead-letter-exchange": "dead_letter_exchange",
        "x-queue-mode": "lazy",  # Maintain flat RAM usage during massive data inputs
        "x-expires": 86400000,   # 24-hour autonomous garbage collection (in milliseconds)
    }

    map_queue_name = f"map_tasks_{job_id}"
    reduce_queue_name = f"reduce_tasks_{job_id}"

    logger.info("Declaring optimized queue topology for job %s (Lazy Mode Enabled)...", job_id)

    await channel.declare_queue(map_queue_name, durable=True, arguments=queue_args)
    await channel.declare_queue(reduce_queue_name, durable=True, arguments=queue_args)


async def teardown_rabbitmq() -> None:
    """
    Gracefully closes isolated consumer channels, pooled publisher channels, and connection.

    :return: None
    :rtype: None
    """
    global _global_mq_connection, _global_mq_channel
    global _job_events_channel, _orchestration_events_channel
    global _channel_pool, _pool_channels, _rr_index

    channels_to_close: List[aio_pika.abc.AbstractRobustChannel] = []
    if _job_events_channel is not None:
        channels_to_close.append(_job_events_channel)
    if _orchestration_events_channel is not None:
        channels_to_close.append(_orchestration_events_channel)
    channels_to_close.extend(_pool_channels)
    if _global_mq_channel is not None and _global_mq_channel not in channels_to_close:
        channels_to_close.append(_global_mq_channel)

    for ch in channels_to_close:
        if not _is_closed(ch):
            try:
                await ch.close()
            except Exception as e:
                logger.debug("Ignoring error closing RabbitMQ channel during teardown: %s", str(e))

    if _global_mq_connection is not None and not _is_closed(_global_mq_connection):
        logger.info("Closing RabbitMQ connection...")
        await _global_mq_connection.close()

    _job_events_channel = None
    _orchestration_events_channel = None
    _channel_pool = None
    _pool_channels = []
    _rr_index = 0
    _global_mq_channel = None
    _global_mq_connection = None
    logger.info("RabbitMQ lifecycle terminated.")


async def get_job_events_channel() -> aio_pika.abc.AbstractRobustChannel:
    """
    Returns the dedicated AMQP channel for ``job_events_queue`` consumption.

    :return: Active dedicated consumer channel for worker telemetry events.
    :rtype: aio_pika.abc.AbstractRobustChannel
    :raises RuntimeError: If RabbitMQ has not been initialized or connection is closed.
    """
    global _job_events_channel
    if _global_mq_connection is None or _is_closed(_global_mq_connection):
        raise RuntimeError(
            "RabbitMQ connection is not initialized or has been closed. "
            "Ensure setup_rabbitmq() is called during application startup."
        )
    if _job_events_channel is None or _is_closed(_job_events_channel):
        logger.warning("Dedicated job_events_queue channel was closed; opening replacement channel.")
        _job_events_channel = await _global_mq_connection.channel()
    return _job_events_channel


async def get_orchestration_events_channel() -> aio_pika.abc.AbstractRobustChannel:
    """
    Returns the dedicated AMQP channel for ``orchestration_events_queue`` consumption.

    :return: Active dedicated consumer channel for phase-transition events.
    :rtype: aio_pika.abc.AbstractRobustChannel
    :raises RuntimeError: If RabbitMQ has not been initialized or connection is closed.
    """
    global _orchestration_events_channel
    if _global_mq_connection is None or _is_closed(_global_mq_connection):
        raise RuntimeError(
            "RabbitMQ connection is not initialized or has been closed. "
            "Ensure setup_rabbitmq() is called during application startup."
        )
    if _orchestration_events_channel is None or _is_closed(_orchestration_events_channel):
        logger.warning("Dedicated orchestration_events_queue channel was closed; opening replacement channel.")
        _orchestration_events_channel = await _global_mq_connection.channel()
    return _orchestration_events_channel


@asynccontextmanager
async def acquire_rabbitmq_channel() -> AsyncIterator[aio_pika.abc.AbstractRobustChannel]:
    """
    Leases a healthy AMQP channel from the bounded publisher/RPC pool.

    Automatically detects closed channels on both lease acquisition and return,
    replacing them against the active connection so pooled operations recover
    seamlessly from broker channel faults.

    :yields: An open, exclusive RabbitMQ channel for the duration of the context block.
    :rtype: AsyncIterator[aio_pika.abc.AbstractRobustChannel]
    :raises RuntimeError: If the pool has not been initialized or has been torn down.
    """
    if _channel_pool is None:
        if _global_mq_channel is not None and not _is_closed(_global_mq_channel):
            yield _global_mq_channel
            return
        logger.error("Attempted to acquire RabbitMQ channel before initialization or after closure.")
        raise RuntimeError(
            "RabbitMQ channel pool is not initialized or has been closed. "
            "Ensure setup_rabbitmq() is called during application startup."
        )

    channel = await _channel_pool.get()
    try:
        if _is_closed(channel):
            if _global_mq_connection is None or _is_closed(_global_mq_connection):
                raise RuntimeError("RabbitMQ connection is closed; cannot replace closed channel.")
            logger.warning("AMQP pool recovery: replacing closed RabbitMQ channel on lease.")
            replacement = await _global_mq_connection.channel()
            _replace_pool_channel(channel, replacement)
            channel = replacement

        yield channel
    finally:
        if _is_closed(channel) and _global_mq_connection is not None and not _is_closed(_global_mq_connection):
            try:
                logger.warning("AMQP pool recovery: replacing closed RabbitMQ channel on return.")
                replacement = await _global_mq_connection.channel()
                _replace_pool_channel(channel, replacement)
                channel = replacement
            except Exception as e:
                logger.error("Failed to replace closed RabbitMQ channel on pool return: %s", str(e))
        await _channel_pool.put(channel)


async def get_rabbitmq_channel() -> aio_pika.abc.AbstractRobustChannel:
    """
    FastAPI dependency and backward-compatible accessor returning a healthy AMQP channel.

    Draws round-robin across the bounded publisher channel pool (replacing closed
    channels automatically) so HTTP handlers and legacy callers benefit from channel
    multiplexing without signature changes.

    :return: An open RabbitMQ channel from the pool.
    :rtype: aio_pika.abc.AbstractRobustChannel
    :raises RuntimeError: If RabbitMQ is not initialized or all channels are unavailable.
    """
    global _rr_index, _global_mq_channel

    if _pool_channels:
        idx = _rr_index % len(_pool_channels)
        _rr_index = (_rr_index + 1) % len(_pool_channels)
        channel = _pool_channels[idx]
        if _is_closed(channel):
            if _global_mq_connection is None or _is_closed(_global_mq_connection):
                raise RuntimeError("RabbitMQ connection is closed; cannot replace closed channel.")
            logger.warning("AMQP pool recovery: replacing closed RabbitMQ channel during round-robin lease.")
            replacement = await _global_mq_connection.channel()
            _replace_pool_channel(channel, replacement)
            channel = replacement
        return channel

    if _global_mq_channel is None or _is_closed(_global_mq_channel):
        logger.error("Attempted to access RabbitMQ channel before initialization or after closure.")
        raise RuntimeError(
            "RabbitMQ channel is not initialized or has been closed. "
            "Ensure the lifespan event is properly configured."
        )

    return _global_mq_channel
