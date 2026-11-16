package com.iliasbolan.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iliasbolan.infrastructure.RabbitMqConnectionManager;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Handles the transmission of task lifecycle events from the Worker node to the global Orchestrator.
 * <p>
 * Maintains a persistent AMQP {@link Connection} and publisher-confirm-enabled {@link Channel}
 * guarded by an internal monitor lock ({@code channelLock}), transparently recreating broken or
 * closed channels/connections and waiting up to {@code 5_000 ms} for broker publisher confirms
 * ({@link Channel#waitForConfirmsOrDie(long)}).
 * </p>
 * <p>
 * <b>Schema Synchronization &amp; Fencing:</b><br>
 * Outgoing JSON payloads align with the Manager's {@code WorkerSignal} schema, echoing both
 * the cryptographic {@code jobToken} and the monotonic {@code attemptId} so stale zombie
 * attempts are deterministically fenced by the Control Plane.
 * </p>
 * <p>
 * <b>Thread Safety:</b><br>
 * All channel acquisition, publish, confirm, recovery, and {@link #close()} operations are
 * serialized under {@code channelLock}, making instances safe to share concurrently between the
 * main consumer thread and the periodic sandbox heartbeat thread.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 3.0
 * @since 2026-04-25
 * @see com.iliasbolan.infrastructure.RabbitMqConnectionManager
 */
public class RabbitMqProducer implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMqProducer.class);

    /** Publisher confirm timeout in milliseconds. */
    static final long CONFIRM_TIMEOUT_MS = 5_000L;

    /** Shared Jackson mapper for serializing event payloads into standard JSON format. */
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final RabbitMqConnectionManager connectionManager;
    private final String eventQueue;
    private final Object channelLock = new Object();

    private Connection persistentConnection;
    private Channel persistentChannel;
    private volatile boolean closed = false;

    /**
     * Constructs a new {@code RabbitMqProducer} for worker-to-manager signaling.
     *
     * @param connectionManager The infrastructure manager responsible for AMQP connections.
     * @param eventQueue        The destination queue name where the Orchestrator listens for task updates.
     */
    public RabbitMqProducer(RabbitMqConnectionManager connectionManager, String eventQueue) {
        this.connectionManager = connectionManager;
        this.eventQueue = eventQueue;
    }

    /**
     * Transmits a successful task completion signal with default {@code attemptId = 0}.
     *
     * @param jobId             The universally unique identifier (UUID) of the Map-Reduce job.
     * @param taskId            The specific partition or chunk identifier of the completed task.
     * @param jobToken          The secure token echoed back to the Manager for signal authentication.
     * @param status            The execution status, typically {@code "COMPLETED"}.
     * @param workerBindAddress The gRPC network endpoint (e.g., {@code "node@@10.244.1.5:7337"}) for P2P shuffle fetches, or {@code null} for Reduce.
     */
    public void sendCompletionSignal(String jobId, String taskId, String jobToken, String status, String workerBindAddress) {
        sendCompletionSignal(jobId, taskId, jobToken, status, workerBindAddress, 0);
    }

    /**
     * Transmits a successful task completion signal to the global Orchestrator.
     *
     * @param jobId             The universally unique identifier (UUID) of the Map-Reduce job.
     * @param taskId            The specific partition or chunk identifier of the completed task.
     * @param jobToken          The secure token echoed back to the Manager for signal authentication.
     * @param status            The execution status, typically {@code "COMPLETED"}.
     * @param workerBindAddress The gRPC network endpoint (e.g., {@code "node@@10.244.1.5:7337"}) for P2P shuffle fetches, or {@code null} for Reduce.
     * @param attemptId         The monotonic task attempt identifier for Control Plane fencing.
     */
    public void sendCompletionSignal(String jobId, String taskId, String jobToken, String status,
                                     String workerBindAddress, int attemptId) {
        try {
            Map<String, Object> payload = new HashMap<>();
            payload.put("job_id", jobId);
            payload.put("task_id", taskId);
            payload.put("jobToken", jobToken);
            payload.put("status", status);
            payload.put("workerBindAddress", workerBindAddress);
            payload.put("attemptId", attemptId);

            publishWithConfirms(objectMapper.writeValueAsBytes(payload));

            logger.info("Successfully signaled task completion to Manager. Job: {}, Task: {}, Attempt: {}, gRPC Endpoint: {}",
                    jobId, taskId, attemptId, workerBindAddress);
        } catch (Exception e) {
            logger.error("Critical Failure: Could not transmit completion signal for task {} (attempt {}) to the Manager.",
                    taskId, attemptId, e);
        }
    }

    /**
     * Transmits a targeted task failure event with default {@code attemptId = 0}.
     *
     * @param jobId    The universally unique identifier (UUID) of the Map-Reduce job.
     * @param taskId   The specific partition or chunk identifier of the failed task.
     * @param jobToken The secure token echoed back to the Manager for signal authentication.
     * @param status   The execution status, typically {@code "FAILED"}.
     * @param error    The specific error diagnostic string for lineage recomputation.
     */
    public void sendErrorSignal(String jobId, String taskId, String jobToken, String status, String error) {
        sendErrorSignal(jobId, taskId, jobToken, status, error, 0);
    }

    /**
     * Transmits a targeted task failure event to the global Orchestrator.
     *
     * @param jobId     The universally unique identifier (UUID) of the Map-Reduce job.
     * @param taskId    The specific partition or chunk identifier of the failed task.
     * @param jobToken  The secure token echoed back to the Manager for signal authentication.
     * @param status    The execution status, typically {@code "FAILED"}.
     * @param error     The specific error diagnostic string for lineage recomputation.
     * @param attemptId The monotonic task attempt identifier for Control Plane fencing.
     */
    public void sendErrorSignal(String jobId, String taskId, String jobToken, String status,
                                String error, int attemptId) {
        try {
            Map<String, Object> payload = new HashMap<>();
            payload.put("job_id", jobId);
            payload.put("task_id", taskId);
            payload.put("jobToken", jobToken);
            payload.put("status", status);
            payload.put("error", error);
            payload.put("attemptId", attemptId);

            publishWithConfirms(objectMapper.writeValueAsBytes(payload));

            logger.warn("Transmitted targeted error signal to Manager. Job: {}, Task: {}, Attempt: {}, Error: {}",
                    jobId, taskId, attemptId, error);
        } catch (Exception e) {
            logger.error("Critical Failure: Could not transmit error signal for task {} (attempt {}) to the Manager.",
                    taskId, attemptId, e);
        }
    }

    /**
     * Transmits an {@code IN_PROGRESS} heartbeat signal with default {@code attemptId = 0}.
     *
     * @param jobId    The universally unique identifier (UUID) of the Map-Reduce job.
     * @param taskId   The specific partition or chunk identifier of the task in progress.
     * @param jobToken The secure token echoed back to the Manager for signal authentication.
     */
    public void sendProgressSignal(String jobId, String taskId, String jobToken) {
        sendProgressSignal(jobId, taskId, jobToken, 0);
    }

    /**
     * Transmits an {@code IN_PROGRESS} heartbeat signal to the global Orchestrator.
     *
     * @param jobId     The universally unique identifier (UUID) of the Map-Reduce job.
     * @param taskId    The specific partition or chunk identifier of the task in progress.
     * @param jobToken  The secure token echoed back to the Manager for signal authentication.
     * @param attemptId The monotonic task attempt identifier for Control Plane fencing.
     */
    public void sendProgressSignal(String jobId, String taskId, String jobToken, int attemptId) {
        try {
            Map<String, Object> payload = new HashMap<>();
            payload.put("job_id", jobId);
            payload.put("task_id", taskId);
            payload.put("jobToken", jobToken);
            payload.put("status", "IN_PROGRESS");
            payload.put("attemptId", attemptId);

            publishWithConfirms(objectMapper.writeValueAsBytes(payload));

            logger.debug("Transmitted IN_PROGRESS heartbeat to Manager. Job: {}, Task: {}, Attempt: {}",
                    jobId, taskId, attemptId);
        } catch (Exception e) {
            logger.warn("Failed to transmit heartbeat signal for task {} (attempt {}). Watchdog may intervene if prolonged.",
                    taskId, attemptId, e);
        }
    }

    /**
     * Publishes a serialized payload using the persistent confirm-enabled AMQP channel,
     * transparently recovering and retrying once if the cached channel or connection broke.
     *
     * @param messageBody UTF-8 JSON bytes to publish to {@code eventQueue}.
     * @throws IOException          If AMQP publishing or confirm waiting fails after recovery.
     * @throws TimeoutException     If connection/channel creation or confirm wait times out.
     * @throws InterruptedException If the calling thread is interrupted while waiting for broker confirms.
     */
    private void publishWithConfirms(byte[] messageBody) throws IOException, TimeoutException, InterruptedException {
        synchronized (channelLock) {
            for (int attempt = 0; attempt < 2; attempt++) {
                Channel channel = ensureOpenChannel();
                try {
                    channel.basicPublish("", eventQueue, null, messageBody);
                    channel.waitForConfirmsOrDie(CONFIRM_TIMEOUT_MS);
                    return;
                } catch (Exception ex) {
                    logger.warn("AMQP publish/confirm failed on attempt {} ({}); resetting persistent channel.",
                            attempt + 1, ex.getMessage());
                    resetPersistentResources();
                    if (attempt == 1) {
                        if (ex instanceof IOException ioEx) throw ioEx;
                        if (ex instanceof TimeoutException toEx) throw toEx;
                        if (ex instanceof InterruptedException intEx) throw intEx;
                        throw new IOException("AMQP publish/confirm failed after channel recovery", ex);
                    }
                }
            }
        }
    }

    /**
     * Ensures a live, confirm-enabled {@link Channel} is open under {@code channelLock}.
     *
     * @return An open AMQP {@link Channel} with publisher confirms enabled.
     * @throws IOException      If creating the connection or channel fails.
     * @throws TimeoutException If broker connection negotiation times out.
     */
    private Channel ensureOpenChannel() throws IOException, TimeoutException {
        if (closed) {
            throw new IllegalStateException("RabbitMqProducer is already closed");
        }
        if (persistentConnection == null || !persistentConnection.isOpen()) {
            closeQuietly(persistentChannel);
            closeQuietly(persistentConnection);
            persistentChannel = null;
            persistentConnection = connectionManager.createConnection();
        }
        if (persistentChannel == null || !persistentChannel.isOpen()) {
            closeQuietly(persistentChannel);
            persistentChannel = persistentConnection.createChannel();
            persistentChannel.confirmSelect();
        }
        return persistentChannel;
    }

    private void resetPersistentResources() {
        closeQuietly(persistentChannel);
        persistentChannel = null;
        if (persistentConnection == null || !persistentConnection.isOpen()) {
            closeQuietly(persistentConnection);
            persistentConnection = null;
        }
    }

    /**
     * Cleanly closes the persistent AMQP channel and connection.
     */
    @Override
    public void close() {
        synchronized (channelLock) {
            closed = true;
            closeQuietly(persistentChannel);
            closeQuietly(persistentConnection);
            persistentChannel = null;
            persistentConnection = null;
        }
    }

    private static void closeQuietly(Channel channel) {
        if (channel != null) {
            try {
                channel.close();
            } catch (Exception ignored) {
            }
        }
    }

    private static void closeQuietly(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception ignored) {
            }
        }
    }
}
