package com.iliasbolan.services;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iliasbolan.engine.TaskExecutor;
import com.iliasbolan.infrastructure.RabbitMqConnectionManager;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Orchestrates the lifecycle of task consumption from the RabbitMQ message broker.
 * <p>
 * Implements Competing Consumers ({@code basicQos(1)}), Poison Pill DLX protection,
 * single-partition Indexed Reduce immediate-exit mode, structured {@link MDC} logging
 * ({@code job_id}, {@code task_id}, {@code attempt_id}, {@code phase}), and ordered
 * {@code SIGTERM} graceful drain with preemption escalation and {@code requeue=true} NACKs.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 3.0
 * @since 2026-04-25
 * @see com.iliasbolan.engine.TaskExecutor
 */
public class RabbitMqConsumer {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMqConsumer.class);

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static final int MAX_RETRIES = 3;
    static final long DEFAULT_GRACEFUL_DRAIN_WAIT_MS = 20_000L;
    static final long DEFAULT_PREEMPT_DESTROY_WAIT_MS = 5_000L;

    private final RabbitMqConnectionManager connectionManager;
    private final String queueName;
    private final int idleTimeoutMillis;
    private final TaskExecutor taskExecutor;
    private final RabbitMqProducer eventProducer;
    private final boolean singlePartitionMode;

    private final Object channelLock = new Object();
    private final AtomicBoolean consuming = new AtomicBoolean(false);
    private final AtomicBoolean draining = new AtomicBoolean(false);
    private final AtomicReference<CountDownLatch> activeTaskLatch = new AtomicReference<>();

    private volatile Thread mainThread;
    private volatile CountDownLatch shutdownLatch = new CountDownLatch(1);
    private volatile CountDownLatch loopExitedLatch = new CountDownLatch(1);
    private volatile Channel activeChannel;
    private volatile String activeConsumerTag;
    private volatile long activeDeliveryTag = -1L;
    private volatile boolean activeDeliveryResolved = true;

    /**
     * Initializes the consumer in standard multi-task queue draining mode.
     *
     * @param connectionManager Factory for creating RabbitMQ connections and channels.
     * @param queueName         The specific AMQP queue to monitor for task intents.
     * @param idleTimeoutMillis Time in milliseconds of inactivity before triggering self-termination.
     * @param taskExecutor      The computational engine that processes localized tasks.
     * @param eventProducer     The signaling component for broadcasting task status back to the Manager.
     */
    public RabbitMqConsumer(RabbitMqConnectionManager connectionManager, String queueName,
                            int idleTimeoutMillis, TaskExecutor taskExecutor, RabbitMqProducer eventProducer) {
        this(connectionManager, queueName, idleTimeoutMillis, taskExecutor, eventProducer, false);
    }

    /**
     * Initializes the consumer with optional single-partition Indexed Reduce mode.
     *
     * @param connectionManager   Factory for creating RabbitMQ connections and channels.
     * @param queueName           The specific AMQP queue to monitor for task intents.
     * @param idleTimeoutMillis   Time in milliseconds of inactivity before triggering self-termination.
     * @param taskExecutor        The computational engine that processes localized tasks.
     * @param eventProducer       The signaling component for broadcasting task status back to the Manager.
     * @param singlePartitionMode When {@code true}, exits immediately after ACK of one successful task
     *                            without waiting for {@code idleTimeoutMillis}.
     */
    public RabbitMqConsumer(RabbitMqConnectionManager connectionManager, String queueName,
                            int idleTimeoutMillis, TaskExecutor taskExecutor, RabbitMqProducer eventProducer,
                            boolean singlePartitionMode) {
        this.connectionManager = connectionManager;
        this.queueName = queueName;
        this.idleTimeoutMillis = idleTimeoutMillis;
        this.taskExecutor = taskExecutor;
        this.eventProducer = eventProducer;
        this.singlePartitionMode = singlePartitionMode;

        logger.info("Initialized RabbitMqConsumer. Target Queue: {}, Idle Timeout: {}ms, SinglePartitionMode: {}",
                queueName, idleTimeoutMillis, singlePartitionMode);
    }

    /**
     * Signals the consumer loop to stop waiting and release AMQP resources cleanly.
     */
    public void stopConsuming() {
        shutdownLatch.countDown();
        Thread t = this.mainThread;
        if (t != null && Thread.currentThread() != t) {
            logger.info("Signaled consumer shutdown for queue {}", queueName);
        }
    }

    /**
     * Executes the ADR-14 {@code SIGTERM} graceful drain protocol using production timeouts
     * ({@code 20s} natural wait, {@code 5s} {@code destroy()} escalation wait).
     */
    public void initiateGracefulDrain() {
        initiateGracefulDrain(DEFAULT_GRACEFUL_DRAIN_WAIT_MS, DEFAULT_PREEMPT_DESTROY_WAIT_MS);
    }

    /**
     * Executes the ADR-14 {@code SIGTERM} graceful drain protocol with configurable timeouts:
     * <ol>
     * <li>Cancels new AMQP deliveries via {@code basicCancel}.</li>
     * <li>Waits up to {@code gracefulWaitMillis} for any in-flight task to complete naturally.</li>
     * <li>If still running, invokes {@link TaskExecutor#preemptActiveTask(long)} ({@code destroy()}
     * -&gt; {@code destroyWaitMillis} -&gt; {@code destroyForcibly()} + {@code .tmp} cleanup).</li>
     * <li>Ensures the preempted delivery receives {@code basicNack(deliveryTag, false, true)}
     * ({@code requeue=true}) before closing the AMQP channel.</li>
     * </ol>
     *
     * @param gracefulWaitMillis Maximum milliseconds to wait for an in-flight task to finish naturally.
     * @param destroyWaitMillis  Milliseconds to wait between {@code destroy()} and {@code destroyForcibly()}.
     */
    public void initiateGracefulDrain(long gracefulWaitMillis, long destroyWaitMillis) {
        if (!draining.compareAndSet(false, true)) {
            return;
        }
        logger.warn("Initiating graceful SIGTERM drain on queue {} (graceWait={}ms, destroyWait={}ms).",
                queueName, gracefulWaitMillis, destroyWaitMillis);

        cancelActiveConsumerQuietly();

        CountDownLatch inFlight = activeTaskLatch.get();
        if (inFlight != null && inFlight.getCount() > 0) {
            boolean finishedNaturally = false;
            try {
                finishedNaturally = inFlight.await(Math.max(0L, gracefulWaitMillis), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            if (!finishedNaturally) {
                logger.warn("In-flight task did not finish within {}ms; escalating preemption.", gracefulWaitMillis);
                taskExecutor.preemptActiveTask(destroyWaitMillis);

                try {
                    inFlight.await(2_000L, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

                nackPreemptedDeliveryIfUnresolved();
            }
        }

        taskExecutor.cleanupTempFiles();
        stopConsuming();

        try {
            loopExitedLatch.await(2_000L, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Establishes the RabbitMQ session and begins the blocking consumption loop.
     *
     * @throws Exception If the connection is refused or the channel fails to initialize.
     */
    public void startConsuming() throws Exception {
        this.mainThread = Thread.currentThread();
        this.shutdownLatch = new CountDownLatch(1);
        this.loopExitedLatch = new CountDownLatch(1);
        this.consuming.set(true);

        Thread idleChecker = null;
        try (Connection connection = connectionManager.createConnection();
             Channel channel = connection.createChannel()) {

            this.activeChannel = channel;

            Map<String, Object> queueArgs = new java.util.HashMap<>();
            queueArgs.put("x-dead-letter-exchange", "dead_letter_exchange");
            queueArgs.put("x-queue-mode", "lazy");
            queueArgs.put("x-expires", 86400000);

            channel.queueDeclare(queueName, true, false, false, queueArgs);
            channel.basicQos(1);

            AtomicLong lastActivity = new AtomicLong(System.currentTimeMillis());
            AtomicBoolean isProcessing = new AtomicBoolean(false);

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                long deliveryTag = delivery.getEnvelope().getDeliveryTag();
                if (draining.get()) {
                    synchronized (channelLock) {
                        try {
                            channel.basicNack(deliveryTag, false, true);
                        } catch (IOException e) {
                            logger.warn("Failed to requeue delivery {} during active drain.", deliveryTag, e);
                        }
                    }
                    return;
                }

                CountDownLatch taskLatch = new CountDownLatch(1);
                activeTaskLatch.set(taskLatch);
                synchronized (channelLock) {
                    activeDeliveryTag = deliveryTag;
                    activeDeliveryResolved = false;
                }

                isProcessing.set(true);
                boolean taskAcked = false;
                try {
                    taskAcked = handleMessage(channel, delivery);
                } finally {
                    lastActivity.set(System.currentTimeMillis());
                    isProcessing.set(false);
                    taskLatch.countDown();
                    activeTaskLatch.compareAndSet(taskLatch, null);
                }

                if (taskAcked && singlePartitionMode && !draining.get()) {
                    logger.info("Single-partition task acknowledged on {}; exiting consumer without waiting for idle timeout.",
                            queueName);
                    cancelActiveConsumerQuietly();
                    stopConsuming();
                }
            };

            this.activeConsumerTag = channel.basicConsume(queueName, false, deliverCallback, (CancelCallback) consumerTag -> {});

            idleChecker = getThread(lastActivity, isProcessing);
            idleChecker.start();

            try {
                shutdownLatch.await();
            } catch (InterruptedException e) {
                logger.warn("Main consumer thread interrupted. Releasing resources...");
                Thread.currentThread().interrupt();
            }
        } finally {
            if (idleChecker != null) {
                idleChecker.interrupt();
            }
            this.activeChannel = null;
            this.activeConsumerTag = null;
            this.consuming.set(false);
            this.loopExitedLatch.countDown();
        }
    }

    /**
     * Processes an individual message delivery, injecting MDC correlation, enforcing Poison Pill
     * limits, executing the task, and issuing ACK or NACK appropriately.
     *
     * @param channel  The active RabbitMQ channel for ACKing or NACKing.
     * @param delivery The encapsulated AMQP delivery containing the body and envelopes.
     * @return {@code true} if the task succeeded and was positively acknowledged; {@code false} otherwise.
     * @throws IOException If a channel-level communication error occurs.
     */
    private boolean handleMessage(Channel channel, Delivery delivery) throws IOException {
        byte[] rawBody = delivery.getBody();
        long deliveryTag = delivery.getEnvelope().getDeliveryTag();
        long deliveryCount = getCustomDeliveryCount(delivery.getProperties());

        String jobId = "UNKNOWN";
        String taskId = "UNKNOWN";
        int attemptId = 0;
        String taskType = "UNKNOWN";
        String jobToken = null;

        try {
            JsonNode jsonNode = objectMapper.readTree(rawBody);
            jobId = jsonNode.path("jobId").asText("UNKNOWN");
            taskId = jsonNode.path("taskId").asText("UNKNOWN");
            attemptId = jsonNode.path("attemptId").asInt(0);
            taskType = jsonNode.path("taskType").asText("UNKNOWN");
            jobToken = jsonNode.path("jobToken").asText(null);
        } catch (Exception e) {
            logger.warn("Metadata extraction failed; logging correlation will be degraded.");
        }

        try {
            MDC.put("job_id", jobId);
            MDC.put("task_id", taskId);
            MDC.put("attempt_id", String.valueOf(attemptId));
            MDC.put("phase", taskType);

            if (deliveryCount > MAX_RETRIES) {
                logger.error("Poison Pill Detected! Task exceeded MAX_RETRIES ({}). Routing to DLX.", MAX_RETRIES);
                eventProducer.sendErrorSignal(jobId, taskId, jobToken, "FAILED", "MAX_RETRIES_EXCEEDED", attemptId);
                synchronized (channelLock) {
                    if (!activeDeliveryResolved) {
                        channel.basicNack(deliveryTag, false, false);
                        activeDeliveryResolved = true;
                    }
                }
                return false;
            }

            try {
                String messageBody = new String(rawBody, StandardCharsets.UTF_8);
                taskExecutor.executeTask(messageBody);

                synchronized (channelLock) {
                    if (!activeDeliveryResolved) {
                        channel.basicAck(deliveryTag, false);
                        activeDeliveryResolved = true;
                    }
                }
                return true;

            } catch (Throwable t) {
                if (draining.get() || taskExecutor.isPreempted()) {
                    logger.warn("Task {} (attempt {}) preempted during SIGTERM drain; NACKing with requeue=true.",
                            taskId, attemptId);
                    synchronized (channelLock) {
                        if (!activeDeliveryResolved) {
                            channel.basicNack(deliveryTag, false, true);
                            activeDeliveryResolved = true;
                        }
                    }
                    return false;
                }

                logger.error("Task execution failed. Signaling Manager and NACKing delivery without local republish.", t);
                try {
                    eventProducer.sendErrorSignal(jobId, taskId, jobToken, "FAILED", t.getClass().getSimpleName(), attemptId);
                } catch (Exception networkError) {
                    logger.error("Failed to broadcast error signal. Proceeding with NACK.", networkError);
                } finally {
                    synchronized (channelLock) {
                        if (!activeDeliveryResolved) {
                            channel.basicNack(deliveryTag, false, false);
                            activeDeliveryResolved = true;
                        }
                    }
                }
                return false;
            }
        } finally {
            MDC.clear();
        }
    }

    private void cancelActiveConsumerQuietly() {
        Channel channel = this.activeChannel;
        String tag = this.activeConsumerTag;
        if (channel != null && tag != null) {
            synchronized (channelLock) {
                try {
                    channel.basicCancel(tag);
                    logger.info("Cancelled AMQP consumer tag {} on queue {}", tag, queueName);
                } catch (Exception e) {
                    logger.debug("Ignoring basicCancel error during shutdown: {}", e.getMessage());
                }
            }
        }
    }

    private void nackPreemptedDeliveryIfUnresolved() {
        Channel channel = this.activeChannel;
        synchronized (channelLock) {
            if (!activeDeliveryResolved && activeDeliveryTag >= 0 && channel != null) {
                try {
                    channel.basicNack(activeDeliveryTag, false, true);
                    activeDeliveryResolved = true;
                    logger.warn("NACKed preempted delivery {} with requeue=true from drain coordinator.", activeDeliveryTag);
                } catch (Exception e) {
                    logger.error("Failed to NACK preempted delivery {} with requeue=true.", activeDeliveryTag, e);
                }
            }
        }
    }

    private long getCustomDeliveryCount(AMQP.BasicProperties properties) {
        if (properties == null) {
            return 1;
        }
        Map<String, Object> headers = properties.getHeaders();
        if (headers != null && headers.containsKey("x-custom-retry-count")) {
            Object countStr = headers.get("x-custom-retry-count");
            if (countStr instanceof Number) {
                return ((Number) countStr).longValue();
            }
        }
        return 1;
    }

    @NotNull
    private Thread getThread(AtomicLong lastActivity, AtomicBoolean isProcessing) {
        return new Thread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted() && !draining.get()) {
                    if (isProcessing.get()) {
                        Thread.sleep(1000);
                        continue;
                    }

                    long elapsed = System.currentTimeMillis() - lastActivity.get();
                    long remainingToWait = idleTimeoutMillis - elapsed;

                    if (remainingToWait <= 0) {
                        logger.info("Inactivity threshold reached ({}s). Terminating pod.", (idleTimeoutMillis / 1000));
                        stopConsuming();
                        break;
                    }
                    Thread.sleep(Math.max(1000, remainingToWait));
                }
            } catch (InterruptedException e) {
                logger.info("Idle monitor interrupted. Shutting down.");
                Thread.currentThread().interrupt();
            }
        });
    }
}
