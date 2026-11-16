package com.iliasbolan;

import com.iliasbolan.engine.TaskExecutor;
import com.iliasbolan.infrastructure.MinioConnectionManager;
import com.iliasbolan.infrastructure.RabbitMqConnectionManager;
import com.iliasbolan.services.RabbitMqConsumer;
import com.iliasbolan.services.RabbitMqProducer;
import com.iliasbolan.services.S3ClientService;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Primary entry point for the distributed Map-Reduce Worker node.
 * <p>
 * Bootstraps RabbitMQ/MinIO clients, resolves deterministic queue bindings (including
 * K8s {@code Indexed} Reduce {@code JOB_COMPLETION_INDEX} and targeted {@code PARTITION_ID}
 * overrides), exposes the Kubernetes {@code /health} probe, and orchestrates ordered
 * {@code SIGTERM} graceful draining.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 3.0
 * @since 2026-04-25
 */
public class App {

    private static final Logger logger = LoggerFactory.getLogger(App.class);

    /**
     * Resolved AMQP queue configuration derived from Kubernetes worker environment variables.
     *
     * @param taskQueue           Target RabbitMQ queue name.
     * @param singlePartitionMode {@code true} when bound to a single Reduce partition index.
     */
    record QueueConfig(String taskQueue, boolean singlePartitionMode) {}

    /**
     * Application execution entry point.
     *
     * @param args Command line arguments (configuration is derived from environment variables).
     */
    public static void main(String[] args) {
        logger.info("====== [ Map-Reduce Ephemeral Compute Worker Initializing ] ======");

        Map<String, String> env = System.getenv();
        String rabbitHost = env.getOrDefault("RABBITMQ_HOST", "localhost");
        String rabbitUser = env.getOrDefault("RABBITMQ_USER", "guest");
        String rabbitPass = env.getOrDefault("RABBITMQ_PASS", "guest");

        QueueConfig queueConfig = resolveQueueConfig(env);
        String taskQueue = queueConfig.taskQueue();
        boolean singlePartitionMode = queueConfig.singlePartitionMode();
        if (env.get("JOB_ID") == null || env.get("JOB_ID").trim().isEmpty()) {
            logger.warn("JOB_ID not found in environment. Falling back to default queue: {}", taskQueue);
        }

        String eventQueue = env.getOrDefault("EVENT_QUEUE", "job_events_queue");
        int idleTimeout = Integer.parseInt(env.getOrDefault("IDLE_TIMEOUT_MILLIS", "5000"));

        String minioEndpoint = env.getOrDefault("MINIO_ENDPOINT", "http://localhost:9000");
        String minioUser = env.getOrDefault("MINIO_ROOT_USER", "minioadmin");
        String minioPass = env.getOrDefault("MINIO_ROOT_PASSWORD", "minioadmin");

        String baseShuffleDir = env.getOrDefault("SHUFFLE_DIR", "/mnt/mapreduce-shuffle");
        String nodeIp = env.getOrDefault("NODE_IP", "127.0.0.1");

        AtomicBoolean shuttingDown = new AtomicBoolean(false);
        AtomicBoolean completedNormally = new AtomicBoolean(false);

        try {
            RabbitMqConnectionManager rabbitManager = new RabbitMqConnectionManager(rabbitHost, rabbitUser, rabbitPass);
            MinioConnectionManager minioManager = new MinioConnectionManager(minioEndpoint, minioUser, minioPass);
            S3ClientService s3ClientService = new S3ClientService(minioManager);

            RabbitMqProducer eventProducer = new RabbitMqProducer(rabbitManager, eventQueue);
            TaskExecutor taskExecutor = new TaskExecutor(s3ClientService, baseShuffleDir, eventProducer, nodeIp);
            RabbitMqConsumer consumer = new RabbitMqConsumer(
                    rabbitManager, taskQueue, idleTimeout, taskExecutor, eventProducer, singlePartitionMode);

            HttpServer healthServer = HttpServer.create(new InetSocketAddress(8080), 0);
            healthServer.createContext("/health", createHealthHandler(shuttingDown));
            healthServer.setExecutor(null);
            healthServer.start();
            logger.info("Health probe server active on port 8080 [/health]");

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (completedNormally.get()) {
                    return;
                }
                logger.warn(">>> Shutdown signal received. Flipping /health to 503 and initiating graceful drain... <<<");
                shuttingDown.set(true);
                try {
                    consumer.initiateGracefulDrain();
                } catch (Exception e) {
                    logger.error("Error during graceful shutdown: {}", e.getMessage(), e);
                } finally {
                    eventProducer.close();
                    taskExecutor.close();
                    healthServer.stop(0);
                }
                logger.info("Worker shutdown complete.");
            }, "worker-sigterm-drain"));

            logger.info("Entering event-driven task loop for queue: {} (singlePartitionMode={})",
                    taskQueue, singlePartitionMode);
            consumer.startConsuming();

            completedNormally.set(true);
            shuttingDown.set(true);
            logger.info("Task consumption finished. Closing worker resources.");
            taskExecutor.close();
            eventProducer.close();
            healthServer.stop(0);
            System.exit(0);

        } catch (Throwable e) {
            logger.error("Fatal initialization error. Worker process terminating.", e);
            System.exit(1);
        }
    }

    /**
     * Resolves the target RabbitMQ task queue and consumer mode from environment variables.
     * <p>
     * For {@code PHASE=reduce} with a non-blank {@code JOB_ID}, checks {@code PARTITION_ID} first
     * (targeted single-partition recovery), then {@code JOB_COMPLETION_INDEX} (Kubernetes
     * {@code Indexed} Job). When either index is present, binds to
     * {@code reduce_tasks_<jobId>_<idx>} with {@code singlePartitionMode=true}. Otherwise falls
     * back to {@code <phase>_tasks_<jobId>} or {@code RABBITMQ_QUEUE} when {@code JOB_ID} is unset.
     * </p>
     *
     * @param env Environment variable map.
     * @return Resolved {@link QueueConfig}.
     */
    static QueueConfig resolveQueueConfig(Map<String, String> env) {
        String jobId = env.get("JOB_ID");
        String phase = env.getOrDefault("PHASE", "map").trim().toLowerCase();

        if (jobId != null && !jobId.trim().isEmpty()) {
            String cleanJobId = jobId.trim();
            if ("reduce".equalsIgnoreCase(phase)) {
                String idx = firstNonBlank(env.get("PARTITION_ID"), env.get("JOB_COMPLETION_INDEX"));
                if (idx != null) {
                    return new QueueConfig("reduce_tasks_" + cleanJobId + "_" + idx, true);
                }
                return new QueueConfig("reduce_tasks_" + cleanJobId, false);
            }
            return new QueueConfig(phase + "_tasks_" + cleanJobId, false);
        }

        String fallbackQueue = env.getOrDefault("RABBITMQ_QUEUE", "map_tasks_queue");
        return new QueueConfig(fallbackQueue, false);
    }

    /**
     * Creates the {@code /health} HTTP handler returning {@code 200 OK} during normal operation
     * and {@code 503 Service Unavailable} once {@code shuttingDown} flips to {@code true}.
     *
     * @param shuttingDown Atomic drain flag toggled immediately upon {@code SIGTERM}.
     * @return Configured {@link HttpHandler}.
     */
    static HttpHandler createHealthHandler(AtomicBoolean shuttingDown) {
        return exchange -> {
            boolean draining = shuttingDown.get();
            int statusCode = draining ? 503 : 200;
            byte[] responseBytes = (draining ? "UNAVAILABLE" : "OK").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(statusCode, responseBytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(responseBytes);
            }
        };
    }

    private static String firstNonBlank(String primary, String secondary) {
        if (primary != null && !primary.trim().isEmpty()) {
            return primary.trim();
        }
        if (secondary != null && !secondary.trim().isEmpty()) {
            return secondary.trim();
        }
        return null;
    }
}
