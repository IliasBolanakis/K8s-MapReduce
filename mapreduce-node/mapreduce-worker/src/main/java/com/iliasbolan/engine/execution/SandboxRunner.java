package com.iliasbolan.engine.execution;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iliasbolan.core.Combiner;
import com.iliasbolan.core.Mapper;
import com.iliasbolan.core.Reducer;
import com.iliasbolan.core.TaskPayload;
import com.iliasbolan.engine.shuffle.ExternalMergeSorter;
import com.iliasbolan.engine.shuffle.ShufflePartitioner;
import com.iliasbolan.infrastructure.MinioConnectionManager;
import com.iliasbolan.services.S3ClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;

/**
 * The isolated entry point for executing user-defined Map-Reduce logic.
 * <p>
 * Runs as a standalone Child JVM spawned by {@link com.iliasbolan.engine.TaskExecutor} so
 * third-party bytecode, ClassLoaders, and Metaspace allocations are completely reclaimed
 * by the OS when the task exits.
 * </p>
 * <p>
 * <b>Attempt Fencing &amp; Staging:</b><br>
 * Map tasks propagate {@link TaskPayload#attemptId()} into {@link ShufflePartitioner} for
 * atomic {@code <mapTaskId>.attempt} fencing. Reduce tasks upload outputs to
 * {@code <objectName>_attempts/part_<p>_attempt_<a>.staging} when {@code attemptId > 0}
 * (for Control Plane promotion after fencing) or directly to
 * {@code <objectName>result_part_<p>.txt} when {@code attemptId == 0}.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 3.4
 * @since 2026-04-24
 */
public class SandboxRunner {

    /** The SLF4J logger instance for recording sandbox lifecycle events and fatal JVM errors. */
    private static final Logger logger = LoggerFactory.getLogger(SandboxRunner.class);

    /** High-performance JSON serializer configured to ignore unknown properties for resilient deserialization. */
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    /**
     * The primary execution hook for the isolated Child JVM.
     *
     * @param args Command line arguments injected by the parent {@code TaskExecutor}:
     *             <ul>
     *             <li>{@code args[0]}: Absolute path to the serialized JSON {@link TaskPayload} file.</li>
     *             <li>{@code args[1]}: The root physical directory for ephemeral shuffle data bounding.</li>
     *             </ul>
     */
    public static void main(String[] args) {
        if (args.length < 2) {
            logger.error("CRITICAL: SandboxRunner requires exactly 2 arguments: <PayloadFilePath> <BaseShuffleDir>");
            System.exit(1);
        }

        try {
            Path payloadFile = Paths.get(args[0]);
            String baseShuffleDir = args[1];

            String jsonPayload = Files.readString(payloadFile);
            TaskPayload payload = objectMapper.readValue(jsonPayload, TaskPayload.class);

            MDC.put("job_id", payload.jobId() != null ? payload.jobId() : "UNKNOWN");
            MDC.put("task_id", payload.taskId() != null ? payload.taskId() : "UNKNOWN");
            MDC.put("attempt_id", String.valueOf(payload.attemptId()));
            MDC.put("phase", payload.taskType() != null ? payload.taskType() : "UNKNOWN");

            logger.info("Initializing Ephemeral Sandbox JVM. Payload: {}", payloadFile);

            MinioConnectionManager manager = new MinioConnectionManager(
                    System.getenv().getOrDefault("MINIO_ENDPOINT", "http://localhost:9000"),
                    resolveEnvFallback(System.getenv(), "MINIO_ACCESS_KEY", "MINIO_ROOT_USER", "minioadmin"),
                    resolveEnvFallback(System.getenv(), "MINIO_SECRET_KEY", "MINIO_ROOT_PASSWORD", "minioadmin")
            );
            S3ClientService s3ClientService = new S3ClientService(manager);

            String localCodeDir = Paths.get(baseShuffleDir, payload.jobId(), "usercode") + "/";

            if ("MAP".equalsIgnoreCase(payload.taskType())) {
                executeMap(payload, localCodeDir, baseShuffleDir, s3ClientService);
            } else if ("REDUCE".equalsIgnoreCase(payload.taskType())) {
                executeReduce(payload, localCodeDir, baseShuffleDir, s3ClientService);
            } else {
                throw new IllegalArgumentException("Unsupported task type in Sandbox: " + payload.taskType());
            }

            logger.info("Sandbox execution completed successfully. Terminating JVM.");
            System.exit(0);

        } catch (Throwable t) {
            logger.error("FATAL: Sandbox JVM encountered an unrecoverable exception.", t);
            System.exit(1);
        } finally {
            MDC.clear();
        }
    }

    /**
     * Resolves a configuration value by checking a primary environment key, falling back
     * to a secondary key, and returning a default value when both are unset or empty.
     *
     * @param env          Environment variable map.
     * @param primaryKey   Preferred environment variable key.
     * @param fallbackKey  Secondary environment variable key.
     * @param defaultValue Default value returned when both keys are absent or empty.
     * @return The resolved non-empty configuration value or {@code defaultValue}.
     */
    static String resolveEnvFallback(Map<String, String> env, String primaryKey, String fallbackKey, String defaultValue) {
        String primary = env.get(primaryKey);
        if (primary != null && !primary.isEmpty()) {
            return primary;
        }
        String fallback = env.get(fallbackKey);
        if (fallback != null && !fallback.isEmpty()) {
            return fallback;
        }
        return defaultValue;
    }

    /**
     * Resolves the S3 output object key for a Reduce partition, routing {@code attemptId > 0}
     * runs to an isolated staging prefix so the Control Plane can fence stale attempts and
     * promote only the winning attempt's output.
     *
     * @param payload        The Reduce {@link TaskPayload} containing {@code objectName} and {@code attemptId}.
     * @param partitionIndex Zero-based Reduce partition index.
     * @return The target S3 object key under the results bucket.
     */
    static String resolveReduceOutputObjectKey(TaskPayload payload, int partitionIndex) {
        String prefix = payload.objectName() != null ? payload.objectName() : "";
        if (payload.attemptId() > 0) {
            return String.format("%s_attempts/part_%d_attempt_%d.staging",
                    prefix, partitionIndex, payload.attemptId());
        }
        return String.format("%sresult_part_%d.txt", prefix, partitionIndex);
    }

    /**
     * Orchestrates the execution of a Map task within the isolated sandbox boundary.
     *
     * @param payload        The deserialized task instructions containing S3 targets and job metadata.
     * @param localCodeDir   The local directory containing the dynamically loaded user bytecode.
     * @param baseShuffleDir The root local directory for persisting intermediate partitioned data.
     * @param s3             The configured service client for interacting with S3-compatible storage.
     * @throws Throwable If class loading, data retrieval, or parallel execution encounters a fatal error.
     */
    private static void executeMap(TaskPayload payload, String localCodeDir, String baseShuffleDir, S3ClientService s3) throws Throwable {
        Mapper mapper = DynamicClassLoader.loadMapper(localCodeDir, payload.className());

        Combiner combiner = null;
        if (payload.combinerClassName() != null && !payload.combinerClassName().trim().isEmpty()) {
            combiner = DynamicClassLoader.loadCombiner(localCodeDir, payload.combinerClassName());
            logger.info("Local reduction optimization active. Loaded Combiner: {}", payload.combinerClassName());
        }

        try (ShufflePartitioner partitioner = new ShufflePartitioner(
                baseShuffleDir, payload.jobId(), payload.taskId(), payload.numReducers(), payload.attemptId())) {

            long remainingBytes = payload.byteLength();
            long currentLogicalOffset = payload.byteOffset();
            final long subChunkMaxSize = 32L * 1024 * 1024;

            logger.info("Initiating high-performance Map execution. Total bytes to process: {}", remainingBytes);

            while (remainingBytes > 0) {
                long fetchSize = Math.min(remainingBytes, subChunkMaxSize);
                logger.debug("Fetching sub-chunk at offset {}, size {} bytes", currentLogicalOffset, fetchSize);

                List<String> records = s3.readDataChunk(
                        payload.bucketName(), payload.objectName(), currentLogicalOffset, fetchSize);

                if (!records.isEmpty()) {
                    MapTaskProcessor rootMapTask = new MapTaskProcessor(records, 0, records.size(), mapper, combiner, partitioner);
                    ForkJoinPool.commonPool().invoke(rootMapTask);
                }

                currentLogicalOffset += fetchSize;
                remainingBytes -= fetchSize;
            }
        }

        logger.info("All sub-chunks processed and compressed for task {}.", payload.taskId());
    }

    /**
     * Orchestrates the execution of a Reduce task within the isolated sandbox boundary.
     *
     * @param payload        The deserialized task instructions and metadata containing multi-tenant S3 targets.
     * @param localCodeDir   The local directory containing the dynamically loaded user bytecode.
     * @param baseShuffleDir The root local directory used to resolve the job's ephemeral boundary.
     * @param s3             The configured service client for persisting final output to shared storage.
     * @throws Throwable If class loading, K-Way merge-sort operations, or data persistence fails.
     */
    private static void executeReduce(TaskPayload payload, String localCodeDir, String baseShuffleDir, S3ClientService s3) throws Throwable {
        Reducer reducer = DynamicClassLoader.loadReducer(localCodeDir, payload.className());
        int partitionIndex = Integer.parseInt(payload.taskId());

        Path rawDataDir = Paths.get(baseShuffleDir, payload.jobId(), "reduce_raw", String.valueOf(partitionIndex));

        Path finalReducedFile = ExternalMergeSorter.sortReduceAndSpill(rawDataDir, reducer);

        String finalPath = resolveReduceOutputObjectKey(payload, partitionIndex);
        logger.info("Uploading reduced output for partition {} (attempt {}) to s3://{}/{}",
                partitionIndex, payload.attemptId(), payload.bucketName(), finalPath);

        s3.uploadFileFromDisk(payload.bucketName(), finalPath, finalReducedFile);
    }
}
