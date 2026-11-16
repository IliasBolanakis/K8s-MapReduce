package com.iliasbolan.engine;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iliasbolan.core.TaskPayload;
import com.iliasbolan.engine.shuffle.ExternalMergeSorter;
import com.iliasbolan.grpc.shuffle.PartitionChunk;
import com.iliasbolan.grpc.shuffle.PartitionRequest;
import com.iliasbolan.grpc.shuffle.ShuffleServiceGrpc;
import com.iliasbolan.services.RabbitMqProducer;
import com.iliasbolan.services.S3ClientService;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * The central orchestration engine for executing Map-Reduce tasks on a Worker node.
 * <p>
 * Manages task localization, bounded parallel P2P gRPC shuffle fetching with channel reuse
 * and full-jitter exponential backoff, ephemeral child-JVM sandbox execution with 30-second
 * heartbeats, and graceful {@code SIGTERM} preemption/cleanup.
 * </p>
 * <p>
 * <b>Thread Safety &amp; Resource Lifecycle:</b><br>
 * Maintains a thread-safe {@link ConcurrentHashMap} of long-lived gRPC {@link ManagedChannel}
 * instances keyed by {@code host:port}. Callers must invoke {@link #close()} on worker shutdown
 * to cleanly terminate pooled channels. Active child {@link Process} and temporary staging
 * paths are tracked atomically so {@link #preemptActiveTask(long)} can escalate termination
 * safely from a shutdown hook thread.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 3.0
 * @since 2026-05-15
 */
public class TaskExecutor implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(TaskExecutor.class);

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static final Metadata.Key<String> AUTH_KEY =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    private static final long DEFAULT_GRPC_FETCH_DEADLINE_SECONDS = 300L;
    static final int DEFAULT_REDUCE_FETCH_CONCURRENCY = 8;
    static final int MIN_REDUCE_FETCH_CONCURRENCY = 4;
    static final int MAX_REDUCE_FETCH_CONCURRENCY = 8;
    static final int DEFAULT_MAX_FETCH_RETRIES = 6;
    static final long DEFAULT_RETRY_BASE_MS = 200L;
    static final long DEFAULT_RETRY_CAP_MS = 10_000L;
    static final long HEARTBEAT_INTERVAL_SECONDS = 30L;

    private final S3ClientService s3ClientService;
    private final String baseShuffleDir;
    private final RabbitMqProducer eventProducer;
    private final String nodeIp;
    private final int reduceFetchConcurrency;
    private final int maxFetchRetries;
    private final long retryBaseMs;
    private final long retryCapMs;

    /** Reusable gRPC channels keyed by normalized {@code host:port} endpoint. */
    private final ConcurrentHashMap<String, ManagedChannel> channelPool = new ConcurrentHashMap<>();

    /** Active child Sandbox JVM process, task payload, and fetch pool references for SIGTERM preemption. */
    private final AtomicReference<Process> activeProcess = new AtomicReference<>();
    private final AtomicReference<ExecutorService> activeFetchPool = new AtomicReference<>();
    private final AtomicReference<TaskPayload> activeTask = new AtomicReference<>();
    private final Set<Path> activeTempPaths = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean preempted = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Initializes the orchestration engine using production environment defaults.
     *
     * @param s3ClientService Service for downloading user-provided JARs and class files.
     * @param baseShuffleDir  Root local directory for persisting intermediate shuffle data.
     * @param eventProducer   Producer for signaling task status (COMPLETED/FAILED/IN_PROGRESS) to the Manager.
     * @param nodeIp          The physical IP of the host node, used for data locality signaling.
     */
    public TaskExecutor(S3ClientService s3ClientService, String baseShuffleDir, RabbitMqProducer eventProducer, String nodeIp) {
        this(
                s3ClientService,
                baseShuffleDir,
                eventProducer,
                nodeIp,
                resolveReduceFetchConcurrency(),
                DEFAULT_MAX_FETCH_RETRIES,
                DEFAULT_RETRY_BASE_MS,
                DEFAULT_RETRY_CAP_MS
        );
    }

    /**
     * Initializes the orchestration engine with explicit gRPC pool and retry parameters
     * (used for deterministic unit testing).
     *
     * @param s3ClientService        Service for downloading user-provided bytecode.
     * @param baseShuffleDir         Root local directory for ephemeral shuffle data.
     * @param eventProducer          Producer for transmitting lifecycle signals to the Manager.
     * @param nodeIp                 Physical IP of the host node.
     * @param reduceFetchConcurrency Bounded parallelism for Reduce gRPC fetches ({@code >= 1}).
     * @param maxFetchRetries        Maximum retry attempts for transient gRPC statuses ({@code >= 0}).
     * @param retryBaseMs            Base backoff delay in milliseconds ({@code >= 0}).
     * @param retryCapMs             Maximum backoff cap in milliseconds ({@code >= 0}).
     */
    TaskExecutor(S3ClientService s3ClientService,
                 String baseShuffleDir,
                 RabbitMqProducer eventProducer,
                 String nodeIp,
                 int reduceFetchConcurrency,
                 int maxFetchRetries,
                 long retryBaseMs,
                 long retryCapMs) {
        this.s3ClientService = s3ClientService;
        this.baseShuffleDir = baseShuffleDir;
        this.eventProducer = eventProducer;
        this.nodeIp = nodeIp;
        this.reduceFetchConcurrency = Math.max(1, reduceFetchConcurrency);
        this.maxFetchRetries = Math.max(0, maxFetchRetries);
        this.retryBaseMs = Math.max(0L, retryBaseMs);
        this.retryCapMs = Math.max(0L, retryCapMs);
    }

    /**
     * Parses the serialized task intent and routes execution to the appropriate phase handler.
     *
     * @param jsonPayload The raw JSON string containing the {@link TaskPayload} metadata.
     * @throws Throwable                If parsing fails or an unrecoverable error occurs during execution.
     * @throws IllegalArgumentException If the specified task type is neither MAP nor REDUCE.
     */
    public void executeTask(String jsonPayload) throws Throwable {
        preempted.set(false);
        TaskPayload payload = objectMapper.readValue(jsonPayload, TaskPayload.class);
        activeTask.set(payload);

        Map<String, String> previousMdc = MDC.getCopyOfContextMap();
        try {
            MDC.put("job_id", payload.jobId() != null ? payload.jobId() : "UNKNOWN");
            MDC.put("task_id", payload.taskId() != null ? payload.taskId() : "UNKNOWN");
            MDC.put("attempt_id", String.valueOf(payload.attemptId()));
            MDC.put("phase", payload.taskType() != null ? payload.taskType() : "UNKNOWN");

            logger.info("Successfully parsed task payload. JobId: {}, TaskType: {}, AttemptId: {}",
                    payload.jobId(), payload.taskType(), payload.attemptId());

            if ("MAP".equalsIgnoreCase(payload.taskType())) {
                executeMapPhase(payload, jsonPayload);
            } else if ("REDUCE".equalsIgnoreCase(payload.taskType())) {
                executeReducePhase(payload, jsonPayload);
            } else {
                throw new IllegalArgumentException("Unknown task type received: " + payload.taskType());
            }
        } finally {
            activeTask.compareAndSet(payload, null);
            if (previousMdc != null) {
                MDC.setContextMap(previousMdc);
            } else {
                MDC.clear();
            }
        }
    }

    /**
     * Orchestrates the complete lifecycle of a Map task.
     *
     * @param payload The deserialized task metadata.
     * @param rawJson The original JSON payload to be ingested by the Sandbox JVM.
     * @throws Throwable             If S3 retrieval, sandbox execution, or network signaling fails.
     * @throws IllegalStateException If the {@code NODE_IP} environment variable is unresolved.
     */
    private void executeMapPhase(TaskPayload payload, String rawJson) throws Throwable {
        String safeJobId = sanitizeId(payload.jobId());
        String safeTaskId = sanitizeId(payload.taskId());
        String safeClassName = sanitizeClassName(payload.className());

        logger.info("--- [ STARTING MAP PHASE: Task {} (attempt {}) ] ---", safeTaskId, payload.attemptId());

        String packagePath = safeClassName.replace(".", "/");
        String localCodeDir = Paths.get(baseShuffleDir, safeJobId, "usercode") + "/";
        String localCodePath = localCodeDir + packagePath + ".class";

        File fileObj = new File(localCodePath);
        fileObj.getParentFile().mkdirs();

        if (!fileObj.exists()) {
            s3ClientService.downloadUserCode(payload.userCodeBucket(), payload.userCodeObject(), localCodePath);
        } else {
            logger.debug("User code (Mapper) already localized. Skipping S3 fetch.");
        }

        if (payload.combinerClassName() != null && !payload.combinerClassName().trim().isEmpty()) {
            String safeCombinerClassName = sanitizeClassName(payload.combinerClassName());
            String combinerPackagePath = safeCombinerClassName.replace(".", "/");
            String combinerLocalCodePath = localCodeDir + combinerPackagePath + ".class";

            File combinerFileObj = new File(combinerLocalCodePath);
            combinerFileObj.getParentFile().mkdirs();

            if (!combinerFileObj.exists()) {
                logger.info("Localizing optional Combiner optimization class: {}", safeCombinerClassName);
                s3ClientService.downloadUserCode(payload.userCodeBucket(), payload.combinerCodeObject(), combinerLocalCodePath);
            } else {
                logger.debug("User code (Combiner) already localized. Skipping S3 fetch.");
            }
        }

        Path payloadFile = Paths.get(baseShuffleDir, safeJobId, "payloads", safeTaskId + ".json");
        Files.createDirectories(payloadFile.getParent());
        Files.writeString(payloadFile, rawJson);

        logger.info("Delegating Map computation to Ephemeral Sandbox JVM...");
        runSandbox(payloadFile, payload);

        if (this.nodeIp == null || this.nodeIp.trim().isEmpty()) {
            throw new IllegalStateException("CRITICAL FATAL: NODE_IP environment variable missing.");
        }

        String nodeName = System.getenv().getOrDefault("NODE_NAME", "unknown-node");
        String essBindAddress = nodeName + "@@" + this.nodeIp + ":7337";

        logger.info("Map finished. Signaling ESS Endpoint: {} (attempt {}).", essBindAddress, payload.attemptId());
        eventProducer.sendCompletionSignal(safeJobId, safeTaskId, payload.jobToken(), "COMPLETED", essBindAddress, payload.attemptId());

        logger.info("--- [ SUCCESSFULLY COMPLETED MAP PHASE: Task {} ] ---", safeTaskId);
    }

    /**
     * Orchestrates the complete lifecycle of a Reduce task using a bounded parallel gRPC
     * shuffle fetch pool, reusable {@link ManagedChannel}s, full-jitter exponential backoff,
     * and sibling cancellation on first unrecoverable failure.
     *
     * @param payload The deserialized task metadata containing upstream ESS endpoints.
     * @param rawJson The original JSON payload to be ingested by the Sandbox JVM.
     * @throws Throwable If P2P fetches fail with a non-gRPC error or sandbox execution faults.
     */
    private void executeReducePhase(TaskPayload payload, String rawJson) throws Throwable {
        String safeJobId = sanitizeId(payload.jobId());
        String safeTaskId = sanitizeId(payload.taskId());
        String safeClassName = sanitizeClassName(payload.className());

        logger.info("--- [ STARTING REDUCE PHASE: Partition {} for Job {} (attempt {}) ] ---",
                safeTaskId, safeJobId, payload.attemptId());

        String packagePath = safeClassName.replace(".", "/");
        String localCodeDir = Paths.get(baseShuffleDir, safeJobId, "usercode") + "/";
        String localCodePath = localCodeDir + packagePath + ".class";

        File fileObj = new File(localCodePath);
        fileObj.getParentFile().mkdirs();

        if (!fileObj.exists()) {
            s3ClientService.downloadUserCode(payload.userCodeBucket(), payload.userCodeObject(), localCodePath);
        } else {
            logger.debug("User code already localized. Skipping S3 fetch.");
        }

        int partitionIndex = Integer.parseInt(safeTaskId);
        Path rawDataDir = Paths.get(baseShuffleDir, safeJobId, "reduce_raw", String.valueOf(partitionIndex));
        Files.createDirectories(rawDataDir);

        try {
            List<String> rawEndpoints = payload.workerEndpoints() != null ? payload.workerEndpoints() : Collections.emptyList();
            Set<String> uniqueEndpoints = new HashSet<>(rawEndpoints);
            List<String> endpoints = new ArrayList<>(uniqueEndpoints);

            logger.info("Initiating bounded parallel gRPC P2P transfer from {} unique ESS nodes (concurrency={})...",
                    endpoints.size(), reduceFetchConcurrency);

            if (!endpoints.isEmpty()) {
                String failedEndpoint = fetchAllEndpointsInParallel(
                        endpoints, safeJobId, partitionIndex, payload.jobToken(), rawDataDir);
                if (failedEndpoint != null) {
                    if (preempted.get()) {
                        throw new InterruptedException("Reduce shuffle preempted during graceful drain");
                    }
                    String errorPayload = "SHUFFLE_FETCH_FAILED_FROM_NODE:" + failedEndpoint;
                    eventProducer.sendErrorSignal(
                            safeJobId, safeTaskId, payload.jobToken(), "FAILED", errorPayload, payload.attemptId());
                    return;
                }
            }

            Path payloadFile = Paths.get(baseShuffleDir, safeJobId, "payloads", safeTaskId + ".json");
            Files.createDirectories(payloadFile.getParent());
            Files.writeString(payloadFile, rawJson);

            logger.info("Streams persisted and validated atomically. Delegating aggregation to Sandbox...");
            runSandbox(payloadFile, payload);

            eventProducer.sendCompletionSignal(safeJobId, safeTaskId, payload.jobToken(), "COMPLETED", null, payload.attemptId());
            logger.info("--- [ SUCCESSFULLY COMPLETED REDUCE PHASE ] ---");

        } finally {
            ExternalMergeSorter.cleanupDirectory(rawDataDir);
        }
    }

    /**
     * Fetches intermediate partition streams concurrently across up to {@code reduceFetchConcurrency}
     * threads, immediately cancelling all sibling futures if any endpoint fails unrecoverably.
     *
     * @param endpoints      Deduplicated list of ESS endpoints.
     * @param safeJobId      Sanitized job UUID.
     * @param partitionIndex Target Reduce partition index.
     * @param jobToken       Cryptographic job token sent in gRPC metadata.
     * @param rawDataDir     Local staging directory for downloaded {@code .tmp}/{@code .lz4} streams.
     * @return {@code null} if all endpoints succeeded; otherwise the first unrecoverable endpoint identifier.
     * @throws Throwable If a non-gRPC exception or thread interruption occurs.
     */
    private String fetchAllEndpointsInParallel(List<String> endpoints,
                                               String safeJobId,
                                               int partitionIndex,
                                               String jobToken,
                                               Path rawDataDir) throws Throwable {
        int poolSize = Math.min(reduceFetchConcurrency, endpoints.size());
        ExecutorService fetchPool = Executors.newFixedThreadPool(poolSize);
        activeFetchPool.set(fetchPool);

        ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<>(fetchPool);
        List<Future<Void>> futures = new ArrayList<>(endpoints.size());
        AtomicReference<String> firstFailedEndpoint = new AtomicReference<>();
        Map<String, String> callerMdc = MDC.getCopyOfContextMap();

        try {
            for (int i = 0; i < endpoints.size(); i++) {
                final int index = i;
                final String endpoint = endpoints.get(i);
                futures.add(completionService.submit(() -> {
                    Map<String, String> previousThreadMdc = MDC.getCopyOfContextMap();
                    try {
                        if (callerMdc != null) {
                            MDC.setContextMap(callerMdc);
                        }
                        fetchEndpointWithRetry(endpoint, index, safeJobId, partitionIndex, jobToken, rawDataDir);
                        return null;
                    } finally {
                        if (previousThreadMdc != null) {
                            MDC.setContextMap(previousThreadMdc);
                        } else {
                            MDC.clear();
                        }
                    }
                }));
            }

            for (int completed = 0; completed < endpoints.size(); completed++) {
                Future<Void> finishedFuture = completionService.take();
                try {
                    finishedFuture.get();
                } catch (ExecutionException execEx) {
                    Throwable cause = execEx.getCause();
                    cancelSiblingFutures(futures);
                    fetchPool.shutdownNow();
                    cleanupDirectoryTmpFiles(rawDataDir);

                    if (cause instanceof UnrecoverableFetchException ufe) {
                        firstFailedEndpoint.compareAndSet(null, ufe.endpoint);
                        return firstFailedEndpoint.get();
                    }
                    if (cause instanceof InterruptedException ie) {
                        throw ie;
                    }
                    throw new RuntimeException(cause);
                }
            }
            return null;
        } catch (InterruptedException ie) {
            cancelSiblingFutures(futures);
            fetchPool.shutdownNow();
            cleanupDirectoryTmpFiles(rawDataDir);
            Thread.currentThread().interrupt();
            throw ie;
        } finally {
            cancelSiblingFutures(futures);
            fetchPool.shutdownNow();
            activeFetchPool.compareAndSet(fetchPool, null);
            cleanupDirectoryTmpFiles(rawDataDir);
        }
    }

    /**
     * Fetches a single endpoint's partition stream with full-jitter exponential backoff on
     * {@link Status.Code#RESOURCE_EXHAUSTED} and {@link Status.Code#UNAVAILABLE}.
     *
     * @param endpoint       ESS endpoint string ({@code host:port} or {@code node@@host:port}).
     * @param streamIndex    Unique stream index for naming {@code grpc_stream_<index>.lz4}.
     * @param safeJobId      Sanitized job identifier.
     * @param partitionIndex Target partition index.
     * @param jobToken       Cryptographic job authorization token.
     * @param rawDataDir     Local staging directory.
     * @throws UnrecoverableFetchException If a non-retryable gRPC status occurs or retries exhaust.
     * @throws IOException                 If local staging or atomic promotion fails.
     * @throws InterruptedException        If cancelled or interrupted during backoff sleep.
     */
    private void fetchEndpointWithRetry(String endpoint,
                                        int streamIndex,
                                        String safeJobId,
                                        int partitionIndex,
                                        String jobToken,
                                        Path rawDataDir)
            throws UnrecoverableFetchException, IOException, InterruptedException {
        String cleanEndpoint = endpoint.contains("@@")
                ? endpoint.substring(endpoint.indexOf("@@") + 2)
                : endpoint;
        String[] addressParts = cleanEndpoint.split(":");
        String host = addressParts[0];
        int port = (addressParts.length > 1) ? Integer.parseInt(addressParts[1]) : 7337;
        String channelKey = host + ":" + port;

        Path tmpPath = rawDataDir.resolve("grpc_stream_" + streamIndex + ".tmp");
        Path finalPath = rawDataDir.resolve("grpc_stream_" + streamIndex + ".lz4");
        activeTempPaths.add(tmpPath);

        try {
            for (int attempt = 0; attempt <= maxFetchRetries; attempt++) {
                if (Thread.currentThread().isInterrupted() || preempted.get()) {
                    throw new InterruptedException("Shuffle fetch cancelled for endpoint " + endpoint);
                }

                try {
                    ManagedChannel channel = getOrCreateChannel(channelKey, host, port);

                    try (BufferedOutputStream bos = new BufferedOutputStream(
                            new FileOutputStream(tmpPath.toFile()), 1024 * 1024)) {
                        Metadata header = new Metadata();
                        header.put(AUTH_KEY, jobToken);

                        ClientInterceptor authInterceptor = MetadataUtils.newAttachHeadersInterceptor(header);
                        long grpcDeadlineSeconds = resolveGrpcFetchDeadlineSeconds();
                        ShuffleServiceGrpc.ShuffleServiceBlockingStub stub = createBlockingStub(channel)
                                .withDeadlineAfter(grpcDeadlineSeconds, TimeUnit.SECONDS)
                                .withInterceptors(authInterceptor);

                        PartitionRequest request = PartitionRequest.newBuilder()
                                .setJobId(safeJobId)
                                .setPartitionId(partitionIndex)
                                .build();

                        Iterator<PartitionChunk> chunkStream = stub.getPartition(request);
                        while (chunkStream.hasNext()) {
                            if (Thread.currentThread().isInterrupted() || preempted.get()) {
                                throw new InterruptedException("Shuffle stream interrupted for endpoint " + endpoint);
                            }
                            chunkStream.next().getContent().writeTo(bos);
                        }
                    }

                    if (Files.exists(tmpPath)) {
                        Files.move(tmpPath, finalPath,
                                StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                    }
                    return;

                } catch (StatusRuntimeException grpcEx) {
                    Files.deleteIfExists(tmpPath);
                    Status.Code code = grpcEx.getStatus().getCode();

                    if (isTransientGrpcStatus(code) && attempt < maxFetchRetries && !preempted.get()) {
                        long sleepMs = computeFullJitterBackoffMs(attempt, retryBaseMs, retryCapMs);
                        logger.warn("Transient gRPC shuffle error from {} (code={}); deleted partial tmp and backing off {}ms (retry {}/{}).",
                                endpoint, code, sleepMs, attempt + 1, maxFetchRetries);
                        if (sleepMs > 0) {
                            Thread.sleep(sleepMs);
                        }
                        continue;
                    }

                    logger.warn("P2P Data Loss Detected! Unrecoverable shuffle fetch failure from {} (code={}, attempt={}).",
                            endpoint, code, attempt);
                    throw new UnrecoverableFetchException(endpoint, grpcEx);

                } catch (InterruptedException ie) {
                    Files.deleteIfExists(tmpPath);
                    throw ie;
                } catch (IOException ioEx) {
                    Files.deleteIfExists(tmpPath);
                    logger.error("Critical local I/O failure during shuffle fetch from node {}", endpoint, ioEx);
                    throw ioEx;
                }
            }
        } finally {
            try {
                Files.deleteIfExists(tmpPath);
            } catch (IOException ignored) {
            }
            activeTempPaths.remove(tmpPath);
        }
    }

    /**
     * Returns an open reusable {@link ManagedChannel} for {@code channelKey}, rebuilding it
     * atomically if absent, shut down, or terminated.
     *
     * @param channelKey Normalized {@code host:port} cache key.
     * @param host       ESS hostname or IP.
     * @param port       ESS gRPC port.
     * @return An active {@link ManagedChannel}.
     */
    ManagedChannel getOrCreateChannel(String channelKey, String host, int port) {
        return channelPool.compute(channelKey, (key, existing) -> {
            if (existing != null && !existing.isShutdown() && !existing.isTerminated()) {
                return existing;
            }
            return createManagedChannel(host, port);
        });
    }

    ManagedChannel createManagedChannel(String host, int port) {
        return ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .keepAliveTime(30, TimeUnit.SECONDS)
                .build();
    }

    ShuffleServiceGrpc.ShuffleServiceBlockingStub createBlockingStub(ManagedChannel channel) {
        return ShuffleServiceGrpc.newBlockingStub(channel);
    }

    /**
     * Returns {@code true} only for transient backpressure/connectivity gRPC statuses
     * eligible for retry ({@code RESOURCE_EXHAUSTED} and {@code UNAVAILABLE}).
     *
     * @param code The gRPC {@link Status.Code} observed on failure.
     * @return {@code true} if retryable; {@code false} for fail-loud terminal statuses.
     */
    static boolean isTransientGrpcStatus(Status.Code code) {
        return code == Status.Code.RESOURCE_EXHAUSTED || code == Status.Code.UNAVAILABLE;
    }

    /**
     * Computes full-jitter exponential backoff in milliseconds:
     * {@code random(0, min(capMs, baseMs * 2^retryAttempt) + 1)}.
     *
     * @param retryAttempt Zero-based retry attempt index.
     * @param baseMs       Base backoff duration in milliseconds.
     * @param capMs        Maximum backoff ceiling in milliseconds.
     * @return Random jittered sleep duration in {@code [0, min(capMs, baseMs * 2^retryAttempt)]}.
     */
    static long computeFullJitterBackoffMs(int retryAttempt, long baseMs, long capMs) {
        int shift = Math.min(Math.max(retryAttempt, 0), 30);
        long ceiling = Math.min(capMs, baseMs * (1L << shift));
        if (ceiling <= 0L) {
            return 0L;
        }
        return ThreadLocalRandom.current().nextLong(0L, ceiling + 1L);
    }

    private static void cancelSiblingFutures(List<Future<Void>> futures) {
        for (Future<Void> future : futures) {
            if (!future.isDone()) {
                future.cancel(true);
            }
        }
    }

    /**
     * Spawns an isolated, ephemeral Child JVM to execute user-defined computation logic,
     * emitting {@code IN_PROGRESS} heartbeats every 30 seconds.
     *
     * @param payloadFile The physical path to the staged JSON payload on disk.
     * @param payload     The deserialized metadata used for periodic heartbeat signaling.
     * @throws IOException          If the child process cannot be spawned due to OS resource limits.
     * @throws InterruptedException If the executor thread is interrupted or preempted while waiting.
     * @throws RuntimeException     If the Sandbox JVM terminates with a non-zero exit code without preemption.
     */
    private void runSandbox(Path payloadFile, TaskPayload payload) throws IOException, InterruptedException {
        String javaHome = System.getProperty("java.home");
        String javaBin = Paths.get(javaHome, "bin", "java").toString();
        String classpath = System.getProperty("java.class.path");

        int vCpus = Runtime.getRuntime().availableProcessors();
        double factor = Double.parseDouble(System.getenv().getOrDefault("PARALLELISM_FACTOR", "2.0"));
        int targetThreads = (int) Math.ceil(vCpus * factor);

        ProcessBuilder pb = new ProcessBuilder(
                javaBin,
                "-XX:+UseContainerSupport",
                "-XX:MaxRAMPercentage=50.0",
                "-XX:MaxMetaspaceSize=64m",
                "-XX:+TieredCompilation",
                "-XX:CICompilerCount=2",
                "-Djava.util.concurrent.ForkJoinPool.common.parallelism=" + targetThreads,
                "-cp", classpath,
                "com.iliasbolan.engine.execution.SandboxRunner",
                payloadFile.toAbsolutePath().toString(),
                this.baseShuffleDir
        );

        pb.inheritIO();

        ScheduledExecutorService heartbeatExecutor = createHeartbeatScheduler();
        Process process = null;

        try {
            heartbeatExecutor.scheduleAtFixedRate(
                    () -> eventProducer.sendProgressSignal(
                            payload.jobId(), payload.taskId(), payload.jobToken(), payload.attemptId()),
                    HEARTBEAT_INTERVAL_SECONDS,
                    HEARTBEAT_INTERVAL_SECONDS,
                    TimeUnit.SECONDS
            );

            process = startSandboxProcess(pb);
            activeProcess.set(process);
            int exitCode = process.waitFor();

            if (preempted.get()) {
                throw new InterruptedException("Sandbox JVM preempted during SIGTERM drain (exitCode=" + exitCode + ")");
            }
            if (exitCode != 0) {
                throw new RuntimeException("Sandbox JVM terminated with exit code: " + exitCode);
            }
        } finally {
            if (process != null) {
                activeProcess.compareAndSet(process, null);
            }
            heartbeatExecutor.shutdownNow();
            Files.deleteIfExists(payloadFile);
        }
    }

    ScheduledExecutorService createHeartbeatScheduler() {
        return Executors.newSingleThreadScheduledExecutor();
    }

    Process startSandboxProcess(ProcessBuilder pb) throws IOException {
        return pb.start();
    }

    /**
     * Preempts any active task execution during a {@code SIGTERM} graceful drain:
     * cancels in-flight gRPC shuffle pools, sends {@code destroy()} ({@code SIGTERM}) to the
     * active child Sandbox JVM, waits up to {@code destroyGraceMillis}, escalates to
     * {@code destroyForcibly()} ({@code SIGKILL}) if still alive, and cleans all {@code .tmp} files.
     *
     * @param destroyGraceMillis Milliseconds to wait between {@code destroy()} and {@code destroyForcibly()}.
     */
    public void preemptActiveTask(long destroyGraceMillis) {
        preempted.set(true);

        ExecutorService pool = activeFetchPool.get();
        if (pool != null) {
            logger.warn("Preempting active gRPC shuffle fetch pool during SIGTERM drain.");
            pool.shutdownNow();
        }

        Process proc = activeProcess.get();
        if (proc != null && proc.isAlive()) {
            logger.warn("Grace window elapsed; sending SIGTERM (destroy) to active Sandbox process.");
            proc.destroy();
            try {
                boolean exited = proc.waitFor(Math.max(0L, destroyGraceMillis), TimeUnit.MILLISECONDS);
                if (!exited && proc.isAlive()) {
                    logger.error("Sandbox process did not exit within {}ms; escalating to SIGKILL (destroyForcibly).",
                            destroyGraceMillis);
                    proc.destroyForcibly();
                    proc.waitFor(1_000L, TimeUnit.MILLISECONDS);
                }
            } catch (InterruptedException ie) {
                if (proc.isAlive()) {
                    proc.destroyForcibly();
                }
                Thread.currentThread().interrupt();
            }
        }

        cleanupTempFiles();
    }

    /**
     * Indicates whether the currently running task was preempted by {@link #preemptActiveTask(long)}.
     *
     * @return {@code true} if preemption has been triggered for the active task.
     */
    public boolean isPreempted() {
        return preempted.get();
    }

    /**
     * Deletes tracked temporary paths owned by this executor and, when an active task is
     * registered, cleans ONLY the {@code .tmp} files belonging to that specific task/attempt
     * without scanning sibling pods' or other tenants' directories under {@code baseShuffleDir}.
     */
    public void cleanupTempFiles() {
        for (Path tmp : activeTempPaths) {
            try {
                Files.deleteIfExists(tmp);
            } catch (IOException e) {
                logger.warn("Failed to remove tracked temporary file {}", tmp, e);
            }
        }
        activeTempPaths.clear();

        TaskPayload current = activeTask.get();
        if (current == null || baseShuffleDir == null || baseShuffleDir.isBlank()) {
            return;
        }

        try {
            String safeJobId = sanitizeId(current.jobId());
            String safeTaskId = sanitizeId(current.taskId());

            if ("MAP".equalsIgnoreCase(current.taskType())) {
                cleanupScopedMapAttemptTmpFiles(Paths.get(baseShuffleDir, safeJobId), safeTaskId, current.attemptId());
            } else if ("REDUCE".equalsIgnoreCase(current.taskType())) {
                Path reduceRawTaskDir = Paths.get(baseShuffleDir, safeJobId, "reduce_raw", safeTaskId);
                cleanupDirectoryTmpFiles(reduceRawTaskDir);
            }
        } catch (SecurityException e) {
            logger.warn("Skipping scoped .tmp cleanup due to invalid task identifiers: {}", e.getMessage());
        }
    }

    private void cleanupScopedMapAttemptTmpFiles(Path jobDir, String safeTaskId, int attemptId) {
        if (jobDir == null || !Files.exists(jobDir)) {
            return;
        }
        String exactAttemptTmp = safeTaskId + ".attempt." + attemptId + ".tmp";
        String attemptSubPrefix = safeTaskId + ".attempt." + attemptId + ".";
        try (Stream<Path> walk = Files.walk(jobDir, 2)) {
            walk.filter(Files::isRegularFile)
                    .filter(p -> {
                        String name = p.getFileName().toString();
                        return name.equals(exactAttemptTmp)
                                || (name.startsWith(attemptSubPrefix) && name.endsWith(".tmp"));
                    })
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException e) {
                            logger.warn("Failed to remove scoped Map temporary file {}", p, e);
                        }
                    });
        } catch (IOException e) {
            logger.warn("Failed walking job directory {} during scoped Map .tmp cleanup", jobDir, e);
        }
    }

    private void cleanupDirectoryTmpFiles(Path rootDir) {
        if (rootDir == null || !Files.exists(rootDir)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(rootDir)) {
            walk.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(".tmp"))
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException e) {
                            logger.warn("Failed to remove temporary file {}", p, e);
                        }
                    });
        } catch (IOException e) {
            logger.warn("Failed walking directory {} during .tmp cleanup", rootDir, e);
        }
    }

    /**
     * Shuts down all cached gRPC {@link ManagedChannel} instances cleanly.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        for (Map.Entry<String, ManagedChannel> entry : channelPool.entrySet()) {
            ManagedChannel channel = entry.getValue();
            if (channel != null) {
                try {
                    channel.shutdown();
                    if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                        channel.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    channel.shutdownNow();
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    logger.warn("Error shutting down gRPC channel for {}", entry.getKey(), e);
                }
            }
        }
        channelPool.clear();
    }

    private String sanitizeId(String input) {
        if (input == null || !input.matches("^[a-zA-Z0-9\\-]+$")) {
            throw new SecurityException("Invalid ID format. Potential path traversal detected: " + input);
        }
        return input;
    }

    private String sanitizeClassName(String input) {
        if (input == null || !input.matches("^[a-zA-Z0-9_.]+$")) {
            throw new SecurityException("Invalid class name format. Potential path traversal detected: " + input);
        }
        return input;
    }

    static long resolveGrpcFetchDeadlineSeconds() {
        return parseGrpcFetchDeadlineSeconds(System.getenv("GRPC_FETCH_DEADLINE_SECONDS"));
    }

    static long parseGrpcFetchDeadlineSeconds(String rawValue) {
        if (rawValue == null || rawValue.trim().isEmpty()) {
            return DEFAULT_GRPC_FETCH_DEADLINE_SECONDS;
        }
        try {
            long parsed = Long.parseLong(rawValue.trim());
            return parsed > 0 ? parsed : DEFAULT_GRPC_FETCH_DEADLINE_SECONDS;
        } catch (NumberFormatException e) {
            logger.warn("Invalid GRPC_FETCH_DEADLINE_SECONDS '{}', defaulting to {}s", rawValue, DEFAULT_GRPC_FETCH_DEADLINE_SECONDS);
            return DEFAULT_GRPC_FETCH_DEADLINE_SECONDS;
        }
    }

    static int resolveReduceFetchConcurrency() {
        return parseReduceFetchConcurrency(System.getenv("REDUCE_FETCH_CONCURRENCY"));
    }

    /**
     * Parses and clamps {@code REDUCE_FETCH_CONCURRENCY} into {@code [4, 8]} (default {@code 8}).
     *
     * @param rawValue Raw environment variable string.
     * @return Clamped fetch pool concurrency in {@code [4, 8]}.
     */
    static int parseReduceFetchConcurrency(String rawValue) {
        if (rawValue == null || rawValue.trim().isEmpty()) {
            return DEFAULT_REDUCE_FETCH_CONCURRENCY;
        }
        try {
            int parsed = Integer.parseInt(rawValue.trim());
            if (parsed <= 0) {
                return DEFAULT_REDUCE_FETCH_CONCURRENCY;
            }
            return Math.max(MIN_REDUCE_FETCH_CONCURRENCY, Math.min(MAX_REDUCE_FETCH_CONCURRENCY, parsed));
        } catch (NumberFormatException e) {
            logger.warn("Invalid REDUCE_FETCH_CONCURRENCY '{}', defaulting to {}", rawValue, DEFAULT_REDUCE_FETCH_CONCURRENCY);
            return DEFAULT_REDUCE_FETCH_CONCURRENCY;
        }
    }

    private static final class UnrecoverableFetchException extends Exception {
        private final String endpoint;

        private UnrecoverableFetchException(String endpoint, Throwable cause) {
            super(cause);
            this.endpoint = endpoint;
        }
    }
}
