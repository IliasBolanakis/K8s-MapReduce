package com.iliasbolan.ess.grpc;

import com.google.protobuf.ByteString;
import com.iliasbolan.ess.ShuffleDaemon;
import com.iliasbolan.ess.gc.StorageJanitor;
import com.iliasbolan.ess.storage.MinioStorageService;
import com.iliasbolan.grpc.shuffle.PartitionChunk;
import com.iliasbolan.grpc.shuffle.PartitionRequest;
import com.iliasbolan.grpc.shuffle.ShuffleServiceGrpc.ShuffleServiceImplBase;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * High-performance, multi-tier data plane implementation for the External Shuffle Service (ESS).
 * <p>
 * This service securely streams intermediate MapReduce partition data to downstream Reducers
 * via Peer-to-Peer (P2P) gRPC connections. It enforces bounded stream admission and utilizes a
 * pooled NIO Direct {@link ByteBuffer} per active stream to maintain a strict O(1) off-heap
 * memory footprint during multi-gigabyte transfers.
 * </p>
 * <p>
 * <b>Architecture Invariants:</b>
 * <ul>
 *   <li><b>ADR-02 (Bounded Local Stream Admission):</b> Concurrent {@code GetPartition} streams
 *   are gated by a fair {@link Semaphore} sized via {@code ESS_MAX_LOCAL_STREAMS} (default
 *   {@value #DEFAULT_MAX_LOCAL_STREAMS}) with timed {@code tryAcquire} configured by
 *   {@code ESS_STREAM_ACQUIRE_TIMEOUT_MS} (default {@value #DEFAULT_STREAM_ACQUIRE_TIMEOUT_MS} ms).
 *   Saturation fails fast with {@link Status#RESOURCE_EXHAUSTED} for downstream jittered retry.</li>
 *   <li><b>ADR-10 (Pooled Direct ByteBuffer per Stream):</b> Each active stream leases one
 *   65KB Direct {@link ByteBuffer} from a bounded {@link ArrayBlockingQueue} pool for all
 *   {@code .lz4} segments in the partition and deterministically returns it in {@code finally}.</li>
 *   <li><b>ADR-14 (Graceful Drain):</b> Implements {@link AutoCloseable} /
 *   {@link #shutdownAndAwait(long, TimeUnit)} to drain active virtual-thread streams cleanly
 *   on {@code SIGTERM}.</li>
 *   <li><b>ADR-15 (Fail-Loud Degraded S3 State):</b> When local partition state is missing and
 *   {@code minioStorageService} is {@code null}, fails loud with
 *   {@link Status#FAILED_PRECONDITION} instead of masking potential data loss as empty.</li>
 * </ul>
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 5.0
 * @since 2026-05-15
 */
public class ShuffleServiceImpl extends ShuffleServiceImplBase implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ShuffleServiceImpl.class);

    /** Chunk size (in bytes) for NIO Direct Buffer streaming over gRPC. */
    public static final int CHUNK_SIZE_BYTES = 65000;

    /** Default maximum number of concurrent local {@code GetPartition} streams. */
    public static final int DEFAULT_MAX_LOCAL_STREAMS = 64;

    /** Default admission wait timeout (in milliseconds) before returning {@code RESOURCE_EXHAUSTED}. */
    public static final long DEFAULT_STREAM_ACQUIRE_TIMEOUT_MS = 2000L;

    /** Default shutdown drain timeout (in seconds) for {@link #close()}. */
    private static final long DEFAULT_DRAIN_TIMEOUT_SECONDS = 20L;

    /** Security boundary: Ensures job identifiers cannot contain path traversal payloads (e.g., "../"). */
    private static final Pattern ID_PATTERN = Pattern.compile("^[a-zA-Z0-9\\-]+$");

    private final String baseShuffleDir;
    private final MinioStorageService minioStorageService;
    private final int maxLocalStreams;
    private final long acquireTimeoutMs;

    /** Ephemeral concurrency pool utilizing lightweight Virtual Threads for massive I/O scaling. */
    private final ExecutorService streamingPool = Executors.newVirtualThreadPerTaskExecutor();

    /** Fair admission semaphore bounding simultaneous active local partition streams (ADR-02). */
    private final Semaphore localStreamPermits;

    /** Thread-safe bounded pool of reusable 65KB Direct ByteBuffers (ADR-10). */
    private final BlockingQueue<ByteBuffer> bufferPool;

    /** Concurrency gate to prevent exhausting MinIO connection pools during massive cache misses. */
    private final Semaphore ioPermits;

    /** Per-(jobId, partitionId) single-flight lock registry preventing concurrent restore races. */
    private final ConcurrentHashMap<String, PartitionLockEntry> restoreLocks = new ConcurrentHashMap<>();

    /** Cryptographic key specification for stateless token validation. */
    private final SecretKeySpec secretKeySpec;

    private static final class PartitionLockEntry {
        final ReentrantLock lock = new ReentrantLock();
        int refCount = 0;
    }

    /**
     * Initializes the gRPC Shuffle Data Plane using environment-configured concurrency bounds
     * ({@code ESS_MAX_LOCAL_STREAMS}, {@code ESS_STREAM_ACQUIRE_TIMEOUT_MS},
     * {@code ESS_MAX_CONCURRENT_IO}, and {@code ESS_SECRET_KEY}).
     *
     * @param baseShuffleDir      The root physical directory containing localized partition data.
     * @param minioStorageService The configured S3 adapter for restoring spilled partitions (may be {@code null} in degraded local-only mode).
     * @throws IllegalArgumentException If configured concurrency or timeout values are out of range.
     */
    public ShuffleServiceImpl(String baseShuffleDir, MinioStorageService minioStorageService) {
        this(
                baseShuffleDir,
                minioStorageService,
                resolveMaxLocalStreams(System.getenv()),
                resolveAcquireTimeoutMs(System.getenv())
        );
    }

    /**
     * Initializes the gRPC Shuffle Data Plane with explicit local stream admission bounds
     * for deterministic backpressure and concurrency unit testing.
     *
     * @param baseShuffleDir      The root physical directory containing localized partition data.
     * @param minioStorageService The configured S3 adapter for restoring spilled partitions (may be {@code null} in degraded local-only mode).
     * @param maxLocalStreams     Maximum simultaneous active streams permitted (must be &ge; 1).
     * @param acquireTimeoutMs    Admission wait timeout in milliseconds before rejecting with {@code RESOURCE_EXHAUSTED} (must be &ge; 0).
     * @throws IllegalArgumentException If {@code maxLocalStreams < 1} or {@code acquireTimeoutMs < 0}.
     */
    public ShuffleServiceImpl(String baseShuffleDir, MinioStorageService minioStorageService,
                              int maxLocalStreams, long acquireTimeoutMs) {
        if (maxLocalStreams < 1) {
            throw new IllegalArgumentException("maxLocalStreams must be >= 1, got: " + maxLocalStreams);
        }
        if (acquireTimeoutMs < 0) {
            throw new IllegalArgumentException("acquireTimeoutMs must be >= 0, got: " + acquireTimeoutMs);
        }
        this.baseShuffleDir = baseShuffleDir;
        this.minioStorageService = minioStorageService;
        this.maxLocalStreams = maxLocalStreams;
        this.acquireTimeoutMs = acquireTimeoutMs;
        this.localStreamPermits = new Semaphore(maxLocalStreams, true);
        this.bufferPool = new ArrayBlockingQueue<>(maxLocalStreams);

        String secretKey = System.getenv().getOrDefault("ESS_SECRET_KEY", "dev-insecure-shared-secret");
        this.secretKeySpec = new SecretKeySpec(secretKey.getBytes(StandardCharsets.UTF_8), "HmacSHA256");

        int maxConcurrentIo = Integer.parseInt(System.getenv().getOrDefault("ESS_MAX_CONCURRENT_IO", "150"));
        this.ioPermits = new Semaphore(maxConcurrentIo, true);
        logger.info("ShuffleService Data Plane initialized. MaxLocalStreams: {}, AcquireTimeoutMs: {}, S3 Restore Ceiling: {}.",
                maxLocalStreams, acquireTimeoutMs, maxConcurrentIo);
    }

    /**
     * Resolves {@code ESS_MAX_LOCAL_STREAMS} from environment variables, defaulting to
     * {@value #DEFAULT_MAX_LOCAL_STREAMS}.
     *
     * @param env Environment variable map.
     * @return Positive local stream concurrency ceiling.
     * @throws IllegalArgumentException If the value is non-numeric or &lt; 1.
     */
    public static int resolveMaxLocalStreams(Map<String, String> env) {
        String raw = env.get("ESS_MAX_LOCAL_STREAMS");
        if (raw == null || raw.isBlank()) {
            return DEFAULT_MAX_LOCAL_STREAMS;
        }
        int parsed = Integer.parseInt(raw.trim());
        if (parsed < 1) {
            throw new IllegalArgumentException("ESS_MAX_LOCAL_STREAMS must be >= 1, got: " + parsed);
        }
        return parsed;
    }

    /**
     * Resolves {@code ESS_STREAM_ACQUIRE_TIMEOUT_MS} from environment variables, defaulting to
     * {@value #DEFAULT_STREAM_ACQUIRE_TIMEOUT_MS} ms.
     *
     * @param env Environment variable map.
     * @return Non-negative acquisition timeout in milliseconds.
     * @throws IllegalArgumentException If the value is non-numeric or &lt; 0.
     */
    public static long resolveAcquireTimeoutMs(Map<String, String> env) {
        String raw = env.get("ESS_STREAM_ACQUIRE_TIMEOUT_MS");
        if (raw == null || raw.isBlank()) {
            return DEFAULT_STREAM_ACQUIRE_TIMEOUT_MS;
        }
        long parsed = Long.parseLong(raw.trim());
        if (parsed < 0) {
            throw new IllegalArgumentException("ESS_STREAM_ACQUIRE_TIMEOUT_MS must be >= 0, got: " + parsed);
        }
        return parsed;
    }

    /**
     * Handles an inbound server-streaming {@code GetPartition} gRPC request.
     * <p>
     * Validates HMAC-SHA256 authorization and job identifier syntax synchronously, binds
     * HTTP/2 flow-control callbacks, and dispatches execution onto a Virtual Thread where
     * admission is gated by {@link #localStreamPermits}.
     * </p>
     *
     * @param request          The partition fetch request containing {@code job_id} and {@code partition_id}.
     * @param responseObserver Downstream gRPC response stream observer.
     */
    @Override
    public void getPartition(PartitionRequest request, StreamObserver<PartitionChunk> responseObserver) {
        String rawJobId = request.getJobId();
        int partitionId = request.getPartitionId();
        String partitionIdStr = String.valueOf(partitionId);

        try (MDC.MDCCloseable jobMdc = MDC.putCloseable("job_id", rawJobId);
             MDC.MDCCloseable partMdc = MDC.putCloseable("partition_id", partitionIdStr)) {
            logger.info("Inbound Stream Request: [Job: {}, Partition: {}]", rawJobId, partitionId);

            // 1. Guard Clause: Zero-Trust Cryptographic Authorization
            if (!isAuthorized(rawJobId)) {
                sendError(responseObserver, Status.UNAUTHENTICATED, "Invalid Cryptographic Signature.");
                return;
            }

            // 2. Guard Clause: Strict Path Sanitization
            String safeJobId;
            try {
                safeJobId = sanitizeId(rawJobId);
            } catch (SecurityException se) {
                logger.warn("Security Alert: Path Traversal Attempted by Job ID: {}", rawJobId);
                sendError(responseObserver, Status.INVALID_ARGUMENT, "Malformed job identifier.");
                return;
            }

            if (streamingPool.isShutdown()) {
                sendError(responseObserver, Status.UNAVAILABLE, "ESS daemon is shutting down.");
                return;
            }

            Path partitionDir = Paths.get(baseShuffleDir, safeJobId, partitionIdStr);
            ServerCallStreamObserver<PartitionChunk> flowController = (ServerCallStreamObserver<PartitionChunk>) responseObserver;

            // 3. Setup Virtual-Thread-Safe Flow Control
            final ReentrantLock flowLock = new ReentrantLock();
            final Condition flowCondition = flowLock.newCondition();

            // Signal waiting virtual threads when the network buffer clears
            flowController.setOnReadyHandler(() -> {
                flowLock.lock();
                try {
                    flowCondition.signalAll();
                } finally {
                    flowLock.unlock();
                }
            });

            // Signal waiting virtual threads to abort cleanly if the client disconnects
            flowController.setOnCancelHandler(() -> {
                flowLock.lock();
                try {
                    flowCondition.signalAll();
                } finally {
                    flowLock.unlock();
                }
            });

            // 4. Delegate heavy I/O to the Virtual Thread pool
            try {
                streamingPool.submit(() -> {
                    try (MDC.MDCCloseable vtJobMdc = MDC.putCloseable("job_id", safeJobId);
                         MDC.MDCCloseable vtPartMdc = MDC.putCloseable("partition_id", partitionIdStr)) {
                        executeStreamingTask(safeJobId, partitionId, partitionDir, flowController, flowLock, flowCondition);
                    }
                });
            } catch (RejectedExecutionException rejected) {
                logger.warn("Rejected stream request during shutdown: job_id={}, partition_id={}", safeJobId, partitionId);
                sendError(responseObserver, Status.UNAVAILABLE, "ESS daemon is shutting down.");
            }
        }
    }

    /**
     * Core asynchronous execution block. Acquires a bounded stream permit, acquires a job lease
     * protecting against Janitor eviction, resolves tiered storage, and streams partition data.
     *
     * @param jobId         Sanitized job identifier.
     * @param partitionId   Target partition index.
     * @param partitionDir  Expected local directory for the partition segments.
     * @param observer      Flow-controlled gRPC stream observer.
     * @param flowLock      Lock coordinating HTTP/2 readiness and cancellation signals.
     * @param flowCondition Condition variable signaled when transport readiness changes.
     */
    private void executeStreamingTask(String jobId, int partitionId, Path partitionDir,
                                      ServerCallStreamObserver<PartitionChunk> observer,
                                      ReentrantLock flowLock, Condition flowCondition) {
        boolean permitAcquired = false;
        try {
            permitAcquired = localStreamPermits.tryAcquire(acquireTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (!observer.isCancelled()) {
                sendError(observer, Status.CANCELLED, "Interrupted while awaiting local stream admission.");
            }
            return;
        }

        if (!permitAcquired) {
            logger.warn("Backpressure: ESS_MAX_LOCAL_STREAMS ({}) saturated after {}ms for job_id={}, partition_id={}",
                    maxLocalStreams, acquireTimeoutMs, jobId, partitionId);
            sendError(observer, Status.RESOURCE_EXHAUSTED,
                    "ESS local stream concurrency limit (" + maxLocalStreams + ") reached; retry later.");
            return;
        }

        try {
            if (observer.isCancelled()) {
                logger.debug("Stream cancelled by client prior to execution for job_id={}, partition_id={}", jobId, partitionId);
                return;
            }

            StorageJanitor.acquireJobLease(jobId);
            try {
                // Attempt to resolve data (either locally or via S3 restore).
                // Returns false if it handled a data-skew empty partition or emitted a gRPC error internally.
                if (!resolveStorageTier(jobId, partitionId, partitionDir, observer)) {
                    return;
                }

                // If we reach here, data exists locally. Stream it.
                streamDirectory(jobId, partitionId, partitionDir, observer, flowLock, flowCondition);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (!observer.isCancelled()) {
                    sendError(observer, Status.CANCELLED, "I/O Interrupted before execution.");
                }
            } catch (Exception e) {
                logger.error("Data Plane Error for directory: {}", partitionDir, e);
                if (!observer.isCancelled()) {
                    sendError(observer, Status.INTERNAL, e.getMessage());
                }
            } finally {
                StorageJanitor.releaseJobLease(jobId);
            }
        } finally {
            localStreamPermits.release();
        }
    }

    /**
     * Resolves cache misses by pulling from MinIO under a per-(jobId, partitionId) single-flight lock.
     * <p>
     * <b>ADR-15 Degraded State Contract:</b> When {@code partitionDir} is absent locally and
     * {@code minioStorageService == null}, this method fails loud with
     * {@link Status#FAILED_PRECONDITION} instead of returning a misleading empty stream.
     * </p>
     *
     * @param jobId        Sanitized job identifier.
     * @param partitionId  Target partition index.
     * @param partitionDir Local partition directory path.
     * @param observer     Downstream gRPC stream observer.
     * @return {@code true} if data is available locally to stream, {@code false} if an empty stream or error was emitted.
     * @throws InterruptedException If the thread is interrupted while waiting for the lock or an I/O permit.
     */
    private boolean resolveStorageTier(String jobId, int partitionId, Path partitionDir, StreamObserver<PartitionChunk> observer) throws InterruptedException {
        String lockKey = jobId + ":" + partitionId;
        PartitionLockEntry lockEntry = restoreLocks.compute(lockKey, (k, existing) -> {
            PartitionLockEntry entry = (existing != null) ? existing : new PartitionLockEntry();
            entry.refCount++;
            return entry;
        });

        boolean lockAcquired = false;
        try {
            lockEntry.lock.lockInterruptibly();
            lockAcquired = true;

            // Fast path (serialized per (jobId, partitionId)): Data is already local
            if (Files.exists(partitionDir)) {
                return true;
            }

            // ADR-15: Fail loud when overflow storage is disabled/unavailable and local data is absent
            if (minioStorageService == null) {
                logger.error("MinIO overflow storage is unavailable and partition directory {} is absent on local disk", partitionDir);
                sendError(observer, Status.FAILED_PRECONDITION,
                        "MinIO overflow storage is unavailable and partition directory is absent on local disk");
                return false;
            }

            // Slow path: Cache miss. Acquire permit and attempt MinIO restore.
            ioPermits.acquire();
            try {
                // Double-check after acquiring permit
                if (Files.exists(partitionDir)) {
                    return true;
                }

                logger.warn("Cache Miss for Partition {}. Restoring from S3...", partitionDir);
                boolean restored = minioStorageService.restorePartition(jobId, partitionId, partitionDir);
                if (!restored) {
                    handleEmptyPartition(observer, partitionDir);
                    return false;
                }
                return true;

            } catch (IOException | RuntimeException e) {
                logger.error("S3 restore failed for Job {} Partition {}: {}", jobId, partitionId, e.getMessage(), e);
                sendError(observer, Status.UNAVAILABLE,
                        "Failed to restore partition from overflow storage: " + e.getMessage());
                return false;
            } finally {
                ioPermits.release();
            }
        } finally {
            if (lockAcquired) {
                lockEntry.lock.unlock();
            }
            restoreLocks.computeIfPresent(lockKey, (k, existing) -> {
                if (existing == lockEntry && --existing.refCount == 0) {
                    return null;
                }
                return existing;
            });
        }
    }

    /**
     * Locates finalized {@code .lz4} files in {@code partitionDir} (ignoring {@code .tmp} and
     * {@code .attempt} marker files) and streams them using a single pooled Direct {@link ByteBuffer}
     * leased for the lifetime of the stream.
     *
     * @param jobId         Sanitized job identifier.
     * @param partitionId   Target partition index.
     * @param partitionDir  Local directory containing finalized partition segments.
     * @param observer      Flow-controlled gRPC stream observer.
     * @param flowLock      Lock protecting HTTP/2 readiness signaling.
     * @param flowCondition Condition variable signaled when transport readiness changes.
     * @throws IOException If directory listing or segment file I/O fails.
     */
    private void streamDirectory(String jobId, int partitionId, Path partitionDir,
                                 ServerCallStreamObserver<PartitionChunk> observer,
                                 ReentrantLock flowLock, Condition flowCondition) throws IOException {

        List<Path> validFiles;
        try (Stream<Path> filePaths = Files.list(partitionDir)) {
            validFiles = filePaths
                    .filter(Files::isRegularFile)
                    .filter(p -> {
                        String name = p.getFileName().toString();
                        return name.endsWith(".lz4") && !name.endsWith(".tmp") && !name.endsWith(".attempt");
                    })
                    .sorted()
                    .toList();
        }

        // Skew Resolution: Directory exists locally but contains zero finalized .lz4 frames
        if (validFiles.isEmpty()) {
            handleEmptyPartition(observer, partitionDir);
            return;
        }

        // ADR-10: Lease ONE pooled Direct ByteBuffer for all segment files in this stream
        ByteBuffer buffer = acquireDirectBuffer();
        try {
            for (Path file : validFiles) {
                streamFileContent(file, buffer, observer, flowLock, flowCondition);
                if (observer.isCancelled() || Thread.currentThread().isInterrupted()) {
                    break;
                }
            }

            if (!observer.isCancelled() && !Thread.currentThread().isInterrupted()) {
                observer.onCompleted();
                logger.info("Stream Concluded: Partition {} (Job: {})", partitionId, jobId);
            }
        } finally {
            releaseDirectBuffer(buffer);
        }
    }

    /**
     * Streams the physical content of a single file segment using the caller-provided pooled
     * NIO Direct {@link ByteBuffer}. Incorporates Virtual-Thread-safe locking to gracefully
     * handle gRPC network backpressure without emitting hot-loop {@code INFO} logs.
     *
     * @param file           Path to the finalized {@code .lz4} segment file.
     * @param buffer         Reusable Direct {@link ByteBuffer} leased for this stream.
     * @param flowController Flow-controlled gRPC stream observer.
     * @param flowLock       Lock coordinating readiness/cancel callbacks.
     * @param flowCondition  Condition variable awaited when transport is not ready.
     * @throws IOException If opening or reading the segment file fails.
     */
    private void streamFileContent(Path file, ByteBuffer buffer,
                                   ServerCallStreamObserver<PartitionChunk> flowController,
                                   ReentrantLock flowLock, Condition flowCondition) throws IOException {

        if (flowController.isCancelled()) {
            return;
        }

        buffer.clear();
        try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.READ)) {
            while (fileChannel.read(buffer) > 0) {
                buffer.flip();

                // Apply Network Backpressure safely without pinning the Carrier Thread
                flowLock.lock();
                try {
                    while (!flowController.isReady() && !flowController.isCancelled()) {
                        flowCondition.await();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } finally {
                    flowLock.unlock();
                }

                if (flowController.isCancelled()) {
                    break;
                }

                transmitChunk(buffer, flowController);
                buffer.clear();
            }
        }
    }

    /**
     * Leases a cleared {@value #CHUNK_SIZE_BYTES}-byte Direct {@link ByteBuffer} from the bounded
     * pool, or allocates a fresh Direct {@link ByteBuffer} if the pool is empty.
     *
     * @return Cleared Direct {@link ByteBuffer} ready for channel reads.
     */
    protected ByteBuffer acquireDirectBuffer() {
        ByteBuffer buffer = bufferPool.poll();
        if (buffer == null) {
            buffer = ByteBuffer.allocateDirect(CHUNK_SIZE_BYTES);
        }
        buffer.clear();
        return buffer;
    }

    /**
     * Clears and returns a leased Direct {@link ByteBuffer} to the bounded pool. If the pool
     * is already at capacity, the excess buffer is safely discarded for GC collection.
     *
     * @param buffer The buffer to return (no-op if {@code null}).
     */
    protected void releaseDirectBuffer(ByteBuffer buffer) {
        if (buffer != null) {
            buffer.clear();
            bufferPool.offer(buffer);
        }
    }

    /**
     * Returns the number of Direct {@link ByteBuffer} instances currently idle in the pool.
     *
     * @return Current idle buffer count in {@link #bufferPool}.
     */
    public int getPooledBufferCount() {
        return bufferPool.size();
    }

    /**
     * Returns the number of available local stream admission permits.
     *
     * @return Current available permits in {@link #localStreamPermits}.
     */
    public int getAvailableStreamPermits() {
        return localStreamPermits.availablePermits();
    }

    /**
     * Initiates an orderly shutdown of {@link #streamingPool}, awaiting active virtual-thread
     * streams to finish up to the specified timeout before escalating to {@code shutdownNow()}.
     *
     * @param timeout Maximum duration to wait for active streams to drain.
     * @param unit    Time unit of {@code timeout}.
     * @return {@code true} if all streaming tasks drained cleanly within the timeout;
     *         {@code false} if forced termination was required or the caller was interrupted.
     */
    public boolean shutdownAndAwait(long timeout, TimeUnit unit) {
        if (streamingPool.isTerminated()) {
            return true;
        }
        logger.info("Draining ShuffleServiceImpl streamingPool (timeout: {} {})...", timeout, unit);
        streamingPool.shutdown();
        try {
            if (!streamingPool.awaitTermination(timeout, unit)) {
                logger.warn("streamingPool did not drain within {} {}; forcing shutdownNow().", timeout, unit);
                streamingPool.shutdownNow();
                return false;
            }
            logger.info("ShuffleServiceImpl streamingPool drained cleanly.");
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Interrupted while draining streamingPool; forcing shutdownNow().");
            streamingPool.shutdownNow();
            return false;
        }
    }

    /**
     * Shuts down the gRPC streaming pool, awaiting active transfers for up to
     * {@value #DEFAULT_DRAIN_TIMEOUT_SECONDS} seconds.
     */
    @Override
    public void close() {
        shutdownAndAwait(DEFAULT_DRAIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    // ==========================================
    // Utility & Security Methods Below
    // ==========================================

    /**
     * Validates the cryptographically signed HMAC token injected by the gRPC Interceptor.
     *
     * @param rawJobId Job identifier extracted from the {@link PartitionRequest}.
     * @return {@code true} if the HMAC-SHA256 header matches the expected signature.
     */
    private boolean isAuthorized(String rawJobId) {
        String providedToken = ShuffleDaemon.AUTH_TOKEN_KEY.get();
        String expectedToken = computeHmacSha256(rawJobId);

        if (providedToken == null || !MessageDigest.isEqual(providedToken.getBytes(StandardCharsets.UTF_8), expectedToken.getBytes(StandardCharsets.UTF_8))) {
            logger.warn("SECURITY ALERT: HMAC Mismatch for Job {}. Token Rejected.", rawJobId);
            return false;
        }
        return true;
    }

    /**
     * Safely resolves verified empty partitions (data skew) by transmitting a zero-byte
     * payload and completing the stream with {@code OK}.
     *
     * @param observer Downstream gRPC stream observer.
     * @param dir      Verified local or S3 partition path.
     */
    private void handleEmptyPartition(StreamObserver<PartitionChunk> observer, Path dir) {
        logger.info("Data Skew Resolution: Injecting empty chunk for partition {}", dir);
        observer.onNext(PartitionChunk.newBuilder().setContent(ByteString.EMPTY).build());
        observer.onCompleted();
    }

    private void sendError(StreamObserver<PartitionChunk> observer, Status status, String description) {
        observer.onError(status.withDescription(description).asRuntimeException());
    }

    private String sanitizeId(String input) {
        if (input == null || !ID_PATTERN.matcher(input).matches()) {
            throw new SecurityException("Illegal character set in identifier. Potential path traversal detected.");
        }
        return input;
    }

    private String computeHmacSha256(String data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(secretKeySpec);
            byte[] hashBytes = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
            StringBuilder hexString = new StringBuilder();
            for (byte b : hashBytes) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) hexString.append('0');
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (Exception e) {
            throw new RuntimeException("Cryptographic provider initialization failed.", e);
        }
    }

    private void transmitChunk(ByteBuffer buffer, StreamObserver<PartitionChunk> observer) {
        observer.onNext(PartitionChunk.newBuilder().setContent(ByteString.copyFrom(buffer)).build());
    }
}
