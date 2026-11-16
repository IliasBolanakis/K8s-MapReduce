package com.iliasbolan.services;

import com.iliasbolan.infrastructure.MinioConnectionManager;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.StatObjectArgs;
import io.minio.StatObjectResponse;
import io.minio.UploadObjectArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * High-performance I/O service for distributed storage orchestration.
 * <p>
 * Encapsulates all interactions between the Worker node and the S3-compatible storage
 * layer (MinIO), providing:
 * <ul>
 * <li><b>Resilience:</b> {@code Resilience4j} retries with exponential random backoff.</li>
 * <li><b>Zero-Loss Boundary Correction (Lookback Pattern):</b> Evaluates {@code offset - 1}
 * to preserve aligned records without skipping across splits.</li>
 * <li><b>Bounded Range Reads &amp; Socket Reuse:</b> Bounds every {@code GetObjectArgs}
 * request with {@code .length(clampedLength)} using a {@code 1 MiB} lookahead clamped against
 * cached {@code statObject} size (preventing HTTP 416), drains unread bytes before closing so
 * OkHttp reuses keep-alive sockets, and issues bounded continuation fetches when a single line
 * straddles past the initial {@code fetchLength + 1 MiB} window (up to the {@code 100 MiB}
 * OOM guard).</li>
 * </ul>
 * </p>
 * <p>
 * <b>Thread Safety:</b><br>
 * Instances are thread-safe assuming immutable input objects per {@code (bucket, object)} key;
 * object sizes are memoized in a {@link ConcurrentHashMap}.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 3.3
 * @see <a href="https://resilience4j.readme.io/">Resilience4j Documentation</a>
 * @since 2026-03-30
 */
public class S3ClientService {

    private static final Logger logger = LoggerFactory.getLogger(S3ClientService.class);

    /** Default lookahead window (1 MiB) appended to bounded range requests for trailing-line completion. */
    public static final long DEFAULT_LOOKAHEAD_BYTES = 1_048_576L;

    /** Absolute maximum record size (100 MiB) permitted before triggering the OOM security guard. */
    static final int MAX_LINE_SIZE_BYTES = 100 * 1024 * 1024;

    private static final int DRAIN_BUFFER_BYTES = 8192;

    private final MinioClient minioClient;
    private final Retry retryContext;
    private final long lookaheadBytes;
    private final ConcurrentHashMap<String, Long> objectSizeCache = new ConcurrentHashMap<>();

    /**
     * Constructs the service with default {@code 1 MiB} lookahead and Resilience4j retry configuration.
     *
     * @param connectionManager Provider for the authenticated {@link MinioClient}.
     */
    public S3ClientService(MinioConnectionManager connectionManager) {
        this(connectionManager, DEFAULT_LOOKAHEAD_BYTES);
    }

    /**
     * Constructs the service with a custom range lookahead size (primarily for unit testing).
     *
     * @param connectionManager Provider for the authenticated {@link MinioClient}.
     * @param lookaheadBytes    Positive lookahead window in bytes added to bounded range reads.
     * @throws IllegalArgumentException If {@code lookaheadBytes <= 0}.
     */
    S3ClientService(MinioConnectionManager connectionManager, long lookaheadBytes) {
        if (lookaheadBytes <= 0) {
            throw new IllegalArgumentException("lookaheadBytes must be positive: " + lookaheadBytes);
        }
        this.minioClient = connectionManager.getClient();
        this.lookaheadBytes = lookaheadBytes;

        RetryConfig config = RetryConfig.custom()
                .maxAttempts(3)
                .intervalFunction(IntervalFunction.ofExponentialRandomBackoff(1000, 2.0, 0.5))
                .retryExceptions(Exception.class)
                .build();

        this.retryContext = RetryRegistry.of(config).retry("minio-communication-retry");
        logger.info("Initialized S3ClientService with Resilience4j Fault-Tolerance (lookahead={}B).", lookaheadBytes);
    }

    /**
     * Downloads executable bytecode from shared storage to a local path via atomic staging.
     *
     * @param bucketName      S3 bucket containing the code artifacts (e.g., {@code "code"}).
     * @param objectName      Sandboxed S3 key for the bytecode (e.g., {@code "<user_id>/Mapper.class"}).
     * @param destinationPath Local file system path for target bytecode persistence.
     * @throws Throwable If retrieval fails or disk persistence cannot complete.
     */
    public void downloadUserCode(String bucketName, String objectName, String destinationPath) throws Throwable {
        Path targetPath = Paths.get(destinationPath).toAbsolutePath();
        if (Files.exists(targetPath)) {
            logger.debug("User code already localized at {}; skipping S3 download.", targetPath);
            return;
        }
        Retry.decorateCheckedRunnable(retryContext, () -> {
            if (Files.exists(targetPath)) {
                logger.debug("User code already localized at {}; skipping S3 download.", targetPath);
                return;
            }
            logger.info("Downloading user code from s3://{}/{} to local path: {}", bucketName, objectName, destinationPath);

            Path parentDir = targetPath.getParent();
            if (parentDir != null) {
                Files.createDirectories(parentDir);
            }

            Path tempPath = targetPath.resolveSibling(targetPath.getFileName() + "." + UUID.randomUUID() + ".tmp");
            try (InputStream stream = minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket(bucketName)
                            .object(objectName)
                            .build())) {

                Files.copy(stream, tempPath, StandardCopyOption.REPLACE_EXISTING);
                Files.move(tempPath, targetPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                logger.info("Successfully downloaded user code to: {}", targetPath);
            } finally {
                Files.deleteIfExists(tempPath);
            }
        }).run();
    }

    /**
     * Reads a boundary-corrected chunk of records using clamped S3 range requests.
     * <p>
     * Bounds {@code GetObjectArgs} with {@code .length(clampedLength)} where
     * {@code clampedLength = min(objectSize - currentOffset, fetchLength + lookahead)} (or
     * {@code lookahead} on continuation windows), preventing HTTP 416 errors and avoiding
     * unbounded stream aborts. Drains unread bytes within the bounded stream before closing
     * so OkHttp reuses HTTP keep-alive connections, and issues bounded continuation fetches
     * whenever a record straddles past the initial {@code fetchLength + lookahead} window.
     * </p>
     *
     * @param bucketName Target S3 bucket (e.g., {@code "data"}).
     * @param objectName Sandboxed S3 key of the source file.
     * @param offset     Logical starting byte offset.
     * @param length     Target logical chunk byte length.
     * @return A {@link List} of UTF-8 text records with line endings stripped.
     * @throws Throwable If object retrieval fails, decoding fails, or a line exceeds {@code 100 MiB}.
     */
    public List<String> readDataChunk(String bucketName, String objectName, long offset, long length) throws Throwable {
        if (length <= 0) {
            return Collections.emptyList();
        }

        return Retry.<List<String>>decorateCheckedSupplier(retryContext, () -> {
            logger.debug("Reading chunk from s3://{}/{} offset={}, targetLength={}",
                    bucketName, objectName, offset, length);

            long objectSize = resolveObjectSize(bucketName, objectName);
            long fetchOffset = offset > 0 ? offset - 1 : 0;
            long fetchLength = offset > 0 ? length + 1 : length;

            if (objectSize >= 0 && fetchOffset >= objectSize) {
                return Collections.<String>emptyList();
            }

            int estimatedRecordCount = (int) Math.min(1_000_000L, Math.max(16L, length / 100));
            List<String> cleanRecords = new ArrayList<>(estimatedRecordCount);

            long currentOffset = fetchOffset;
            long bytesProcessedInChunk = 0L;
            boolean lookbackEvaluated = (offset == 0);
            boolean skipFirstRecord = false;
            boolean quotaFulfilled = false;
            ByteArrayOutputStream lineBuffer = new ByteArrayOutputStream(256);
            byte[] drainBuffer = new byte[DRAIN_BUFFER_BYTES];

            while (!quotaFulfilled) {
                long desiredLength = (currentOffset == fetchOffset)
                        ? fetchLength + lookaheadBytes
                        : Math.max(lookaheadBytes, Math.max(0L, fetchLength - bytesProcessedInChunk) + lookaheadBytes);

                long clampedLength = clampRequestLength(desiredLength, currentOffset, objectSize);
                if (clampedLength <= 0) {
                    break;
                }

                GetObjectArgs args = GetObjectArgs.builder()
                        .bucket(bucketName)
                        .object(objectName)
                        .offset(currentOffset)
                        .length(clampedLength)
                        .build();

                long bytesReadThisWindow = 0L;
                boolean abortDrain = false;

                try (InputStream rawStream = minioClient.getObject(args);
                     BufferedInputStream stream = new BufferedInputStream(rawStream, 32768)) {

                    if (!lookbackEvaluated) {
                        int lookbackByte = stream.read();
                        if (lookbackByte == -1) {
                            break;
                        }
                        bytesReadThisWindow++;
                        bytesProcessedInChunk++;
                        lookbackEvaluated = true;
                        if (lookbackByte != 0x0A) {
                            skipFirstRecord = true;
                        }
                    }

                    if (skipFirstRecord) {
                        int b;
                        while ((b = stream.read()) != -1) {
                            bytesReadThisWindow++;
                            bytesProcessedInChunk++;
                            if (b == 0x0A) {
                                skipFirstRecord = false;
                                break;
                            }
                        }
                    }

                    if (!skipFirstRecord) {
                        int b;
                        while ((b = stream.read()) != -1) {
                            bytesReadThisWindow++;
                            bytesProcessedInChunk++;
                            lineBuffer.write(b);

                            if (lineBuffer.size() > MAX_LINE_SIZE_BYTES) {
                                abortDrain = true;
                                throw new IOException(
                                        String.format("Security/OOM Guard Triggered: Encountered a line exceeding %d bytes.", MAX_LINE_SIZE_BYTES)
                                );
                            }

                            if (b == 0x0A) {
                                String line = stripLineEndings(lineBuffer.toString(StandardCharsets.UTF_8));
                                if (!line.isEmpty()) {
                                    cleanRecords.add(line);
                                }

                                if (lineBuffer.size() > 1024 * 1024) {
                                    lineBuffer = new ByteArrayOutputStream(256);
                                } else {
                                    lineBuffer.reset();
                                }

                                if (bytesProcessedInChunk >= fetchLength) {
                                    quotaFulfilled = true;
                                    break;
                                }
                            }
                        }
                    }

                    if (!abortDrain) {
                        while (stream.read(drainBuffer) != -1) {
                            // Consume bounded remainder so OkHttp returns the socket to the keep-alive pool.
                        }
                    }
                }

                currentOffset += bytesReadThisWindow;

                if (quotaFulfilled || bytesReadThisWindow == 0) {
                    break;
                }
                if (objectSize >= 0 && currentOffset >= objectSize) {
                    break;
                }
                logger.debug("Record straddled past bounded window at offset {}; issuing continuation range fetch.", currentOffset);
            }

            if (lineBuffer.size() > 0) {
                String lastLine = stripLineEndings(lineBuffer.toString(StandardCharsets.UTF_8));
                if (!lastLine.isEmpty()) {
                    cleanRecords.add(lastLine);
                }
            }

            logger.debug("Chunk processing complete. Offset: {}, Records parsed: {}", offset, cleanRecords.size());
            return cleanRecords;
        }).get();
    }

    /**
     * Resolves and caches the total byte size of an S3 object via {@code statObject}.
     *
     * @param bucketName Target S3 bucket.
     * @param objectName Target S3 object key.
     * @return Object size in bytes, or {@code -1} if metadata is unavailable.
     * @throws Exception If {@code statObject} fails on the underlying client.
     */
    private long resolveObjectSize(String bucketName, String objectName) throws Exception {
        String cacheKey = bucketName + "/" + objectName;
        Long cached = objectSizeCache.get(cacheKey);
        if (cached != null) {
            return cached;
        }
        StatObjectResponse stat = minioClient.statObject(
                StatObjectArgs.builder()
                        .bucket(bucketName)
                        .object(objectName)
                        .build()
        );
        if (stat == null) {
            return -1L;
        }
        long size = stat.size();
        if (size >= 0) {
            objectSizeCache.put(cacheKey, size);
        }
        return size;
    }

    private static long clampRequestLength(long desiredLength, long currentOffset, long objectSize) {
        if (objectSize < 0) {
            return desiredLength;
        }
        long remaining = objectSize - currentOffset;
        if (remaining <= 0) {
            return 0L;
        }
        return Math.min(desiredLength, remaining);
    }

    private static String stripLineEndings(String line) {
        if (line.endsWith("\r\n")) {
            return line.substring(0, line.length() - 2);
        }
        if (line.endsWith("\n") || line.endsWith("\r")) {
            return line.substring(0, line.length() - 1);
        }
        return line;
    }

    /**
     * Persists UTF-8 string output to shared object storage.
     *
     * @param bucketName Target S3 bucket (e.g., {@code "results"}).
     * @param objectName Sandboxed path (e.g., {@code "<user_id>/<job_id>/part_n.txt"}).
     * @param data       Raw text results to be uploaded.
     * @throws Throwable If the upload is rejected by the storage cluster.
     */
    public void writeData(String bucketName, String objectName, String data) throws Throwable {
        Retry.decorateCheckedRunnable(retryContext, () -> {
            byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);
            logger.debug("Writing {} bytes to s3://{}/{}", dataBytes.length, bucketName, objectName);

            try (InputStream inputStream = new ByteArrayInputStream(dataBytes)) {
                minioClient.putObject(
                        PutObjectArgs.builder()
                                .bucket(bucketName)
                                .object(objectName)
                                .stream(inputStream, dataBytes.length, -1)
                                .contentType("text/plain")
                                .build());

                logger.debug("Successfully wrote object to s3://{}/{}", bucketName, objectName);
            }
        }).run();
    }

    /**
     * Streams a local file directly to S3 with O(1) heap usage.
     *
     * @param bucketName Target S3 bucket (e.g., {@code "results"}).
     * @param objectName Sandboxed path (e.g., {@code "<user_id>/<job_id>/result_part_0.txt"}).
     * @param filePath   The local physical file to be uploaded.
     * @throws Throwable If the upload fails or the local file cannot be read.
     */
    public void uploadFileFromDisk(String bucketName, String objectName, Path filePath) throws Throwable {
        Retry.decorateCheckedRunnable(retryContext, () -> {
            logger.debug("Streaming file {} to s3://{}/{}", filePath, bucketName, objectName);

            minioClient.uploadObject(
                    UploadObjectArgs.builder()
                            .bucket(bucketName)
                            .object(objectName)
                            .filename(filePath.toAbsolutePath().toString())
                            .contentType("text/plain")
                            .build());

            logger.debug("Successfully streamed file to S3.");
        }).run();
    }
}
