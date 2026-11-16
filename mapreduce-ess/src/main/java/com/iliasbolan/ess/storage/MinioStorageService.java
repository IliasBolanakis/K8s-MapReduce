package com.iliasbolan.ess.storage;

import io.minio.*;
import io.minio.messages.Item;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Enterprise-grade MinIO Storage adapter for the External Shuffle Service (ESS) overflow tier.
 * <p>
 * This service manages the seamless offloading (spilling) of intermediate MapReduce shuffle
 * data to object storage when physical node disk utilization reaches critical thresholds.
 * By persisting state to an S3-compatible backend, it safely decouples storage from compute,
 * allowing KEDA-driven ephemeral job pods to scale down or terminate in multitenant environments
 * without causing data loss for lagging Reducers.
 * </p>
 * <p>
 * <b>Performance &amp; Concurrency Architecture (ADR-02 Global S3 Transfer Pool):</b><br>
 * Utilizes direct physical-to-network streaming for O(1) memory OOM protection during massive
 * spills and restorations. All MinIO {@code spillJobData} uploads and {@code restorePartition}
 * downloads share a single bounded global transfer {@link ExecutorService} sized via
 * {@code ESS_MAX_S3_CONCURRENCY} (default {@value #DEFAULT_MAX_S3_CONCURRENCY}). This prevents
 * per-request executor proliferation and guarantees total concurrent MinIO HTTP operations
 * across the entire ESS daemon never exceed the configured cap.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 3.0
 * @since 2026-05-13
 */
public class MinioStorageService implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(MinioStorageService.class);

    /** Default global cap on concurrent MinIO S3 upload and download operations. */
    public static final int DEFAULT_MAX_S3_CONCURRENCY = 32;

    /** Default shutdown drain window (in seconds) for the shared S3 transfer pool. */
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 15L;

    private final MinioClient minioClient;
    private final String bucketName;
    private final int maxS3Concurrency;

    /**
     * Shared bounded global executor governing all MinIO uploads and downloads across
     * {@link #spillJobData(String, Path)} and {@link #restorePartition(String, int, Path)}.
     */
    private final ExecutorService s3TransferPool;

    /**
     * Initializes the MinIO Storage client using environment-injected configurations.
     * Ensures the target bucket exists upon startup, adhering to fail-fast principles.
     *
     * @throws IllegalArgumentException If {@code ESS_MAX_S3_CONCURRENCY} is not a positive integer.
     * @throws RuntimeException         If connection to the MinIO cluster fails or credentials are invalid.
     */
    public MinioStorageService() {
        String endpoint = System.getenv().getOrDefault("MINIO_ENDPOINT", "http://minio:9000");
        String accessKey = System.getenv().getOrDefault("MINIO_ACCESS_KEY", "minioadmin");
        String secretKey = System.getenv().getOrDefault("MINIO_SECRET_KEY", "minioadmin");
        this.bucketName = resolveBucketName(System.getenv());
        this.maxS3Concurrency = resolveMaxS3Concurrency(System.getenv());
        this.s3TransferPool = createTransferPool(this.maxS3Concurrency);

        try {
            this.minioClient = MinioClient.builder()
                    .endpoint(endpoint)
                    .credentials(accessKey, secretKey)
                    .build();

            ensureBucketExists();
            logger.info("MinioStorageService initialized (endpoint: {}, bucket: {}, maxS3Concurrency: {})",
                    endpoint, bucketName, maxS3Concurrency);
        } catch (Exception e) {
            s3TransferPool.shutdownNow();
            logger.error("FATAL: Failed to initialize MinIO Client for overflow tier.", e);
            throw new RuntimeException("MinIO Initialization Failure", e);
        }
    }

    /**
     * Protected constructor for unit testing with an injected client and bucket name,
     * deriving {@code maxS3Concurrency} from {@code ESS_MAX_S3_CONCURRENCY} (default {@value #DEFAULT_MAX_S3_CONCURRENCY}).
     *
     * @param minioClient Pre-configured MinIO client instance.
     * @param bucketName  Target overflow bucket name.
     */
    protected MinioStorageService(MinioClient minioClient, String bucketName) {
        this(minioClient, bucketName, resolveMaxS3Concurrency(System.getenv()));
    }

    /**
     * Constructor allowing deterministic configuration of the global S3 transfer concurrency cap
     * for unit and concurrency testing.
     *
     * @param minioClient      Pre-configured MinIO client instance.
     * @param bucketName       Target overflow bucket name.
     * @param maxS3Concurrency Maximum number of concurrent S3 uploads/downloads allowed globally (must be &ge; 1).
     * @throws IllegalArgumentException If {@code maxS3Concurrency} is less than 1.
     */
    public MinioStorageService(MinioClient minioClient, String bucketName, int maxS3Concurrency) {
        if (maxS3Concurrency < 1) {
            throw new IllegalArgumentException("maxS3Concurrency must be >= 1, got: " + maxS3Concurrency);
        }
        this.minioClient = minioClient;
        this.bucketName = bucketName;
        this.maxS3Concurrency = maxS3Concurrency;
        this.s3TransferPool = createTransferPool(maxS3Concurrency);
    }

    private static ExecutorService createTransferPool(int maxS3Concurrency) {
        AtomicInteger threadCounter = new AtomicInteger(1);
        ThreadFactory factory = runnable -> {
            Thread thread = new Thread(runnable, "ess-s3-transfer-" + threadCounter.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        };
        return Executors.newFixedThreadPool(maxS3Concurrency, factory);
    }

    /**
     * Resolves the global S3 concurrency ceiling from {@code ESS_MAX_S3_CONCURRENCY},
     * defaulting to {@value #DEFAULT_MAX_S3_CONCURRENCY}.
     *
     * @param env Environment variable map.
     * @return Positive integer concurrency ceiling.
     * @throws IllegalArgumentException If {@code ESS_MAX_S3_CONCURRENCY} is non-numeric or &lt; 1.
     */
    public static int resolveMaxS3Concurrency(Map<String, String> env) {
        String raw = env.get("ESS_MAX_S3_CONCURRENCY");
        if (raw == null || raw.isBlank()) {
            return DEFAULT_MAX_S3_CONCURRENCY;
        }
        int parsed = Integer.parseInt(raw.trim());
        if (parsed < 1) {
            throw new IllegalArgumentException("ESS_MAX_S3_CONCURRENCY must be >= 1, got: " + parsed);
        }
        return parsed;
    }

    /**
     * Resolves the MinIO overflow bucket name from environment variables, preferring
     * {@code MINIO_BUCKET}, falling back to {@code MINIO_SHUFFLE_BUCKET}, and finally
     * defaulting to {@code "ess-overflow"}.
     *
     * @param env Environment variable map.
     * @return Resolved MinIO bucket name.
     */
    public static String resolveBucketName(Map<String, String> env) {
        String bucket = env.get("MINIO_BUCKET");
        if (bucket == null || bucket.isBlank()) {
            bucket = env.get("MINIO_SHUFFLE_BUCKET");
        }
        if (bucket == null || bucket.isBlank()) {
            bucket = "ess-overflow";
        }
        return bucket;
    }

    /**
     * Returns the configured MinIO overflow bucket name.
     *
     * @return The target bucket name.
     */
    public String getBucketName() {
        return bucketName;
    }

    /**
     * Returns the configured global S3 transfer concurrency ceiling.
     *
     * @return Maximum concurrent MinIO operations permitted across this adapter.
     */
    public int getMaxS3Concurrency() {
        return maxS3Concurrency;
    }

    /**
     * Recursively collects and uploads all finalized compressed ({@code .lz4}) partition files
     * within a specific Job directory to the MinIO overflow bucket using the shared bounded
     * global S3 transfer pool.
     * <p>
     * <b>Concurrency &amp; Failure Safety:</b> All uploads execute concurrently across
     * {@link #s3TransferPool}, bounded globally by {@code ESS_MAX_S3_CONCURRENCY}. This method
     * awaits completion of all submitted upload tasks and propagates any failure as an
     * {@link IOException} so callers (e.g., {@code StorageJanitor}) never purge local data after
     * a partial spill.
     * </p>
     *
     * @param jobId        The unique identifier for the MapReduce job.
     * @param jobDirectory The root local path containing the partitioned data for the job.
     * @throws IOException If local disk traversal or any MinIO upload fails.
     */
    public void spillJobData(String jobId, Path jobDirectory) throws IOException {
        try (MDC.MDCCloseable jobMdc = MDC.putCloseable("job_id", jobId)) {
            if (!Files.exists(jobDirectory)) {
                logger.warn("Attempted to spill non-existent job directory: {}", jobDirectory);
                return;
            }

            logger.info("Initiating MinIO overflow spill for Job: {}", jobId);

            List<Path> filesToSpill = new ArrayList<>();
            Files.walkFileTree(jobDirectory, new SimpleFileVisitor<>() {
                @Override
                public @NonNull FileVisitResult visitFile(@NonNull Path file, @NonNull BasicFileAttributes attrs) {
                    if (attrs.isRegularFile() && file.getFileName().toString().endsWith(".lz4")) {
                        filesToSpill.add(file);
                    }
                    return FileVisitResult.CONTINUE;
                }
            });

            if (filesToSpill.isEmpty()) {
                logger.info("No finalized .lz4 files found to spill for Job: {}", jobId);
                return;
            }

            Path parentDir = jobDirectory.getParent();
            List<CompletableFuture<Void>> uploadTasks = new ArrayList<>(filesToSpill.size());

            try {
                for (Path file : filesToSpill) {
                    String objectName = (parentDir != null)
                            ? parentDir.relativize(file).toString().replace("\\", "/")
                            : jobId + "/" + jobDirectory.relativize(file).toString().replace("\\", "/");

                    uploadTasks.add(CompletableFuture.runAsync(() -> {
                        try (MDC.MDCCloseable taskJobMdc = MDC.putCloseable("job_id", jobId)) {
                            uploadObject(objectName, file);
                            logger.debug("Successfully spilled object: {}", objectName);
                        } catch (Exception e) {
                            logger.error("Failed to spill file {} to MinIO.", file, e);
                            throw new CompletionException("MinIO spill operation interrupted for " + objectName, e);
                        }
                    }, s3TransferPool));
                }

                CompletableFuture.allOf(uploadTasks.toArray(CompletableFuture[]::new)).join();
                logger.info("Spill complete for Job: {} ({} segment(s) uploaded)", jobId, filesToSpill.size());
            } catch (CompletionException | RejectedExecutionException e) {
                Throwable cause = (e instanceof CompletionException && e.getCause() != null) ? e.getCause() : e;
                throw new IOException("MinIO spill operation failed for job " + jobId, cause);
            }
        }
    }

    /**
     * Restores all partition segments from the MinIO overflow bucket back to the physical disk
     * using the shared bounded global S3 transfer pool.
     * <p>
     * <b>Atomic Staging Logic:</b> Downloads all segments into a temporary sibling staging
     * directory ({@code <partitionId>.restore-<uuid>.tmp}) and atomically moves it into
     * {@code targetDir} only after every segment succeeds. On failure or empty partition,
     * {@code targetDir} is never created and any temporary staging directory is deleted.
     * </p>
     *
     * @param jobId       The unique identifier for the MapReduce job.
     * @param partitionId The target partition integer.
     * @param targetDir   The local directory where the partition segments should be restored.
     * @return {@code true} if at least one segment was restored to {@code targetDir},
     *         {@code false} if MinIO genuinely contained 0 segments for the partition.
     * @throws IOException If the object listing, network/disk I/O, or atomic promotion fails.
     */
    public boolean restorePartition(String jobId, int partitionId, Path targetDir) throws IOException {
        try (MDC.MDCCloseable jobMdc = MDC.putCloseable("job_id", jobId);
             MDC.MDCCloseable partMdc = MDC.putCloseable("partition_id", String.valueOf(partitionId))) {
            // Construct the prefix: e.g., "job-123/0/"
            String objectPrefix = jobId + "/" + partitionId + "/";

            logger.info("Initiating multi-segment concurrent restoration from MinIO for Job: {}, Partition: {}", jobId, partitionId);

            Path stagingDir = null;
            try {
                // 1. List all objects matching the job/partition prefix BEFORE touching local directories
                List<String> objectsToDownload = listObjectNames(objectPrefix);
                int segmentCount = objectsToDownload.size();

                if (segmentCount == 0) {
                    logger.warn("Restore requested for Job {} Partition {}, but no objects were found in MinIO prefix: {}",
                            jobId, partitionId, objectPrefix);
                    return false;
                }

                // 2. Stage downloads in a sibling .tmp directory so targetDir is never half-populated
                Path parentDir = targetDir.getParent();
                if (parentDir != null) {
                    Files.createDirectories(parentDir);
                }
                stagingDir = (parentDir != null ? parentDir : Path.of("."))
                        .resolve(partitionId + ".restore-" + UUID.randomUUID() + ".tmp");
                Files.createDirectories(stagingDir);

                final Path resolvedStagingDir = stagingDir;
                final String partitionIdStr = String.valueOf(partitionId);

                // 3. Perform bounded concurrent downloads via the shared global S3 transfer pool
                List<CompletableFuture<Void>> downloadTasks = objectsToDownload.stream().map(objectName ->
                        CompletableFuture.runAsync(() -> {
                            try (MDC.MDCCloseable taskJobMdc = MDC.putCloseable("job_id", jobId);
                                 MDC.MDCCloseable taskPartMdc = MDC.putCloseable("partition_id", partitionIdStr)) {
                                // Extract the filename from the object path to preserve naming
                                Path stagedFile = resolvedStagingDir.resolve(Path.of(objectName).getFileName());

                                logger.debug("Downloading segment concurrently: {} -> {}", objectName, stagedFile);
                                downloadObject(objectName, stagedFile);
                            } catch (Exception e) {
                                throw new CompletionException("Failed to download segment concurrently: " + objectName, e);
                            }
                        }, s3TransferPool)
                ).toList();

                // Wait for all segments to download successfully
                CompletableFuture.allOf(downloadTasks.toArray(CompletableFuture[]::new)).join();

                // 4. Atomically publish the fully populated staging directory as targetDir
                try {
                    Files.move(stagingDir, targetDir, StandardCopyOption.ATOMIC_MOVE);
                } catch (AtomicMoveNotSupportedException e) {
                    logger.warn("ATOMIC_MOVE not supported for {} -> {}, falling back to standard move", stagingDir, targetDir);
                    Files.move(stagingDir, targetDir);
                }

                logger.info("Successfully restored {} segments concurrently for Partition {} from overflow tier.", segmentCount, partitionId);
                return true;

            } catch (Exception e) {
                logger.error("Critical failure during MinIO partition restoration: Job {} Partition {}", jobId, partitionId, e);
                if (e instanceof IOException ioe) {
                    throw ioe;
                }
                Throwable cause = (e instanceof CompletionException && e.getCause() != null) ? e.getCause() : e;
                throw new IOException("Failed to restore partition from overflow bucket", cause);
            } finally {
                if (stagingDir != null && Files.exists(stagingDir)) {
                    deleteDirectoryQuietly(stagingDir);
                }
            }
        }
    }

    /**
     * Lists non-directory object names in the overflow bucket under {@code objectPrefix}.
     *
     * @param objectPrefix Key prefix within the overflow bucket (e.g., {@code "job-123/0/"}).
     * @return List of matching object keys.
     * @throws Exception If the S3 listing request fails.
     */
    protected List<String> listObjectNames(String objectPrefix) throws Exception {
        Iterable<Result<Item>> results = minioClient.listObjects(
                ListObjectsArgs.builder()
                        .bucket(bucketName)
                        .prefix(objectPrefix)
                        .recursive(true) // Ensure we catch nested segments if they exist
                        .build()
        );

        List<String> objectNames = new ArrayList<>();
        for (Result<Item> result : results) {
            Item item = result.get();
            if (!item.isDir()) {
                objectNames.add(item.objectName());
            }
        }
        return objectNames;
    }

    /**
     * Uploads a single local segment file to MinIO under {@code objectName}.
     *
     * @param objectName Destination key in the overflow bucket.
     * @param sourceFile Local path of the segment file to upload.
     * @throws Exception If the MinIO upload fails.
     */
    protected void uploadObject(String objectName, Path sourceFile) throws Exception {
        minioClient.uploadObject(
                UploadObjectArgs.builder()
                        .bucket(bucketName)
                        .object(objectName)
                        .filename(sourceFile.toAbsolutePath().toString())
                        .build()
        );
    }

    /**
     * Downloads a single object segment from MinIO to {@code destinationFile}.
     * Uses internal stream-chunking to maintain O(1) memory overhead.
     *
     * @param objectName      Source key in the overflow bucket.
     * @param destinationFile Local path where the segment will be written.
     * @throws Exception If the MinIO download or file write fails.
     */
    protected void downloadObject(String objectName, Path destinationFile) throws Exception {
        minioClient.downloadObject(
                DownloadObjectArgs.builder()
                        .bucket(bucketName)
                        .object(objectName)
                        .filename(destinationFile.toAbsolutePath().toString())
                        .build()
        );
    }

    /**
     * Gracefully shuts down the shared global S3 transfer pool, awaiting completion of in-flight
     * transfers for up to {@value #SHUTDOWN_TIMEOUT_SECONDS} seconds before forcing termination.
     */
    @Override
    public void close() {
        if (s3TransferPool.isShutdown()) {
            return;
        }
        logger.info("Shutting down MinioStorageService global S3 transfer pool...");
        s3TransferPool.shutdown();
        try {
            if (!s3TransferPool.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                logger.warn("S3 transfer pool did not terminate within {}s; forcing shutdownNow().",
                        SHUTDOWN_TIMEOUT_SECONDS);
                s3TransferPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Interrupted while closing MinioStorageService transfer pool; forcing shutdownNow().");
            s3TransferPool.shutdownNow();
        }
    }

    private static void deleteDirectoryQuietly(Path dir) {
        try {
            Files.walkFileTree(dir, new SimpleFileVisitor<>() {
                @Override
                public @NonNull FileVisitResult visitFile(@NonNull Path file, @NonNull BasicFileAttributes attrs) throws IOException {
                    Files.deleteIfExists(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public @NonNull FileVisitResult postVisitDirectory(@NonNull Path d, IOException exc) throws IOException {
                    Files.deleteIfExists(d);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException cleanupEx) {
            logger.warn("Failed to clean up temporary restore staging directory {}: {}", dir, cleanupEx.getMessage());
        }
    }

    /**
     * Idempotent check to guarantee the target overflow bucket exists on the MinIO cluster.
     * Creates the bucket if it is missing.
     *
     * @throws Exception If bucket verification or creation fails.
     */
    private void ensureBucketExists() throws Exception {
        boolean found = minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
        if (!found) {
            minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
            logger.info("Created new MinIO overflow bucket: {}", bucketName);
        }
    }
}
