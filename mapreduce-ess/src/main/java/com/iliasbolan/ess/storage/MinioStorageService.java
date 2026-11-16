package com.iliasbolan.ess.storage;

import io.minio.*;
import io.minio.messages.Item;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.concurrent.Executors;

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
 * <b>Performance Architecture:</b><br>
 * Utilizes direct physical-to-network streaming for O(1) memory OOM protection during massive
 * spills, and employs parallel concurrent streams during restoration to maximize bandwidth
 * saturation and minimize Reducer wait times.
 * </p>
 * <p>
 * <b>Architecture Update (ForkJoinPool Starvation Fix):</b><br>
 * Migrated concurrent S3 restorations from <code>parallelStream()</code> to explicit
 * Virtual Thread orchestration. Because MinIO downloads are strictly blocking I/O operations,
 * using the global ForkJoinPool previously risked starving the entire JVM of compute threads
 * during massive cache misses. Virtual threads safely unmount during I/O wait times, maintaining
 * flawless node stability.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 2.1
 * @since 2026-05-13
 */
public class MinioStorageService {

    private static final Logger logger = LoggerFactory.getLogger(MinioStorageService.class);

    private final MinioClient minioClient;
    private final String bucketName;

    /**
     * Initializes the MinIO Storage client using environment-injected configurations.
     * Ensures the target bucket exists upon startup, adhering to fail-fast principles.
     *
     * @throws RuntimeException If connection to the MinIO cluster fails or credentials are invalid.
     */
    public MinioStorageService() {
        String endpoint = System.getenv().getOrDefault("MINIO_ENDPOINT", "http://minio:9000");
        String accessKey = System.getenv().getOrDefault("MINIO_ACCESS_KEY", "minioadmin");
        String secretKey = System.getenv().getOrDefault("MINIO_SECRET_KEY", "minioadmin");
        this.bucketName = resolveBucketName(System.getenv());

        try {
            this.minioClient = MinioClient.builder()
                    .endpoint(endpoint)
                    .credentials(accessKey, secretKey)
                    .build();

            ensureBucketExists();
            logger.info("MinioStorageService initialized successfully connected to endpoint: {} (bucket: {})", endpoint, bucketName);
        } catch (Exception e) {
            logger.error("FATAL: Failed to initialize MinIO Client for overflow tier.", e);
            throw new RuntimeException("MinIO Initialization Failure", e);
        }
    }

    /**
     * Package-private constructor for unit testing with an injected client and bucket name.
     */
    protected MinioStorageService(MinioClient minioClient, String bucketName) {
        this.minioClient = minioClient;
        this.bucketName = bucketName;
    }

    /**
     * Resolves the MinIO overflow bucket name from environment variables, preferring
     * {@code MINIO_BUCKET}, falling back to {@code MINIO_SHUFFLE_BUCKET}, and finally
     * defaulting to {@code "ess-overflow"}.
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

    public String getBucketName() {
        return bucketName;
    }

    /**
     * Recursively uploads all compressed (.lz4) partition files within a specific Job directory
     * to the MinIO overflow bucket.
     * <p>
     * This operation streams the files directly from the physical disk to the network interface,
     * maintaining a near O(1) memory footprint regardless of directory size.
     * </p>
     *
     * @param jobId        The unique identifier for the MapReduce job.
     * @param jobDirectory The root local path containing the partitioned data for the job.
     * @throws IOException If local disk traversal or network transmission fails.
     */
    public void spillJobData(String jobId, Path jobDirectory) throws IOException {
        if (!Files.exists(jobDirectory)) {
            logger.warn("Attempted to spill non-existent job directory: {}", jobDirectory);
            return;
        }

        logger.info("Initiating MinIO overflow spill for Job: {}", jobId);

        Files.walkFileTree(jobDirectory, new SimpleFileVisitor<>() {
            @Override
            public @NonNull FileVisitResult visitFile(@NonNull Path file, @NonNull BasicFileAttributes attrs) throws IOException {
                if (file.toString().endsWith(".lz4")) {
                    // Calculate relative path to maintain partition structure in the bucket (e.g., job-123/0/part.lz4)
                    String objectName = jobDirectory.getParent().relativize(file).toString().replace("\\", "/");

                    try {
                        minioClient.uploadObject(
                                UploadObjectArgs.builder()
                                        .bucket(bucketName)
                                        .object(objectName)
                                        .filename(file.toAbsolutePath().toString())
                                        .build()
                        );
                        logger.debug("Successfully spilled object: {}", objectName);
                    } catch (Exception e) {
                        logger.error("Failed to spill file {} to MinIO.", file, e);
                        throw new IOException("MinIO spill operation interrupted", e);
                    }
                }
                return FileVisitResult.CONTINUE;
            }
        });

        logger.info("Spill complete for Job: {}", jobId);
    }

    /**
     * Restores all partition segments from the MinIO overflow bucket back to the physical disk.
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

            // 3. Perform highly concurrent downloads via Virtual Threads
            // Replaced parallelStream() to prevent starvation of the global ForkJoinPool during blocking I/O.
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                List<CompletableFuture<Void>> downloadTasks = objectsToDownload.stream().map(objectName ->
                        CompletableFuture.runAsync(() -> {
                            try {
                                // Extract the filename from the object path to preserve naming
                                Path stagedFile = resolvedStagingDir.resolve(Path.of(objectName).getFileName());

                                logger.debug("Downloading segment concurrently: {} -> {}", objectName, stagedFile);
                                downloadObject(objectName, stagedFile);
                            } catch (Exception e) {
                                throw new RuntimeException("Failed to download segment concurrently: " + objectName, e);
                            }
                        }, executor)
                ).toList();

                // Wait for all segments to download successfully
                CompletableFuture.allOf(downloadTasks.toArray(new CompletableFuture[0])).join();
            }

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
            throw new IOException("Failed to restore partition from overflow bucket", e);
        } finally {
            if (stagingDir != null && Files.exists(stagingDir)) {
                deleteDirectoryQuietly(stagingDir);
            }
        }
    }

    /**
     * Lists non-directory object names in the overflow bucket under {@code objectPrefix}.
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
     * Downloads a single object segment from MinIO to {@code destinationFile}.
     * Uses internal stream-chunking to maintain O(1) memory overhead.
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