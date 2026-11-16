package com.iliasbolan.ess.gc;

import com.iliasbolan.ess.storage.MinioStorageService;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

/**
 * Intelligent background Garbage Collector (Janitor) for the External Shuffle Service (ESS).
 * <p>
 * This service implements a "Space-Aware" reclamation strategy designed for high-throughput
 * MapReduce environments where disk exhaustion on physical nodes is a critical failure vector.
 * Unlike standard time-based TTL (Time-To-Live) mechanisms, this Janitor monitors real-time
 * disk utilization of the underlying {@link FileStore}.
 * </p>
 * <p>
 * <b>High-Resolution Monitoring (LZ4 Optimization):</b><br>
 * Polls on a 30-second interval. In environments processing large payloads, disk usage can
 * escalate rapidly. This resolution ensures that the reclamation watermarks are enforced
 * reactively to prevent node-level outages.
 * </p>
 * <p>
 * <b>Architecture Update (Active Task &amp; Lease Protection):</b><br>
 * Prevents the garbage collector from corrupting active Map writers or active Reducer streams.
 * Before initiating a MinIO spill and local deletion, the Janitor checks for active
 * {@code .tmp} artifacts and active {@code job_id} reader/restore leases
 * ({@link #acquireJobLease(String)} / {@link #releaseJobLease(String)}), re-verifying under a
 * {@link ReentrantReadWriteLock} before local deletion.
 * </p>
 * <p>
 * <b>Graceful Shutdown (ADR-14):</b><br>
 * Calling {@link #stop()} sets {@link #running} to {@code false}, calls
 * {@code gcExecutor.shutdown()}, and awaits any active spill/GC cycle up to
 * {@value #DEFAULT_STOP_TIMEOUT_SECONDS} seconds before escalating to {@code shutdownNow()}.
 * This prevents interrupting in-flight MinIO {@code spillJobData} uploads or directory deletions
 * during {@code SIGTERM}.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 3.0
 * @since 2026-05-13
 */
public class StorageJanitor {

    private static final Logger logger = LoggerFactory.getLogger(StorageJanitor.class);

    /**
     * Dedicated single-thread scheduler to prevent Janitor tasks from interfering
     * with gRPC shuffle request handling.
     */
    private static ScheduledExecutorService gcExecutor;

    /** Flag indicating whether the background scheduler is permitted to continue new cycles/jobs. */
    private static volatile boolean running = false;

    /** Tracks whether the scheduled Janitor loop is currently active (distinguishes scheduled runs from direct unit calls). */
    private static volatile boolean started = false;

    /** Default graceful drain window (in seconds) for {@link #stop()}. */
    public static final long DEFAULT_STOP_TIMEOUT_SECONDS = 15L;

    /**
     * Threshold for initiating aggressive space reclamation (85% utilization).
     */
    private static final double HIGH_WATERMARK_PERCENT = 0.85;

    /**
     * Target threshold for halting space reclamation (70% utilization).
     */
    private static final double LOW_WATERMARK_PERCENT = 0.70;

    /**
     * Absolute maximum age for any job directory before it is forcibly purged (24 hours).
     */
    private static final long HARD_TTL_HOURS = 24;

    /**
     * The MinIO storage service used for spilling data before physical deletion.
     */
    private static MinioStorageService minioStorageService;

    /**
     * Per-job lease and read/write lock registry protecting active MinIO restorations
     * and active Reducer gRPC streams from premature garbage collection.
     */
    private static final ConcurrentHashMap<String, JobLeaseEntry> activeJobLeases = new ConcurrentHashMap<>();

    private static final class JobLeaseEntry {
        final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
        int refCount = 0;
        final AtomicInteger activeLeases = new AtomicInteger(0);
    }

    /**
     * Acquires an active reader/restore lease for {@code jobId}. Must be paired with
     * {@link #releaseJobLease(String)} in a {@code finally} block.
     *
     * @param jobId Sanitized MapReduce job identifier.
     */
    public static void acquireJobLease(String jobId) {
        if (jobId == null || jobId.isEmpty()) {
            return;
        }
        JobLeaseEntry entry = activeJobLeases.compute(jobId, (k, existing) -> {
            JobLeaseEntry e = (existing != null) ? existing : new JobLeaseEntry();
            e.refCount++;
            e.activeLeases.incrementAndGet();
            return e;
        });
        entry.rwLock.readLock().lock();
    }

    /**
     * Releases an active reader/restore lease for {@code jobId}.
     *
     * @param jobId Sanitized MapReduce job identifier.
     */
    public static void releaseJobLease(String jobId) {
        if (jobId == null || jobId.isEmpty()) {
            return;
        }
        JobLeaseEntry entry = activeJobLeases.get(jobId);
        if (entry != null) {
            entry.rwLock.readLock().unlock();
        }
        activeJobLeases.computeIfPresent(jobId, (k, existing) -> {
            if (existing.activeLeases.get() > 0) {
                existing.activeLeases.decrementAndGet();
            }
            if (--existing.refCount <= 0) {
                return null;
            }
            return existing;
        });
    }

    /**
     * Returns the current number of active restore/stream leases for {@code jobId}.
     *
     * @param jobId MapReduce job identifier.
     * @return Number of active leases currently held for the job.
     */
    public static int getActiveLeaseCount(String jobId) {
        if (jobId == null || jobId.isEmpty()) {
            return 0;
        }
        JobLeaseEntry entry = activeJobLeases.get(jobId);
        return entry != null ? entry.activeLeases.get() : 0;
    }

    /**
     * Returns {@code true} if {@code jobId} currently holds one or more active leases.
     *
     * @param jobId MapReduce job identifier.
     * @return {@code true} if at least one lease is active.
     */
    public static boolean hasActiveLease(String jobId) {
        return getActiveLeaseCount(jobId) > 0;
    }

    /**
     * Initializes and schedules the background Janitor thread to monitor storage health.
     * <p>
     * Uses a 30-second scan interval to provide reactive space reclamation for large-scale
     * shuffle payloads. Defaults to a 10-second initial delay.
     * </p>
     *
     * @param baseShuffleDir The root physical directory containing per-job partition data.
     * @param storageService The MinIO storage client to handle data offloading (may be {@code null}).
     */
    public static void start(String baseShuffleDir, MinioStorageService storageService) {
        start(baseShuffleDir, storageService, 10, 30, TimeUnit.SECONDS);
    }

    /**
     * Initializes and schedules the background Janitor thread with explicit cadence settings.
     * Package-private/public helper enabling deterministic scheduling in unit tests.
     *
     * @param baseShuffleDir The root physical directory containing per-job partition data.
     * @param storageService The MinIO storage client to handle data offloading (may be {@code null}).
     * @param initialDelay   Delay before the first scan executes.
     * @param period         Interval between successive scans.
     * @param unit           Time unit for {@code initialDelay} and {@code period}.
     */
    public static void start(String baseShuffleDir, MinioStorageService storageService,
                             long initialDelay, long period, TimeUnit unit) {
        start(baseShuffleDir, storageService, initialDelay, period, unit, null);
    }

    /**
     * Package-private overload allowing unit tests to inject a deterministic {@link FileStore}
     * for verifying scheduled watermark reclamation and graceful {@link #stop()} draining.
     *
     * @param baseShuffleDir The root physical directory containing per-job partition data.
     * @param storageService The MinIO storage client to handle data offloading (may be {@code null}).
     * @param initialDelay   Delay before the first scan executes.
     * @param period         Interval between successive scans.
     * @param unit           Time unit for {@code initialDelay} and {@code period}.
     * @param fileStoreOverride Optional {@link FileStore} override for testing (when {@code null}, uses {@link Files#getFileStore(Path)}).
     */
    public static void start(String baseShuffleDir, MinioStorageService storageService,
                             long initialDelay, long period, TimeUnit unit, FileStore fileStoreOverride) {
        if (gcExecutor != null && !gcExecutor.isShutdown()) {
            stop();
        }
        minioStorageService = storageService;
        running = true;
        started = true;
        gcExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "ess-storage-janitor");
            thread.setDaemon(false);
            return thread;
        });

        gcExecutor.scheduleAtFixedRate(() -> {
            if (!running || Thread.currentThread().isInterrupted()) {
                return;
            }
            Path rootPath = Paths.get(baseShuffleDir);
            if (!Files.exists(rootPath)) {
                return;
            }

            try {
                // Accessing NIO FileStore to retrieve physical disk metrics
                FileStore store = (fileStoreOverride != null) ? fileStoreOverride : Files.getFileStore(rootPath);
                long totalSpace = store.getTotalSpace();
                long usableSpace = store.getUsableSpace();
                long usedSpace = totalSpace - usableSpace;

                double usedPercentage = (double) usedSpace / totalSpace;

                logger.debug("Storage Janitor Scan: Disk Utilization at {}", String.format("%.1f%%", usedPercentage * 100));

                // Trigger aggressive reclamation if High-Watermark is reached
                if (usedPercentage >= HIGH_WATERMARK_PERCENT) {
                    logger.warn("HIGH WATERMARK BREACHED ({}%). Running reactive space reclamation...", Math.round(usedPercentage * 100));
                    reclaimSpace(rootPath, store, totalSpace);
                }

                // Execute the fallback TTL sweep to clean up zombie job data
                if (running && !Thread.currentThread().isInterrupted()) {
                    purgeHardTtl(rootPath);
                }

            } catch (IOException e) {
                logger.error("Janitor Service failure: {}", e.getMessage(), e);
            }
        }, initialDelay, period, unit);
    }

    /**
     * Gracefully terminates the background Janitor service, awaiting completion of any
     * in-flight spill or GC cycle for up to {@value #DEFAULT_STOP_TIMEOUT_SECONDS} seconds
     * before escalating to {@code shutdownNow()}.
     */
    public static void stop() {
        stop(DEFAULT_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Gracefully terminates the background Janitor service within the given timeout.
     * Sets {@link #running} to {@code false}, initiates {@code gcExecutor.shutdown()},
     * awaits completion of the active GC/spill cycle, and escalates to {@code shutdownNow()}
     * only if the cycle does not finish in time.
     *
     * @param timeout Maximum duration to await completion of an active GC cycle.
     * @param unit    Time unit of {@code timeout}.
     * @return {@code true} if the Janitor terminated cleanly within {@code timeout};
     *         {@code false} if forced termination via {@code shutdownNow()} was required.
     */
    public static boolean stop(long timeout, TimeUnit unit) {
        running = false;
        ScheduledExecutorService executor = gcExecutor;
        if (executor == null || executor.isShutdown()) {
            started = false;
            return true;
        }

        logger.info("Stopping StorageJanitor gracefully (timeout: {} {})...", timeout, unit);
        executor.shutdown();
        try {
            if (!executor.awaitTermination(timeout, unit)) {
                logger.warn("StorageJanitor active cycle did not complete within {} {}; forcing shutdownNow().",
                        timeout, unit);
                executor.shutdownNow();
                return false;
            }
            logger.info("StorageJanitor stopped cleanly.");
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Interrupted while awaiting StorageJanitor shutdown; forcing shutdownNow().");
            executor.shutdownNow();
            return false;
        } finally {
            started = false;
        }
    }

    /**
     * Returns whether the background Janitor scheduler is currently marked as running.
     *
     * @return {@code true} if running and not yet signaled to stop.
     */
    public static boolean isRunning() {
        return running;
    }

    /**
     * Temporary data structure for O(N) timestamp-based sorting.
     */
    private record JobDirectory(Path path, long creationTime) {
    }

    /**
     * Identifies and purges the oldest job directories until disk usage is restored
     * to the Low-Watermark buffer. Integrates with the MinIO overflow tier to
     * persist data prior to local eviction.
     *
     * @param rootPath   The directory to scan for jobs.
     * @param store      The FileStore being managed.
     * @param totalSpace Total capacity of the store for percentage calculations.
     * @throws IOException If the directory listing or attribute access fails.
     */
    private static void reclaimSpace(Path rootPath, FileStore store, long totalSpace) throws IOException {
        reclaimSpace(rootPath, store, totalSpace, minioStorageService);
    }

    /**
     * Identifies and purges the oldest job directories until disk usage is restored
     * to the Low-Watermark buffer. Skips any directory with active restore/stream
     * leases or in-flight {@code .tmp} files/directories.
     *
     * @param rootPath       The root shuffle directory containing per-job directories.
     * @param store          The {@link FileStore} providing usable space metrics.
     * @param totalSpace     Total capacity of the store in bytes.
     * @param storageService Overflow adapter for spilling data before deletion (may be {@code null}).
     * @throws IOException If directory listing or local eviction fails.
     */
    public static void reclaimSpace(Path rootPath, FileStore store, long totalSpace,
                                    MinioStorageService storageService) throws IOException {
        List<Path> sortedJobs;

        // Collect and sort all subdirectories by pre-fetched creation time (Oldest First)
        try (Stream<Path> stream = Files.list(rootPath)) {
            sortedJobs = stream.filter(Files::isDirectory)
                    .map(p -> {
                        try {
                            long time = Files.readAttributes(p, BasicFileAttributes.class).creationTime().toMillis();
                            return new JobDirectory(p, time);
                        } catch (IOException e) {
                            return new JobDirectory(p, Long.MAX_VALUE);
                        }
                    })
                    .sorted(Comparator.comparingLong(jd -> jd.creationTime))
                    .map(jd -> jd.path)
                    .toList();
        }

        for (Path jobDir : sortedJobs) {
            if ((started && !running) || Thread.currentThread().isInterrupted()) {
                logger.info("StorageJanitor shutdown requested; halting reclamation before next job.");
                break;
            }

            long usableSpace = store.getUsableSpace();
            long usedSpace = totalSpace - usableSpace;
            double currentUsedPercent = (double) usedSpace / totalSpace;

            // Stop once we are back within the safe operating range
            if (currentUsedPercent <= LOW_WATERMARK_PERCENT) {
                logger.info("Low-Watermark reached ({}%). Halting aggressive reclamation.", Math.round(currentUsedPercent * 100));
                break;
            }

            String jobId = jobDir.getFileName().toString();
            try (MDC.MDCCloseable jobMdc = MDC.putCloseable("job_id", jobId)) {
                // Active Lease Protection: Skip jobs currently being restored from MinIO or streamed to Reducers
                if (hasActiveLease(jobId)) {
                    logger.debug("Skipping job directory {} due to active restore/stream lease.", jobId);
                    continue;
                }

                // ARCHITECTURAL FIX: Active Task Protection
                // Deeply inspects the job directory to prevent the Janitor from wiping out
                // a job that has active .tmp writers or .tmp restore staging directories.
                if (hasActiveTmpEntries(jobDir)) {
                    logger.debug("Skipping job directory {} due to active .tmp entries.", jobId);
                    continue;
                }

                logger.info("Reclaiming Space: Spilling job directory to MinIO -> {}", jobId);

                try {
                    // Spill the data to MinIO before deleting it from local disk
                    if (storageService != null) {
                        storageService.spillJobData(jobId, jobDir);
                    } else {
                        logger.warn("MinioStorageService is not initialized. Proceeding with hard deletion!");
                    }

                    // Re-verify active leases & acquire write lock atomically before local deletion
                    deleteJobDirIfIdle(jobId, jobDir);
                } catch (IOException e) {
                    logger.error("Failed to spill job directory {}. Skipping local deletion to prevent data loss.", jobId, e);
                    // By failing safely and skipping the local deletion, we ensure data reliability
                    // over disk space in the event of an object storage outage.
                }
            }
        }
    }

    private static boolean hasActiveTmpEntries(Path jobDir) {
        try (Stream<Path> jobFiles = Files.walk(jobDir)) {
            return jobFiles.anyMatch(p -> p.toString().endsWith(".tmp"));
        } catch (IOException e) {
            logger.warn("Failed to check for active .tmp entries in {}. Skipping to prevent data loss.", jobDir.getFileName());
            return true;
        }
    }

    private static void deleteJobDirIfIdle(String jobId, Path jobDir) throws IOException {
        try (MDC.MDCCloseable jobMdc = MDC.putCloseable("job_id", jobId)) {
            JobLeaseEntry entry = activeJobLeases.compute(jobId, (k, existing) -> {
                JobLeaseEntry e = (existing != null) ? existing : new JobLeaseEntry();
                e.refCount++;
                return e;
            });

            try {
                if (entry.activeLeases.get() > 0 || !entry.rwLock.writeLock().tryLock()) {
                    logger.debug("Skipping local deletion of {} because a restore/stream lease became active during spill.", jobId);
                    return;
                }
                try {
                    if (entry.activeLeases.get() > 0 || hasActiveTmpEntries(jobDir)) {
                        logger.debug("Skipping local deletion of {} because an active lease or .tmp entry appeared during spill.", jobId);
                        return;
                    }
                    logger.info("Spill complete. Purging local directory -> {}", jobId);
                    deleteDirectoryRecursively(jobDir);
                } finally {
                    entry.rwLock.writeLock().unlock();
                }
            } finally {
                activeJobLeases.computeIfPresent(jobId, (k, existing) -> {
                    if (--existing.refCount <= 0) {
                        return null;
                    }
                    return existing;
                });
            }
        }
    }

    /**
     * Executes a hard TTL sweep across the shuffle directory to purge stale metadata.
     * Note: TTL sweeps skip MinIO spilling as the job data is considered fundamentally dead.
     *
     * @param rootPath The root directory containing job data.
     * @throws IOException If file attribute reading fails.
     */
    private static void purgeHardTtl(Path rootPath) throws IOException {
        long cutoff = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(HARD_TTL_HOURS);

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(rootPath)) {
            for (Path jobDir : stream) {
                if ((started && !running) || Thread.currentThread().isInterrupted()) {
                    logger.info("StorageJanitor shutdown requested; halting TTL sweep.");
                    break;
                }
                if (Files.isDirectory(jobDir)) {
                    String jobId = jobDir.getFileName().toString();
                    try (MDC.MDCCloseable jobMdc = MDC.putCloseable("job_id", jobId)) {
                        if (hasActiveLease(jobId) || hasActiveTmpEntries(jobDir)) {
                            continue;
                        }
                        BasicFileAttributes attrs = Files.readAttributes(jobDir, BasicFileAttributes.class);
                        if (attrs.creationTime().toMillis() < cutoff) {
                            logger.info("Hard TTL Expired for Job: {}. Purging data...", jobId);
                            deleteJobDirIfIdle(jobId, jobDir);
                        }
                    }
                }
            }
        }
    }

    /**
     * Performs a high-performance recursive deletion of a directory and all nested contents.
     *
     * @param root The directory path to delete.
     * @throws IOException If any file deletion or directory traversal operation fails.
     */
    private static void deleteDirectoryRecursively(Path root) throws IOException {
        if (!Files.exists(root)) return;

        Files.walkFileTree(root, new SimpleFileVisitor<>() {
            @Override @NonNull
            public FileVisitResult visitFile(@NonNull Path file, @NonNull BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override @NonNull
            public FileVisitResult postVisitDirectory(@NonNull Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
