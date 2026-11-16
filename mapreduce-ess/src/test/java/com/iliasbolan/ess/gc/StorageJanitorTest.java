package com.iliasbolan.ess.gc;

import com.iliasbolan.ess.storage.MinioStorageService;
import io.minio.MinioClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class StorageJanitorTest {

    private static final MinioClient DUMMY_CLIENT = MinioClient.builder()
            .endpoint("http://127.0.0.1:9000")
            .credentials("minioadmin", "minioadmin")
            .build();

    @Test
    void reclaimSpaceSkipsActiveLeaseAndTmpStagingDirectories(@TempDir Path rootDir) throws Exception {
        Path leasedJob = Files.createDirectories(rootDir.resolve("job-leased/0"));
        Files.writeString(leasedJob.resolve("map-0.lz4"), "data");

        Path restoringJob = Files.createDirectories(rootDir.resolve("job-restoring/0.restore-abc.tmp"));
        Files.writeString(restoringJob.resolve("map-0.lz4"), "staging-data");

        Path idleJob = Files.createDirectories(rootDir.resolve("job-idle/0"));
        Files.writeString(idleJob.resolve("map-0.lz4"), "evictable-data");

        List<String> spilledJobs = new CopyOnWriteArrayList<>();
        MinioStorageService recordingStorage = new MinioStorageService(DUMMY_CLIENT, "test-bucket") {
            @Override
            public void spillJobData(String jobId, Path jobDirectory) {
                spilledJobs.add(jobId);
            }
        };

        FileStore highPressureStore = new FakeFileStore(1000L, 100L); // 90% used >= 85% high watermark

        StorageJanitor.acquireJobLease("job-leased");
        try {
            StorageJanitor.reclaimSpace(rootDir, highPressureStore, 1000L, recordingStorage);

            assertTrue(Files.exists(rootDir.resolve("job-leased")), "Leased job must not be evicted");
            assertTrue(Files.exists(rootDir.resolve("job-restoring")), "Job with .tmp staging dir must not be evicted");
            assertFalse(Files.exists(rootDir.resolve("job-idle")), "Idle job should be evicted");
            assertEquals(List.of("job-idle"), spilledJobs);
        } finally {
            StorageJanitor.releaseJobLease("job-leased");
        }

        // Once lease is released, job-leased becomes eligible for eviction
        spilledJobs.clear();
        StorageJanitor.reclaimSpace(rootDir, highPressureStore, 1000L, recordingStorage);
        assertFalse(Files.exists(rootDir.resolve("job-leased")), "Released job should now be evicted");
        assertEquals(List.of("job-leased"), spilledJobs);
    }

    @Test
    void reclaimSpaceAbortsDeletionIfLeaseAcquiredDuringSpill(@TempDir Path rootDir) throws Exception {
        String jobId = "job-race-during-spill";
        Path jobPartition = Files.createDirectories(rootDir.resolve(jobId + "/0"));
        Files.writeString(jobPartition.resolve("map-0.lz4"), "important-data");

        CountDownLatch spillStarted = new CountDownLatch(1);
        CountDownLatch leaseAcquired = new CountDownLatch(1);

        MinioStorageService slowSpillStorage = new MinioStorageService(DUMMY_CLIENT, "test-bucket") {
            @Override
            public void spillJobData(String id, Path jobDirectory) throws IOException {
                spillStarted.countDown();
                try {
                    assertTrue(leaseAcquired.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted", e);
                }
            }
        };

        FileStore highPressureStore = new FakeFileStore(1000L, 100L);

        try (ExecutorService exec = Executors.newSingleThreadExecutor()) {
            Future<?> janitorFuture = exec.submit(() -> {
                try {
                    StorageJanitor.reclaimSpace(rootDir, highPressureStore, 1000L, slowSpillStorage);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            assertTrue(spillStarted.await(5, TimeUnit.SECONDS));
            StorageJanitor.acquireJobLease(jobId);
            leaseAcquired.countDown();

            try {
                janitorFuture.get(5, TimeUnit.SECONDS);
                assertTrue(Files.exists(rootDir.resolve(jobId)),
                        "Directory must not be deleted if a reader/restore lease became active during spill");
            } finally {
                StorageJanitor.releaseJobLease(jobId);
            }
        }
    }

    @Test
    void stopAwaitsInFlightSpillWithoutInterruptingAndHaltsBeforeNextJob(@TempDir Path rootDir) throws Exception {
        Path job1 = Files.createDirectories(rootDir.resolve("job-1/0"));
        Files.writeString(job1.resolve("map-0.lz4"), "data-1");
        Files.setAttribute(rootDir.resolve("job-1"), "creationTime", FileTime.from(Instant.now().minusSeconds(120)));

        Path job2 = Files.createDirectories(rootDir.resolve("job-2/0"));
        Files.writeString(job2.resolve("map-0.lz4"), "data-2");
        Files.setAttribute(rootDir.resolve("job-2"), "creationTime", FileTime.from(Instant.now().minusSeconds(10)));

        CountDownLatch spillStarted = new CountDownLatch(1);
        CountDownLatch allowSpillFinish = new CountDownLatch(1);
        AtomicBoolean spillWasInterrupted = new AtomicBoolean(false);
        List<String> spilledJobs = new CopyOnWriteArrayList<>();

        MinioStorageService slowSpillStorage = new MinioStorageService(DUMMY_CLIENT, "test-bucket", 2) {
            @Override
            public void spillJobData(String jobId, Path jobDirectory) throws IOException {
                spilledJobs.add(jobId);
                spillStarted.countDown();
                try {
                    assertTrue(allowSpillFinish.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    spillWasInterrupted.set(true);
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted during spill", e);
                }
            }
        };

        FileStore highPressureStore = new FakeFileStore(1000L, 100L);
        StorageJanitor.start(rootDir.toString(), slowSpillStorage, 10, 50, TimeUnit.MILLISECONDS, highPressureStore);
        try (ExecutorService exec = Executors.newSingleThreadExecutor()) {
            assertTrue(spillStarted.await(5, TimeUnit.SECONDS));

            Future<Boolean> stopFuture = exec.submit(() -> StorageJanitor.stop(5, TimeUnit.SECONDS));
            Thread.sleep(100);

            assertFalse(StorageJanitor.isRunning(), "running flag must flip false immediately on stop()");
            assertFalse(spillWasInterrupted.get(), "In-flight spillJobData must NOT be interrupted during graceful stop");
            assertFalse(stopFuture.isDone(), "stop() must await completion of in-flight spill");

            allowSpillFinish.countDown();
            assertTrue(stopFuture.get(5, TimeUnit.SECONDS), "stop() should return true after clean cycle completion");
            assertFalse(spillWasInterrupted.get());
            assertEquals(List.of("job-1"), spilledJobs, "Janitor must finish job-1 and stop before starting job-2");
            assertFalse(Files.exists(rootDir.resolve("job-1")), "Completed spill job-1 should be deleted locally");
            assertTrue(Files.exists(rootDir.resolve("job-2")), "job-2 must remain untouched after stop signal");
        } finally {
            StorageJanitor.stop(1, TimeUnit.SECONDS);
            slowSpillStorage.close();
        }
    }

    @Test
    void stopEscalatesToShutdownNowWhenSpillExceedsTimeout(@TempDir Path rootDir) throws Exception {
        Path jobStuck = Files.createDirectories(rootDir.resolve("job-stuck/0"));
        Files.writeString(jobStuck.resolve("map-0.lz4"), "stuck-data");

        CountDownLatch spillStarted = new CountDownLatch(1);
        CountDownLatch interruptedObserved = new CountDownLatch(1);

        MinioStorageService hungSpillStorage = new MinioStorageService(DUMMY_CLIENT, "test-bucket", 1) {
            @Override
            public void spillJobData(String jobId, Path jobDirectory) throws IOException {
                spillStarted.countDown();
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    interruptedObserved.countDown();
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted by shutdownNow", e);
                }
            }
        };

        FileStore highPressureStore = new FakeFileStore(1000L, 100L);
        StorageJanitor.start(rootDir.toString(), hungSpillStorage, 10, 50, TimeUnit.MILLISECONDS, highPressureStore);
        try {
            assertTrue(spillStarted.await(5, TimeUnit.SECONDS));
            boolean stoppedCleanly = StorageJanitor.stop(80, TimeUnit.MILLISECONDS);
            assertFalse(stoppedCleanly, "stop() must return false when escalating to shutdownNow()");
            assertTrue(interruptedObserved.await(5, TimeUnit.SECONDS), "shutdownNow() must interrupt stuck worker");
            assertTrue(Files.exists(rootDir.resolve("job-stuck")), "Stuck/failed spill must never delete local data");
        } finally {
            StorageJanitor.stop(1, TimeUnit.SECONDS);
            hungSpillStorage.close();
        }
    }

    private static final class FakeFileStore extends FileStore {
        private final long totalSpace;
        private final long usableSpace;

        FakeFileStore(long totalSpace, long usableSpace) {
            this.totalSpace = totalSpace;
            this.usableSpace = usableSpace;
        }

        @Override public String name() { return "fake"; }
        @Override public String type() { return "fakefs"; }
        @Override public boolean isReadOnly() { return false; }
        @Override public long getTotalSpace() { return totalSpace; }
        @Override public long getUsableSpace() { return usableSpace; }
        @Override public long getUnallocatedSpace() { return usableSpace; }
        @Override public boolean supportsFileAttributeView(Class<? extends FileAttributeView> type) { return false; }
        @Override public boolean supportsFileAttributeView(String name) { return false; }
        @Override public <V extends FileStoreAttributeView> V getFileStoreAttributeView(Class<V> type) { return null; }
        @Override public Object getAttribute(String attribute) { return null; }
    }
}
