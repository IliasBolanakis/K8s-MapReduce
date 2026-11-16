package com.iliasbolan.ess.storage;

import io.minio.MinioClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class MinioStorageServiceTest {

    private static final MinioClient DUMMY_CLIENT = MinioClient.builder()
            .endpoint("http://127.0.0.1:9000")
            .credentials("minioadmin", "minioadmin")
            .build();

    @Test
    void resolveBucketNamePrefersMinioBucketThenShuffleBucketThenDefault() {
        assertEquals("primary-bucket", MinioStorageService.resolveBucketName(
                Map.of("MINIO_BUCKET", "primary-bucket", "MINIO_SHUFFLE_BUCKET", "secondary-bucket")));
        assertEquals("secondary-bucket", MinioStorageService.resolveBucketName(
                Map.of("MINIO_SHUFFLE_BUCKET", "secondary-bucket")));
        assertEquals("secondary-bucket", MinioStorageService.resolveBucketName(
                Map.of("MINIO_BUCKET", "   ", "MINIO_SHUFFLE_BUCKET", "secondary-bucket")));
        assertEquals("ess-overflow", MinioStorageService.resolveBucketName(Map.of()));
    }

    @Test
    void restorePartitionReturnsFalseWithoutCreatingTargetDirWhenNoSegmentsExist(@TempDir Path tempDir) throws IOException {
        MinioStorageService service = new MinioStorageService(DUMMY_CLIENT, "test-bucket") {
            @Override
            protected List<String> listObjectNames(String objectPrefix) {
                return List.of();
            }
        };

        Path targetDir = tempDir.resolve("job-empty").resolve("0");
        boolean restored = service.restorePartition("job-empty", 0, targetDir);

        assertFalse(restored);
        assertFalse(Files.exists(targetDir), "targetDir must not be created for empty partitions");
        assertFalse(Files.exists(targetDir.getParent()), "job parent dir should not be created when nothing is restored");
    }

    @Test
    void restorePartitionCleansUpStagingDirAndNeverCreatesTargetDirOnDownloadFailure(@TempDir Path tempDir) {
        MinioStorageService service = new MinioStorageService(DUMMY_CLIENT, "test-bucket") {
            @Override
            protected List<String> listObjectNames(String objectPrefix) {
                return List.of("job-fail/0/map-0.lz4", "job-fail/0/map-1.lz4");
            }

            @Override
            protected void downloadObject(String objectName, Path destinationFile) throws Exception {
                if (objectName.endsWith("map-0.lz4")) {
                    Files.writeString(destinationFile, "partial-data", StandardCharsets.UTF_8);
                } else {
                    throw new IOException("Simulated S3 read timeout");
                }
            }
        };

        Path jobDir = tempDir.resolve("job-fail");
        Path targetDir = jobDir.resolve("0");

        assertThrows(IOException.class, () -> service.restorePartition("job-fail", 0, targetDir));
        assertFalse(Files.exists(targetDir), "targetDir must never exist after failed restore");

        if (Files.exists(jobDir)) {
            try (Stream<Path> entries = Files.list(jobDir)) {
                assertEquals(0L, entries.count(), "Temporary .tmp staging directory must be cleaned up on failure");
            } catch (IOException e) {
                fail(e);
            }
        }
    }

    @Test
    void restorePartitionStagesInSiblingTmpDirBeforeAtomicMove(@TempDir Path tempDir) throws Exception {
        Path jobDir = tempDir.resolve("job-ok");
        Path targetDir = jobDir.resolve("2");
        CountDownLatch inFlight = new CountDownLatch(1);
        AtomicBoolean targetExistedDuringDownload = new AtomicBoolean(false);
        AtomicBoolean stagingTmpExistedDuringDownload = new AtomicBoolean(false);

        MinioStorageService service = new MinioStorageService(DUMMY_CLIENT, "test-bucket") {
            @Override
            protected List<String> listObjectNames(String objectPrefix) {
                return List.of("job-ok/2/map-0.lz4", "job-ok/2/map-1.lz4");
            }

            @Override
            protected void downloadObject(String objectName, Path destinationFile) throws Exception {
                targetExistedDuringDownload.compareAndSet(false, Files.exists(targetDir));
                try (Stream<Path> siblings = Files.list(jobDir)) {
                    if (siblings.anyMatch(p -> p.getFileName().toString().startsWith("2.restore-")
                            && p.getFileName().toString().endsWith(".tmp"))) {
                        stagingTmpExistedDuringDownload.set(true);
                    }
                }
                inFlight.countDown();
                Files.writeString(destinationFile, "payload-" + destinationFile.getFileName(), StandardCharsets.UTF_8);
            }
        };

        boolean restored = service.restorePartition("job-ok", 2, targetDir);
        assertTrue(inFlight.await(5, TimeUnit.SECONDS));

        assertTrue(restored);
        assertFalse(targetExistedDuringDownload.get(), "targetDir must not exist before all segments finish downloading");
        assertTrue(stagingTmpExistedDuringDownload.get(), "segments must download into a sibling .tmp staging directory");
        assertTrue(Files.isDirectory(targetDir));
        assertEquals("payload-map-0.lz4", Files.readString(targetDir.resolve("map-0.lz4")));
        assertEquals("payload-map-1.lz4", Files.readString(targetDir.resolve("map-1.lz4")));

        try (Stream<Path> siblings = Files.list(jobDir)) {
            assertFalse(siblings.anyMatch(p -> p.toString().endsWith(".tmp")),
                    "No .tmp staging directory should remain after atomic move");
        }
    }

    @Test
    void resolveMaxS3ConcurrencyDefaultsTo32AndRejectsInvalidValues() {
        assertEquals(32, MinioStorageService.resolveMaxS3Concurrency(Map.of()));
        assertEquals(16, MinioStorageService.resolveMaxS3Concurrency(Map.of("ESS_MAX_S3_CONCURRENCY", "16")));
        assertThrows(IllegalArgumentException.class,
                () -> MinioStorageService.resolveMaxS3Concurrency(Map.of("ESS_MAX_S3_CONCURRENCY", "0")));
        assertThrows(IllegalArgumentException.class,
                () -> new MinioStorageService(DUMMY_CLIENT, "test-bucket", 0));
    }

    @Test
    void globalS3ConcurrencyCapEnforcedAcrossSimultaneousSpillAndRestore(@TempDir Path tempDir) throws Exception {
        int cap = 3;
        Path spillJobDir = Files.createDirectories(tempDir.resolve("spill-source/job-spill/0"));
        for (int i = 0; i < 6; i++) {
            Files.writeString(spillJobDir.resolve("map-" + i + ".lz4"), "spill-data-" + i, StandardCharsets.UTF_8);
        }
        Path restoreTargetDir = tempDir.resolve("restore-target/job-restore/1");

        AtomicInteger currentInFlight = new AtomicInteger(0);
        AtomicInteger maxObservedInFlight = new AtomicInteger(0);
        CountDownLatch saturatedAtCap = new CountDownLatch(1);

        try (MinioStorageService service = new MinioStorageService(DUMMY_CLIENT, "test-bucket", cap) {
            @Override
            protected List<String> listObjectNames(String objectPrefix) {
                return List.of(
                        "job-restore/1/seg-0.lz4",
                        "job-restore/1/seg-1.lz4",
                        "job-restore/1/seg-2.lz4",
                        "job-restore/1/seg-3.lz4",
                        "job-restore/1/seg-4.lz4",
                        "job-restore/1/seg-5.lz4"
                );
            }

            @Override
            protected void uploadObject(String objectName, Path sourceFile) throws Exception {
                trackConcurrentIo();
            }

            @Override
            protected void downloadObject(String objectName, Path destinationFile) throws Exception {
                trackConcurrentIo();
                Files.writeString(destinationFile, "restored-" + destinationFile.getFileName(), StandardCharsets.UTF_8);
            }

            private void trackConcurrentIo() throws InterruptedException {
                int active = currentInFlight.incrementAndGet();
                maxObservedInFlight.accumulateAndGet(active, Math::max);
                if (active >= cap) {
                    saturatedAtCap.countDown();
                }
                try {
                    Thread.sleep(60);
                } finally {
                    currentInFlight.decrementAndGet();
                }
            }
        }; ExecutorService callers = Executors.newFixedThreadPool(2)) {
            Future<?> spillFuture = callers.submit(() -> {
                try {
                    service.spillJobData("job-spill", tempDir.resolve("spill-source/job-spill"));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            Future<Boolean> restoreFuture = callers.submit(() -> {
                try {
                    return service.restorePartition("job-restore", 1, restoreTargetDir);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            assertTrue(saturatedAtCap.await(5, TimeUnit.SECONDS), "Shared S3 pool should saturate up to configured cap");
            spillFuture.get(10, TimeUnit.SECONDS);
            assertTrue(restoreFuture.get(10, TimeUnit.SECONDS));

            assertEquals(cap, service.getMaxS3Concurrency());
            assertTrue(maxObservedInFlight.get() <= cap,
                    "Total concurrent MinIO operations (" + maxObservedInFlight.get() + ") exceeded cap " + cap);
            assertEquals(0, currentInFlight.get());
        }
    }

    @Test
    void spillJobDataIgnoresMarkerFilesAndPropagatesPartialFailure(@TempDir Path tempDir) throws Exception {
        Path jobDir = Files.createDirectories(tempDir.resolve("job-partial-fail/0"));
        Files.writeString(jobDir.resolve("map-0.lz4"), "ok-0");
        Files.writeString(jobDir.resolve("map-1.lz4"), "fail-1");
        Files.writeString(jobDir.resolve("map-2.lz4"), "ok-2");
        Files.writeString(jobDir.resolve("map-3.lz4.tmp"), "ignore-tmp");
        Files.writeString(jobDir.resolve("attempt-1.attempt"), "ignore-attempt");

        Set<String> uploadedObjects = ConcurrentHashMap.newKeySet();

        try (MinioStorageService service = new MinioStorageService(DUMMY_CLIENT, "test-bucket", 2) {
            @Override
            protected void uploadObject(String objectName, Path sourceFile) throws Exception {
                uploadedObjects.add(objectName);
                if (objectName.endsWith("map-1.lz4")) {
                    throw new IOException("Simulated S3 PutObject failure");
                }
            }
        }) {
            assertThrows(IOException.class, () -> service.spillJobData("job-partial-fail", tempDir.resolve("job-partial-fail")));
            assertFalse(uploadedObjects.stream().anyMatch(name -> name.endsWith(".tmp") || name.endsWith(".attempt")),
                    "Spill must ignore .tmp and .attempt files");
        }
    }

    @Test
    void closeShutsDownTransferPoolAndRejectsSubsequentTransfers(@TempDir Path tempDir) throws Exception {
        Path jobDir = Files.createDirectories(tempDir.resolve("job-closed/0"));
        Files.writeString(jobDir.resolve("map-0.lz4"), "data");

        MinioStorageService service = new MinioStorageService(DUMMY_CLIENT, "test-bucket", 2);
        service.close();

        assertThrows(IOException.class, () -> service.spillJobData("job-closed", tempDir.resolve("job-closed")));
    }
}
