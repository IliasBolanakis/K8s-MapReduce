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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
}
