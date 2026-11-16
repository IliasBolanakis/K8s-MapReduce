package com.iliasbolan.services;

import com.iliasbolan.infrastructure.MinioConnectionManager;
import io.minio.GetObjectArgs;
import io.minio.GetObjectResponse;
import io.minio.MinioClient;
import okhttp3.Headers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for atomic user-code staging in {@link S3ClientService}.
 */
class S3ClientServiceTest {

    private MinioClient minioClient;
    private S3ClientService s3ClientService;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        MinioConnectionManager connectionManager = mock(MinioConnectionManager.class);
        minioClient = mock(MinioClient.class);
        when(connectionManager.getClient()).thenReturn(minioClient);
        s3ClientService = new S3ClientService(connectionManager);
    }

    @Test
    void testDownloadUserCode_AtomicallyStagesViaTmpAndLeavesNoTmpFiles() throws Throwable {
        byte[] expectedBytes = new byte[]{(byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE, 1, 2, 3, 4};
        GetObjectResponse response = new GetObjectResponse(
                Headers.of(), "code", "", "Mapper.class", new ByteArrayInputStream(expectedBytes));
        when(minioClient.getObject(any(GetObjectArgs.class))).thenReturn(response);

        Path targetPath = tempDir.resolve("usercode").resolve("Mapper.class");
        s3ClientService.downloadUserCode("code", "Mapper.class", targetPath.toString());

        assertTrue(Files.exists(targetPath), "Target class file should exist after atomic move");
        assertArrayEquals(expectedBytes, Files.readAllBytes(targetPath));

        try (Stream<Path> files = Files.list(targetPath.getParent())) {
            List<String> names = files.map(p -> p.getFileName().toString()).toList();
            assertEquals(List.of("Mapper.class"), names, "No .tmp files should remain after download");
        }
    }

    @Test
    void testDownloadUserCode_CleansUpTmpFileOnStreamFailure() throws Exception {
        when(minioClient.getObject(any(GetObjectArgs.class))).thenAnswer(invocation -> {
            InputStream failingStream = new InputStream() {
                private int count = 0;

                @Override
                public int read() throws IOException {
                    if (count++ < 4) {
                        return 0x42;
                    }
                    throw new IOException("Simulated S3 read interruption");
                }
            };
            return new GetObjectResponse(Headers.of(), "code", "", "Mapper.class", failingStream);
        });

        Path targetPath = tempDir.resolve("usercode").resolve("Mapper.class");

        assertThrows(IOException.class,
                () -> s3ClientService.downloadUserCode("code", "Mapper.class", targetPath.toString()));

        assertFalse(Files.exists(targetPath), "Partial target file must not exist after failed download");
        try (Stream<Path> files = Files.list(targetPath.getParent())) {
            List<Path> remaining = files.toList();
            assertTrue(remaining.isEmpty(), "Temporary staging files must be cleaned up in finally: " + remaining);
        }
    }
}
