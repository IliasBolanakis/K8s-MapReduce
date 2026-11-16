package com.iliasbolan.services;

import com.iliasbolan.infrastructure.MinioConnectionManager;
import io.minio.GetObjectArgs;
import io.minio.GetObjectResponse;
import io.minio.MinioClient;
import io.minio.StatObjectArgs;
import io.minio.StatObjectResponse;
import okhttp3.Headers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for atomic user-code staging and bounded S3 range reads in {@link S3ClientService}.
 */
class S3ClientServiceTest {

    private MinioConnectionManager connectionManager;
    private MinioClient minioClient;
    private S3ClientService s3ClientService;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        connectionManager = mock(MinioConnectionManager.class);
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

    @Test
    void testDownloadUserCode_SkipsS3FetchWhenTargetAlreadyExists() throws Throwable {
        Path existing = tempDir.resolve("usercode").resolve("Mapper.class");
        Files.createDirectories(existing.getParent());
        Files.writeString(existing, "already-localized");

        s3ClientService.downloadUserCode("code", "Mapper.class", existing.toString());

        verify(minioClient, times(0)).getObject(any(GetObjectArgs.class));
        assertEquals("already-localized", Files.readString(existing));
    }

    @Test
    void testReadDataChunk_BoundsLengthClampedToObjectSize_CachesStat_AndDrainsStream() throws Throwable {
        byte[] content = "rec0\nrec1\nrec2\ntrailing_extra_data\n".getBytes(StandardCharsets.UTF_8);
        StatObjectResponse stat = mock(StatObjectResponse.class);
        when(stat.size()).thenReturn((long) content.length);
        when(minioClient.statObject(any(StatObjectArgs.class))).thenReturn(stat);

        AtomicBoolean fullyDrained = new AtomicBoolean(false);
        when(minioClient.getObject(any(GetObjectArgs.class))).thenAnswer(invocation -> {
            GetObjectArgs args = invocation.getArgument(0);
            int start = args.offset().intValue();
            int len = args.length().intValue();
            byte[] slice = Arrays.copyOfRange(content, start, Math.min(content.length, start + len));
            InputStream trackingStream = new ByteArrayInputStream(slice) {
                @Override
                public int read(byte[] b, int off, int l) {
                    int n = super.read(b, off, l);
                    if (n == -1) {
                        fullyDrained.set(true);
                    }
                    return n;
                }
            };
            return new GetObjectResponse(Headers.of(), "data", "", "input.txt", trackingStream);
        });

        // Request offset=5 ('r' in rec1), length=6 with 1MiB lookahead -> clamped to content.length - 4
        List<String> first = s3ClientService.readDataChunk("data", "input.txt", 5, 6);
        assertEquals(List.of("rec1", "rec2"), first);
        assertTrue(fullyDrained.get(), "Bounded stream must be drained for OkHttp keep-alive socket reuse");

        // Second call reuses cached statObject size without calling statObject again
        List<String> second = s3ClientService.readDataChunk("data", "input.txt", 0, 4);
        assertEquals(List.of("rec0"), second);

        verify(minioClient, times(1)).statObject(any(StatObjectArgs.class));

        ArgumentCaptor<GetObjectArgs> argsCaptor = ArgumentCaptor.forClass(GetObjectArgs.class);
        verify(minioClient, times(2)).getObject(argsCaptor.capture());
        GetObjectArgs firstArgs = argsCaptor.getAllValues().get(0);
        assertEquals(4L, firstArgs.offset());
        assertEquals((long) content.length - 4L, firstArgs.length());
    }

    @Test
    void testReadDataChunk_StraddlingLinePastInitialWindow_IssuesBoundedContinuationFetch() throws Throwable {
        // Use small lookahead=8 bytes so a 30-byte line straddles across multiple bounded windows
        S3ClientService boundedService = new S3ClientService(connectionManager, 8L);

        String longLine = "first_long_record_exceeding_window_boundary";
        byte[] content = ("hdr\n" + longLine + "\nafter\n").getBytes(StandardCharsets.UTF_8);

        StatObjectResponse stat = mock(StatObjectResponse.class);
        when(stat.size()).thenReturn((long) content.length);
        when(minioClient.statObject(any(StatObjectArgs.class))).thenReturn(stat);

        when(minioClient.getObject(any(GetObjectArgs.class))).thenAnswer(invocation -> {
            GetObjectArgs args = invocation.getArgument(0);
            int start = args.offset().intValue();
            int len = args.length().intValue();
            byte[] slice = Arrays.copyOfRange(content, start, Math.min(content.length, start + len));
            return new GetObjectResponse(Headers.of(), "data", "", "straddle.txt", new ByteArrayInputStream(slice));
        });

        // offset=4 (right after "hdr\n"), length=4 -> lookback byte 3 is '\n' (aligned!), initial window=13B
        // longLine is 43 bytes -> requires continuation bounded fetches!
        List<String> records = boundedService.readDataChunk("data", "straddle.txt", 4, 4);
        assertEquals(List.of(longLine), records);

        ArgumentCaptor<GetObjectArgs> captor = ArgumentCaptor.forClass(GetObjectArgs.class);
        verify(minioClient, times(5)).getObject(captor.capture());
        for (GetObjectArgs callArgs : captor.getAllValues()) {
            assertTrue(callArgs.length() > 0 && callArgs.offset() + callArgs.length() <= content.length,
                    "Every continuation request must be bounded and clamped to object size");
        }
    }
}
