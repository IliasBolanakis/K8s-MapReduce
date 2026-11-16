package com.iliasbolan.ess.grpc;

import com.iliasbolan.ess.ShuffleDaemon;
import com.iliasbolan.ess.gc.StorageJanitor;
import com.iliasbolan.ess.storage.MinioStorageService;
import com.iliasbolan.grpc.shuffle.PartitionChunk;
import com.iliasbolan.grpc.shuffle.PartitionRequest;
import com.iliasbolan.grpc.shuffle.ShuffleServiceGrpc;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.minio.MinioClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ShuffleServiceImplTest {

    private static final Metadata.Key<String> AUTH_HEADER_KEY =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    private static final MinioClient DUMMY_CLIENT = MinioClient.builder()
            .endpoint("http://127.0.0.1:9000")
            .credentials("minioadmin", "minioadmin")
            .build();

    @Test
    void concurrentGetPartitionSingleFlightRestoreStreamsFullDataToAllCallers(@TempDir Path tempDir) throws Exception {
        String jobId = "job-singleflight-1";
        int partitionId = 3;
        byte[] expectedSegment0 = "segment-zero-payload-123456".getBytes(StandardCharsets.UTF_8);
        byte[] expectedSegment1 = "segment-one-payload-abcdef".getBytes(StandardCharsets.UTF_8);

        AtomicInteger restoreCalls = new AtomicInteger(0);
        CountDownLatch restoreStarted = new CountDownLatch(1);
        CountDownLatch allowRestoreToFinish = new CountDownLatch(1);

        MinioStorageService slowRestoreService = new MinioStorageService(DUMMY_CLIENT, "test-bucket") {
            @Override
            protected List<String> listObjectNames(String objectPrefix) {
                restoreCalls.incrementAndGet();
                return List.of(jobId + "/" + partitionId + "/map-0.lz4",
                        jobId + "/" + partitionId + "/map-1.lz4");
            }

            @Override
            protected void downloadObject(String objectName, Path destinationFile) throws Exception {
                restoreStarted.countDown();
                assertTrue(allowRestoreToFinish.await(5, TimeUnit.SECONDS));
                if (objectName.endsWith("map-0.lz4")) {
                    Files.write(destinationFile, expectedSegment0);
                } else {
                    Files.write(destinationFile, expectedSegment1);
                }
            }
        };

        try (TestGrpcHarness harness = new TestGrpcHarness(tempDir, slowRestoreService)) {
            int callers = 6;
            ExecutorService clientPool = Executors.newFixedThreadPool(callers);
            CountDownLatch allSubmitted = new CountDownLatch(callers);
            List<Future<byte[]>> futures = new ArrayList<>();

            for (int i = 0; i < callers; i++) {
                futures.add(clientPool.submit((Callable<byte[]>) () -> {
                    allSubmitted.countDown();
                    return harness.fetchPartition(jobId, partitionId);
                }));
            }

            assertTrue(allSubmitted.await(5, TimeUnit.SECONDS));
            assertTrue(restoreStarted.await(5, TimeUnit.SECONDS));
            // Give concurrent callers time to queue behind the per-(jobId, partitionId) lock
            Thread.sleep(150);
            assertTrue(StorageJanitor.hasActiveLease(jobId), "Active lease must be held during restore");
            allowRestoreToFinish.countDown();

            String expectedCombined = new String(expectedSegment0, StandardCharsets.UTF_8)
                    + new String(expectedSegment1, StandardCharsets.UTF_8);

            for (Future<byte[]> future : futures) {
                byte[] received = future.get(10, TimeUnit.SECONDS);
                String receivedStr = new String(received, StandardCharsets.UTF_8);
                assertFalse(receivedStr.isEmpty(), "Concurrent caller must never receive an empty stream");
                assertTrue(receivedStr.contains("segment-zero-payload-123456"));
                assertTrue(receivedStr.contains("segment-one-payload-abcdef"));
                assertEquals(expectedCombined.length(), received.length);
            }

            clientPool.shutdown();
            assertTrue(clientPool.awaitTermination(5, TimeUnit.SECONDS));
            assertEquals(1, restoreCalls.get(), "Single-flight lock must ensure restore runs only once");

            // Wait briefly for server finally block to release the lease after onCompleted
            long deadline = System.currentTimeMillis() + 2000;
            while (StorageJanitor.hasActiveLease(jobId) && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertEquals(0, StorageJanitor.getActiveLeaseCount(jobId));
        }
    }

    @Test
    void s3RestoreFailurePropagatesStatusUnavailableInsteadOfEmptyOk(@TempDir Path tempDir) throws Exception {
        String jobId = "job-s3-down";
        MinioStorageService failingService = new MinioStorageService(DUMMY_CLIENT, "test-bucket") {
            @Override
            protected List<String> listObjectNames(String objectPrefix) throws Exception {
                throw new IOException("MinIO connection refused");
            }
        };

        try (TestGrpcHarness harness = new TestGrpcHarness(tempDir, failingService)) {
            StatusRuntimeException ex = assertThrows(StatusRuntimeException.class,
                    () -> harness.fetchPartition(jobId, 0));

            assertEquals(Status.Code.UNAVAILABLE, ex.getStatus().getCode());
            assertFalse(Files.exists(tempDir.resolve(jobId).resolve("0")));
            assertEquals(0, StorageJanitor.getActiveLeaseCount(jobId));
        }
    }

    @Test
    void genuineEmptyPartitionReturnsEmptyChunkAndOk(@TempDir Path tempDir) throws Exception {
        String jobId = "job-skew-empty";
        MinioStorageService emptyService = new MinioStorageService(DUMMY_CLIENT, "test-bucket") {
            @Override
            protected List<String> listObjectNames(String objectPrefix) {
                return List.of();
            }
        };

        try (TestGrpcHarness harness = new TestGrpcHarness(tempDir, emptyService)) {
            byte[] data = harness.fetchPartition(jobId, 1);
            assertEquals(0, data.length);
            assertFalse(Files.exists(tempDir.resolve(jobId).resolve("1")));
            assertEquals(0, StorageJanitor.getActiveLeaseCount(jobId));
        }
    }

    private static final class TestGrpcHarness implements AutoCloseable {
        private final Server server;
        private final ManagedChannel channel;

        TestGrpcHarness(Path baseDir, MinioStorageService minioStorageService) throws IOException {
            ServerInterceptor authInterceptor = new ServerInterceptor() {
                @Override
                public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                        ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
                    String token = headers.get(AUTH_HEADER_KEY);
                    Context context = Context.current().withValue(ShuffleDaemon.AUTH_TOKEN_KEY, token);
                    return Contexts.interceptCall(context, call, headers, next);
                }
            };

            this.server = ServerBuilder.forPort(0)
                    .addService(new ShuffleServiceImpl(baseDir.toString(), minioStorageService))
                    .intercept(authInterceptor)
                    .build()
                    .start();

            this.channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
                    .usePlaintext()
                    .build();
        }

        byte[] fetchPartition(String jobId, int partitionId) throws IOException {
            Metadata headers = new Metadata();
            headers.put(AUTH_HEADER_KEY, computeToken(jobId));

            ShuffleServiceGrpc.ShuffleServiceBlockingStub stub = ShuffleServiceGrpc.newBlockingStub(channel)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers))
                    .withDeadlineAfter(10, TimeUnit.SECONDS);

            Iterator<PartitionChunk> chunks = stub.getPartition(PartitionRequest.newBuilder()
                    .setJobId(jobId)
                    .setPartitionId(partitionId)
                    .build());

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            while (chunks.hasNext()) {
                out.write(chunks.next().getContent().toByteArray());
            }
            return out.toByteArray();
        }

        private static String computeToken(String jobId) {
            try {
                String secretKey = System.getenv().getOrDefault("ESS_SECRET_KEY", "dev-insecure-shared-secret");
                Mac mac = Mac.getInstance("HmacSHA256");
                mac.init(new SecretKeySpec(secretKey.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
                byte[] hashBytes = mac.doFinal(jobId.getBytes(StandardCharsets.UTF_8));
                StringBuilder hex = new StringBuilder();
                for (byte b : hashBytes) {
                    String h = Integer.toHexString(0xff & b);
                    if (h.length() == 1) hex.append('0');
                    hex.append(h);
                }
                return hex.toString();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() throws InterruptedException {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
