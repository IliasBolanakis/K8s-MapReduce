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
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.MetadataUtils;
import io.minio.MinioClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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

    @Test
    void maxLocalStreamsSaturationReturnsResourceExhaustedAndReleasesPermits(@TempDir Path tempDir) throws Exception {
        String jobId = "job-stream-cap";
        CountDownLatch twoRestoresActive = new CountDownLatch(2);
        CountDownLatch allowRestoresToFinish = new CountDownLatch(1);

        MinioStorageService gatedService = new MinioStorageService(DUMMY_CLIENT, "test-bucket", 4) {
            @Override
            protected List<String> listObjectNames(String objectPrefix) {
                return List.of(objectPrefix + "map-0.lz4");
            }

            @Override
            protected void downloadObject(String objectName, Path destinationFile) throws Exception {
                twoRestoresActive.countDown();
                assertTrue(allowRestoresToFinish.await(5, TimeUnit.SECONDS));
                Files.writeString(destinationFile, "payload-" + objectName, StandardCharsets.UTF_8);
            }
        };

        ShuffleServiceImpl service = new ShuffleServiceImpl(tempDir.toString(), gatedService, 2, 60L);
        try (TestGrpcHarness harness = new TestGrpcHarness(service);
             ExecutorService clientPool = Executors.newFixedThreadPool(2)) {
            Future<byte[]> f0 = clientPool.submit(() -> harness.fetchPartition(jobId, 0));
            Future<byte[]> f1 = clientPool.submit(() -> harness.fetchPartition(jobId, 1));

            assertTrue(twoRestoresActive.await(5, TimeUnit.SECONDS));
            waitUntil(() -> service.getAvailableStreamPermits() == 0, 2000);

            StatusRuntimeException rejected = assertThrows(StatusRuntimeException.class,
                    () -> harness.fetchPartition(jobId, 2));
            assertEquals(Status.Code.RESOURCE_EXHAUSTED, rejected.getStatus().getCode());
            assertEquals(2, StorageJanitor.getActiveLeaseCount(jobId),
                    "Rejected request must not run stream or acquire a job lease");

            allowRestoresToFinish.countDown();
            assertTrue(new String(f0.get(5, TimeUnit.SECONDS), StandardCharsets.UTF_8).contains("map-0.lz4"));
            assertTrue(new String(f1.get(5, TimeUnit.SECONDS), StandardCharsets.UTF_8).contains("map-0.lz4"));

            waitUntil(() -> service.getAvailableStreamPermits() == 2 && StorageJanitor.getActiveLeaseCount(jobId) == 0, 2000);

            // Subsequent fetch succeeds cleanly once permits are released
            byte[] thirdOk = harness.fetchPartition(jobId, 2);
            assertTrue(new String(thirdOk, StandardCharsets.UTF_8).contains("job-stream-cap/2/map-0.lz4"));
        } finally {
            service.close();
            gatedService.close();
        }
    }

    @Test
    void pooledDirectBufferIsReusedAcrossMultiSegmentAndSequentialStreams(@TempDir Path tempDir) throws Exception {
        String jobId = "job-buffer-reuse";
        Path partitionDir = Files.createDirectories(tempDir.resolve(jobId + "/0"));
        ByteArrayOutputStream expectedBytes = new ByteArrayOutputStream();
        for (int i = 0; i < 4; i++) {
            byte[] segment = new byte[140_000];
            for (int j = 0; j < segment.length; j++) {
                segment[j] = (byte) ((i * 31 + j) & 0x7F);
            }
            Files.write(partitionDir.resolve("map-" + i + ".lz4"), segment);
            expectedBytes.write(segment);
        }

        AtomicInteger acquireCalls = new AtomicInteger(0);
        AtomicInteger releaseCalls = new AtomicInteger(0);
        Set<Integer> bufferIdentityHashes = ConcurrentHashMap.newKeySet();

        ShuffleServiceImpl trackingService = new ShuffleServiceImpl(tempDir.toString(), null, 4, 500L) {
            @Override
            protected ByteBuffer acquireDirectBuffer() {
                acquireCalls.incrementAndGet();
                ByteBuffer buf = super.acquireDirectBuffer();
                assertTrue(buf.isDirect(), "Pooled stream buffer must be a Direct ByteBuffer");
                bufferIdentityHashes.add(System.identityHashCode(buf));
                return buf;
            }

            @Override
            protected void releaseDirectBuffer(ByteBuffer buffer) {
                releaseCalls.incrementAndGet();
                super.releaseDirectBuffer(buffer);
            }
        };

        try (TestGrpcHarness harness = new TestGrpcHarness(trackingService)) {
            byte[] first = harness.fetchPartition(jobId, 0);
            assertArrayEquals(expectedBytes.toByteArray(), first);
            waitUntil(() -> trackingService.getPooledBufferCount() == 1, 2000);
            assertEquals(1, acquireCalls.get(), "4 segments in one stream must lease only 1 Direct ByteBuffer");
            assertEquals(1, releaseCalls.get());
            assertEquals(1, bufferIdentityHashes.size());

            byte[] second = harness.fetchPartition(jobId, 0);
            assertArrayEquals(expectedBytes.toByteArray(), second);
            waitUntil(() -> trackingService.getPooledBufferCount() == 1 && releaseCalls.get() == 2, 2000);
            assertEquals(2, acquireCalls.get());
            assertEquals(1, bufferIdentityHashes.size(), "Sequential stream must reuse the exact pooled Direct ByteBuffer");
            assertEquals(4, trackingService.getAvailableStreamPermits());
        } finally {
            trackingService.close();
        }
    }

    @Test
    void midStreamClientCancellationReturnsPooledBufferAndStreamPermit(@TempDir Path tempDir) throws Exception {
        String jobId = "job-cancel-midstream";
        Path partitionDir = Files.createDirectories(tempDir.resolve(jobId + "/0"));
        byte[] largePayload = new byte[3 * 1024 * 1024];
        Files.write(partitionDir.resolve("map-0.lz4"), largePayload);

        AtomicInteger acquireCalls = new AtomicInteger(0);
        AtomicInteger releaseCalls = new AtomicInteger(0);

        ShuffleServiceImpl trackingService = new ShuffleServiceImpl(tempDir.toString(), null, 2, 500L) {
            @Override
            protected ByteBuffer acquireDirectBuffer() {
                acquireCalls.incrementAndGet();
                return super.acquireDirectBuffer();
            }

            @Override
            protected void releaseDirectBuffer(ByteBuffer buffer) {
                releaseCalls.incrementAndGet();
                super.releaseDirectBuffer(buffer);
            }
        };

        try (TestGrpcHarness harness = new TestGrpcHarness(trackingService)) {
            CountDownLatch firstChunkReceived = new CountDownLatch(1);
            CountDownLatch streamTerminated = new CountDownLatch(1);
            AtomicReference<ClientCallStreamObserver<PartitionRequest>> requestObserverRef = new AtomicReference<>();

            Metadata headers = new Metadata();
            headers.put(AUTH_HEADER_KEY, TestGrpcHarness.computeToken(jobId));
            ShuffleServiceGrpc.ShuffleServiceStub asyncStub = ShuffleServiceGrpc.newStub(harness.channel)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));

            asyncStub.getPartition(
                    PartitionRequest.newBuilder().setJobId(jobId).setPartitionId(0).build(),
                    new ClientResponseObserver<PartitionRequest, PartitionChunk>() {
                        @Override
                        public void beforeStart(ClientCallStreamObserver<PartitionRequest> requestStream) {
                            requestObserverRef.set(requestStream);
                            requestStream.disableAutoInboundFlowControl();
                        }

                        @Override
                        public void onNext(PartitionChunk value) {
                            firstChunkReceived.countDown();
                        }

                        @Override
                        public void onError(Throwable t) {
                            streamTerminated.countDown();
                        }

                        @Override
                        public void onCompleted() {
                            streamTerminated.countDown();
                        }
                    });

            requestObserverRef.get().request(1);
            assertTrue(firstChunkReceived.await(5, TimeUnit.SECONDS));
            requestObserverRef.get().cancel("Abort mid-stream", null);
            assertTrue(streamTerminated.await(5, TimeUnit.SECONDS));

            waitUntil(() -> releaseCalls.get() == 1
                    && trackingService.getPooledBufferCount() == 1
                    && trackingService.getAvailableStreamPermits() == 2
                    && StorageJanitor.getActiveLeaseCount(jobId) == 0, 3000);

            assertEquals(acquireCalls.get(), releaseCalls.get());
        } finally {
            trackingService.close();
        }
    }

    @Test
    void midStreamIoExceptionReturnsPooledBufferAndPermit(@TempDir Path tempDir) throws Exception {
        String jobId = "job-ioexception-midstream";
        Path partitionDir = Files.createDirectories(tempDir.resolve(jobId + "/0"));
        Files.write(partitionDir.resolve("map-0.lz4"), new byte[3 * 1024 * 1024]);
        Path secondSegment = partitionDir.resolve("map-1.lz4");
        Files.write(secondSegment, new byte[10_000]);

        AtomicInteger acquireCalls = new AtomicInteger(0);
        AtomicInteger releaseCalls = new AtomicInteger(0);

        ShuffleServiceImpl trackingService = new ShuffleServiceImpl(tempDir.toString(), null, 2, 500L) {
            @Override
            protected ByteBuffer acquireDirectBuffer() {
                acquireCalls.incrementAndGet();
                return super.acquireDirectBuffer();
            }

            @Override
            protected void releaseDirectBuffer(ByteBuffer buffer) {
                releaseCalls.incrementAndGet();
                super.releaseDirectBuffer(buffer);
            }
        };

        try (TestGrpcHarness harness = new TestGrpcHarness(trackingService)) {
            CountDownLatch firstChunkReceived = new CountDownLatch(1);
            CountDownLatch errorReceived = new CountDownLatch(1);
            AtomicReference<Throwable> terminalError = new AtomicReference<>();
            AtomicReference<ClientCallStreamObserver<PartitionRequest>> requestObserverRef = new AtomicReference<>();

            Metadata headers = new Metadata();
            headers.put(AUTH_HEADER_KEY, TestGrpcHarness.computeToken(jobId));
            ShuffleServiceGrpc.ShuffleServiceStub asyncStub = ShuffleServiceGrpc.newStub(harness.channel)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));

            asyncStub.getPartition(
                    PartitionRequest.newBuilder().setJobId(jobId).setPartitionId(0).build(),
                    new ClientResponseObserver<PartitionRequest, PartitionChunk>() {
                        @Override
                        public void beforeStart(ClientCallStreamObserver<PartitionRequest> requestStream) {
                            requestObserverRef.set(requestStream);
                            requestStream.disableAutoInboundFlowControl();
                        }

                        @Override
                        public void onNext(PartitionChunk value) {
                            firstChunkReceived.countDown();
                        }

                        @Override
                        public void onError(Throwable t) {
                            terminalError.set(t);
                            errorReceived.countDown();
                        }

                        @Override
                        public void onCompleted() {
                            errorReceived.countDown();
                        }
                    });

            requestObserverRef.get().request(1);
            assertTrue(firstChunkReceived.await(5, TimeUnit.SECONDS));
            Files.deleteIfExists(secondSegment);
            requestObserverRef.get().request(1000);

            assertTrue(errorReceived.await(5, TimeUnit.SECONDS));
            StatusRuntimeException statusEx = assertInstanceOf(StatusRuntimeException.class, terminalError.get());
            assertEquals(Status.Code.INTERNAL, statusEx.getStatus().getCode());

            waitUntil(() -> releaseCalls.get() == 1
                    && trackingService.getPooledBufferCount() == 1
                    && trackingService.getAvailableStreamPermits() == 2
                    && StorageJanitor.getActiveLeaseCount(jobId) == 0, 3000);

            assertEquals(acquireCalls.get(), releaseCalls.get());
        } finally {
            trackingService.close();
        }
    }

    @Test
    void degradedNullMinioFailsLoudWithFailedPreconditionOnMissingLocalPartition(@TempDir Path tempDir) throws Exception {
        String jobId = "job-degraded-missing";
        ShuffleServiceImpl degradedService = new ShuffleServiceImpl(tempDir.toString(), null, 4, 500L);

        try (TestGrpcHarness harness = new TestGrpcHarness(degradedService)) {
            StatusRuntimeException ex = assertThrows(StatusRuntimeException.class,
                    () -> harness.fetchPartition(jobId, 0));

            assertEquals(Status.Code.FAILED_PRECONDITION, ex.getStatus().getCode());
            assertTrue(ex.getStatus().getDescription().contains("MinIO overflow storage is unavailable"));
            waitUntil(() -> degradedService.getAvailableStreamPermits() == 4
                    && StorageJanitor.getActiveLeaseCount(jobId) == 0, 2000);
        } finally {
            degradedService.close();
        }
    }

    @Test
    void localEmptyDirectoryIgnoringTmpAndAttemptMarkersReturnsEmptyOk(@TempDir Path tempDir) throws Exception {
        String jobId = "job-local-empty";
        Path partitionDir = Files.createDirectories(tempDir.resolve(jobId + "/0"));
        Files.writeString(partitionDir.resolve("map-0.lz4.tmp"), "in-progress-tmp", StandardCharsets.UTF_8);
        Files.writeString(partitionDir.resolve("attempt-1.attempt"), "fenced-attempt", StandardCharsets.UTF_8);
        Files.writeString(partitionDir.resolve(".attempt"), "marker", StandardCharsets.UTF_8);

        ShuffleServiceImpl degradedService = new ShuffleServiceImpl(tempDir.toString(), null, 4, 500L);
        try (TestGrpcHarness harness = new TestGrpcHarness(degradedService)) {
            byte[] data = harness.fetchPartition(jobId, 0);
            assertEquals(0, data.length);
            assertEquals(0, degradedService.getPooledBufferCount(), "Empty skew partition does not lease a Direct ByteBuffer");
            waitUntil(() -> degradedService.getAvailableStreamPermits() == 4
                    && StorageJanitor.getActiveLeaseCount(jobId) == 0, 2000);
        } finally {
            degradedService.close();
        }
    }

    private static void waitUntil(java.util.function.BooleanSupplier condition, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertTrue(condition.getAsBoolean(), "Condition did not become true within " + timeoutMs + "ms");
    }

    private static final class TestGrpcHarness implements AutoCloseable {
        private final Server server;
        private final ManagedChannel channel;

        TestGrpcHarness(Path baseDir, MinioStorageService minioStorageService) throws IOException {
            this(new ShuffleServiceImpl(baseDir.toString(), minioStorageService));
        }

        TestGrpcHarness(ShuffleServiceImpl service) throws IOException {
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
                    .addService(service)
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
