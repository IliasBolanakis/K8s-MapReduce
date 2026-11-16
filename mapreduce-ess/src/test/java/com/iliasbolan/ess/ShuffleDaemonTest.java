package com.iliasbolan.ess;

import com.iliasbolan.ess.gc.StorageJanitor;
import com.iliasbolan.ess.grpc.ShuffleServiceImpl;
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
import io.grpc.stub.MetadataUtils;
import io.minio.MinioClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class ShuffleDaemonTest {

    private static final Metadata.Key<String> AUTH_HEADER_KEY =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    private static final MinioClient DUMMY_CLIENT = MinioClient.builder()
            .endpoint("http://127.0.0.1:9000")
            .credentials("minioadmin", "minioadmin")
            .build();

    @Test
    void shutdownSequenceDrainsActiveStreamAndJanitorSpillBeforeClosingMinio(@TempDir Path rootDir) throws Exception {
        String streamJobId = "job-drain-stream";
        String spillJobId = "job-drain-spill";
        Files.createDirectories(rootDir.resolve(spillJobId + "/0"));
        Files.writeString(rootDir.resolve(spillJobId + "/0/map-0.lz4"), "spill-payload", StandardCharsets.UTF_8);

        CountDownLatch restoreStarted = new CountDownLatch(1);
        CountDownLatch allowRestoreFinish = new CountDownLatch(1);
        CountDownLatch spillStarted = new CountDownLatch(1);
        CountDownLatch allowSpillFinish = new CountDownLatch(1);
        AtomicBoolean spillInterrupted = new AtomicBoolean(false);
        List<String> lifecycleEvents = new CopyOnWriteArrayList<>();

        MinioStorageService minioService = new MinioStorageService(DUMMY_CLIENT, "test-bucket", 4) {
            @Override
            protected List<String> listObjectNames(String objectPrefix) {
                return List.of(streamJobId + "/0/map-0.lz4");
            }

            @Override
            protected void downloadObject(String objectName, Path destinationFile) throws Exception {
                restoreStarted.countDown();
                assertTrue(allowRestoreFinish.await(5, TimeUnit.SECONDS));
                Files.writeString(destinationFile, "streamed-payload", StandardCharsets.UTF_8);
                lifecycleEvents.add("stream-restore-finished");
            }

            @Override
            public void spillJobData(String jobId, Path jobDirectory) throws IOException {
                spillStarted.countDown();
                try {
                    assertTrue(allowSpillFinish.await(5, TimeUnit.SECONDS));
                    lifecycleEvents.add("janitor-spill-finished");
                } catch (InterruptedException e) {
                    spillInterrupted.set(true);
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted during spill", e);
                }
            }

            @Override
            public void close() {
                lifecycleEvents.add("minio-closed");
                super.close();
            }
        };

        ShuffleServiceImpl shuffleService = new ShuffleServiceImpl(rootDir.toString(), minioService, 4, 500L);
        ServerInterceptor authInterceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
                String token = headers.get(AUTH_HEADER_KEY);
                Context context = Context.current().withValue(ShuffleDaemon.AUTH_TOKEN_KEY, token);
                return Contexts.interceptCall(context, call, headers, next);
            }
        };

        Server server = ServerBuilder.forPort(0)
                .addService(shuffleService)
                .intercept(authInterceptor)
                .build()
                .start();

        ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
                .usePlaintext()
                .build();

        StorageJanitor.start(rootDir.toString(), minioService, 10, 50, TimeUnit.MILLISECONDS,
                new FakeFileStore(1000L, 100L));

        try (ExecutorService exec = Executors.newFixedThreadPool(2)) {
            Future<byte[]> streamFuture = exec.submit(() -> fetchPartition(channel, streamJobId, 0));

            assertTrue(restoreStarted.await(5, TimeUnit.SECONDS));
            assertTrue(spillStarted.await(5, TimeUnit.SECONDS));

            Future<?> shutdownFuture = exec.submit(() ->
                    ShuffleDaemon.shutdownSequence(server, shuffleService, minioService, 5L));

            // Allow both in-flight operations to drain after shutdownSequence begins
            Thread.sleep(100);
            assertFalse(shutdownFuture.isDone(), "shutdownSequence must wait for active stream and Janitor spill");
            assertFalse(spillInterrupted.get(), "In-flight Janitor spill must not be interrupted");

            allowRestoreFinish.countDown();
            byte[] received = streamFuture.get(5, TimeUnit.SECONDS);
            assertEquals("streamed-payload", new String(received, StandardCharsets.UTF_8));

            allowSpillFinish.countDown();
            shutdownFuture.get(10, TimeUnit.SECONDS);

            assertTrue(server.isTerminated());
            assertFalse(StorageJanitor.isRunning());
            assertFalse(spillInterrupted.get());
            assertFalse(Files.exists(rootDir.resolve(spillJobId)), "Completed spill should delete local dir before stopping");
            assertEquals(4, shuffleService.getAvailableStreamPermits());

            int streamIdx = lifecycleEvents.indexOf("stream-restore-finished");
            int spillIdx = lifecycleEvents.indexOf("janitor-spill-finished");
            int closeIdx = lifecycleEvents.indexOf("minio-closed");
            assertTrue(streamIdx >= 0 && spillIdx > streamIdx && closeIdx > spillIdx,
                    "Expected strict drain order [stream -> janitor spill -> minio close], got: " + lifecycleEvents);
        } finally {
            StorageJanitor.stop(1, TimeUnit.SECONDS);
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            server.shutdownNow();
            shuffleService.close();
            minioService.close();
        }
    }

    private static byte[] fetchPartition(ManagedChannel channel, String jobId, int partitionId) throws IOException {
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
