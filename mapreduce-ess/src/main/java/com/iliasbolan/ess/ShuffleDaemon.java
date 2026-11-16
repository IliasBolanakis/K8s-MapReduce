package com.iliasbolan.ess;

import com.iliasbolan.ess.gc.StorageJanitor;
import com.iliasbolan.ess.grpc.ShuffleServiceImpl;
import com.iliasbolan.ess.storage.MinioStorageService;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * The primary entry point for the standalone External Shuffle Service (ESS) Daemon.
 * <p>
 * The ESS is a critical infrastructure component in the MapReduce architecture,
 * decoupled from the compute workers to enable stateless and ephemeral compute pods.
 * This daemon manages the physical persistence of intermediate shuffle data and
 * provides a high-performance gRPC streaming interface for Peer-to-Peer data
 * retrieval by Reducer pods.
 * </p>
 * <p>
 * <b>Architecture Update 1: Secure Data Plane (Token Authentication)</b><br>
 * Implements a Zero-Trust security model utilizing a global gRPC {@link ServerInterceptor}
 * to intercept incoming requests, extract a Manager-issued HMAC token, and bind it to a
 * thread-local {@link Context} for downstream cryptographic origin validation.
 * </p>
 * <p>
 * <b>Architecture Update 2: Multi-Tier Storage (MinIO Overflow)</b><br>
 * Integrates an S3-compatible overflow tier. To maintain O(1) memory footprint and
 * optimize connection pooling, a single {@link MinioStorageService} instance is bootstrapped
 * and injected into both the background Garbage Collector and the gRPC data plane.
 * When MinIO initialization fails, the daemon enters degraded local-only mode where
 * {@link ShuffleServiceImpl} fails loud ({@code FAILED_PRECONDITION}) on missing local partitions.
 * </p>
 * <p>
 * <b>Architecture Update 3: Ordered SIGTERM Drain (ADR-14)</b><br>
 * The JVM shutdown hook drains components in strict dependency order:
 * {@code server.shutdown()} &rarr; await gRPC server &amp; {@link ShuffleServiceImpl}
 * {@code streamingPool} drain (&le;20s, escalating to {@code server.shutdownNow()} if needed)
 * &rarr; {@link StorageJanitor#stop()} (completing any active spill/GC cycle cleanly)
 * &rarr; {@link MinioStorageService#close()} (draining the shared global S3 transfer pool).
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 4.0
 * @since 2026-05-13
 * @see com.iliasbolan.ess.gc.StorageJanitor
 * @see com.iliasbolan.ess.grpc.ShuffleServiceImpl
 * @see com.iliasbolan.ess.storage.MinioStorageService
 */
public class ShuffleDaemon {

    private static final Logger logger = LoggerFactory.getLogger(ShuffleDaemon.class);

    /** Default shutdown drain budget (in seconds) for gRPC server and active streams. */
    public static final long DEFAULT_DRAIN_TIMEOUT_SECONDS = 20L;

    /**
     * Thread-local gRPC Context Key used to propagate the authorization token securely
     * across the request lifecycle.
     */
    public static final Context.Key<String> AUTH_TOKEN_KEY = Context.key("jobToken");

    /**
     * The ASCII metadata key used to extract the authorization header from
     * the incoming gRPC network request.
     */
    private static final Metadata.Key<String> AUTH_HEADER_KEY =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    /**
     * Bootstraps the ESS Daemon, initializes multi-tier storage maintenance, activates
     * the secure gRPC server, and registers an ordered {@code SIGTERM} shutdown hook.
     *
     * @param args Command line arguments (Configuration is derived from Environment Variables).
     * @throws IOException          If the gRPC server fails to bind or physical storage is inaccessible.
     * @throws InterruptedException If the server process is interrupted while running.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        // 1. Configuration Loading (Cloud-Native Pattern)
        int port = Integer.parseInt(System.getenv().getOrDefault("ESS_PORT", "7337"));
        String baseShuffleDir = System.getenv().getOrDefault("SHUFFLE_DIR", "/mnt/mapreduce-shuffle");

        logger.info("====== [ ESS DAEMON BOOTSTRAP ] ======");
        logger.info("Port: {} | Local Shuffle Path: {}", port, baseShuffleDir);

        // 2. Initialize the Multi-Tier Storage Adapter (MinIO Overflow)
        // Instantiated once and shared to optimize HTTP connection pooling to the S3 cluster.
        MinioStorageService minioStorageService = null;
        try {
            minioStorageService = new MinioStorageService();
            logger.info("MinIO Overflow Tier successfully integrated.");
        } catch (Exception e) {
            logger.error("Failed to connect to MinIO cluster. Proceeding with Local-Only mode. WARNING: Missing local partitions will fail with FAILED_PRECONDITION!", e);
            // By catching and not throwing, the ESS runs in degraded local-only mode if S3 is down.
        }

        // 3. Initialize the Storage Maintenance Service
        // The Janitor is now aware of the overflow tier and will spill data to S3 before physical deletion.
        StorageJanitor.start(baseShuffleDir, minioStorageService);

        // --- 4. SECURE INTERCEPTOR ---
        // Implementation of the security gateway that extracts headers before message parsing.
        ServerInterceptor authInterceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

                // Extract the token from the network layer
                String token = headers.get(AUTH_HEADER_KEY);

                // Bind the token to the gRPC Thread-Local Context for downstream validation
                Context context = Context.current().withValue(AUTH_TOKEN_KEY, token);

                // Transfer control to the service implementation with the enriched context
                return Contexts.interceptCall(context, call, headers, next);
            }
        };

        // 5. Configure and start the gRPC Service Plane
        // We inject the storage service to enable Lazy-Restoration of spilled data.
        final ShuffleServiceImpl shuffleService = new ShuffleServiceImpl(baseShuffleDir, minioStorageService);
        final MinioStorageService sharedMinioService = minioStorageService;

        Server server = ServerBuilder.forPort(port)
                .addService(shuffleService)
                .intercept(authInterceptor)
                .build();

        server.start();
        logger.info("ESS Daemon successfully bound. Secure Interceptor & Overflow Routing ACTIVE.");

        // 6. Ordered Graceful Teardown hook (ADR-14)
        // Ensures active streams drain, Janitor finishes in-flight spills, and S3 pool closes in strict order.
        Runtime.getRuntime().addShutdownHook(new Thread(
                () -> shutdownSequence(server, shuffleService, sharedMinioService, DEFAULT_DRAIN_TIMEOUT_SECONDS),
                "ess-shutdown-hook"
        ));

        // Block the main thread until the server is shut down
        server.awaitTermination();
    }

    /**
     * Executes the ordered {@code SIGTERM} drain sequence across all ESS subsystems:
     * <ol>
     *   <li>Stop accepting new RPCs via {@link Server#shutdown()} and await termination up to
     *   {@code drainTimeoutSeconds}, escalating to {@link Server#shutdownNow()} if stalled.</li>
     *   <li>Drain {@link ShuffleServiceImpl#shutdownAndAwait(long, TimeUnit)} so active
     *   virtual-thread streams release leases, permits, and pooled Direct ByteBuffers.</li>
     *   <li>Stop {@link StorageJanitor#stop()} cleanly after any in-flight spill/GC cycle.</li>
     *   <li>Close {@link MinioStorageService#close()} to drain the global S3 transfer pool.</li>
     * </ol>
     *
     * @param server              The running gRPC server (may be {@code null}).
     * @param shuffleService      The gRPC data-plane service instance (may be {@code null}).
     * @param minioStorageService The shared MinIO overflow adapter (may be {@code null}).
     * @param drainTimeoutSeconds Maximum seconds to await gRPC server and stream drain.
     */
    public static void shutdownSequence(Server server,
                                        ShuffleServiceImpl shuffleService,
                                        MinioStorageService minioStorageService,
                                        long drainTimeoutSeconds) {
        logger.warn("Shutdown signal received. Executing ordered ESS Daemon drain...");
        long timeoutSecs = Math.max(1L, drainTimeoutSeconds);
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSecs);

        if (server != null) {
            server.shutdown();
            try {
                if (!server.awaitTermination(timeoutSecs, TimeUnit.SECONDS)) {
                    logger.warn("gRPC server did not terminate within {}s; escalating to server.shutdownNow().", timeoutSecs);
                    server.shutdownNow();
                    server.awaitTermination(2, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Interrupted while awaiting gRPC server termination; forcing server.shutdownNow().");
                server.shutdownNow();
            }
        }

        if (shuffleService != null) {
            long minFloorMs = TimeUnit.SECONDS.toMillis(Math.min(2L, timeoutSecs));
            long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
            shuffleService.shutdownAndAwait(Math.max(minFloorMs, remainingMs), TimeUnit.MILLISECONDS);
        }

        StorageJanitor.stop();

        if (minioStorageService != null) {
            minioStorageService.close();
        }
        logger.info("ESS Daemon ordered shutdown complete.");
    }
}
