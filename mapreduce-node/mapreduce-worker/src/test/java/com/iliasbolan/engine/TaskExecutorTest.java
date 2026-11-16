package com.iliasbolan.engine;

import com.google.protobuf.ByteString;
import com.iliasbolan.grpc.shuffle.PartitionChunk;
import com.iliasbolan.grpc.shuffle.PartitionRequest;
import com.iliasbolan.grpc.shuffle.ShuffleServiceGrpc;
import com.iliasbolan.services.RabbitMqProducer;
import com.iliasbolan.services.S3ClientService;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InOrder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit test suite for the {@link TaskExecutor} orchestration engine covering Map/Reduce
 * execution, bounded parallel gRPC fetch pooling, channel reuse, full-jitter exponential
 * backoff, sibling cancellation, 30s heartbeats, {@code attemptId} threading, and
 * {@code SIGTERM} preemption escalation.
 *
 * @author Ilias Bolanakis
 * @version 3.0
 * @since 2026-04-25
 * @see TaskExecutor
 */
class TaskExecutorTest {

    private S3ClientService s3ClientService;
    private RabbitMqProducer eventProducer;
    private TaskExecutor taskExecutor;
    private final String nodeIp = "192.168.1.50";

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        s3ClientService = mock(S3ClientService.class);
        eventProducer = mock(RabbitMqProducer.class);
        taskExecutor = spy(new TaskExecutor(
                s3ClientService, tempDir.toString(), eventProducer, nodeIp,
                4, 3, 1L, 5L));
    }

    @AfterEach
    void tearDown() {
        taskExecutor.close();
    }

    @Test
    void testExecuteMapPhase_Success_SignalsCompletionWithAttemptId() throws Throwable {
        String jsonPayload = """
                {
                    "jobId": "job-123",
                    "taskId": "map-0",
                    "jobToken": "test-secure-token",
                    "taskType": "MAP",
                    "className": "com.example.WordCountMapper",
                    "userCodeBucket": "code",
                    "userCodeObject": "mapper.class",
                    "attemptId": 2
                }
                """;

        Process mockProcess = mock(Process.class);
        when(mockProcess.waitFor()).thenReturn(0);
        doReturn(mockProcess).when(taskExecutor).startSandboxProcess(any(ProcessBuilder.class));

        taskExecutor.executeTask(jsonPayload);

        org.mockito.ArgumentCaptor<ProcessBuilder> pbCaptor = org.mockito.ArgumentCaptor.forClass(ProcessBuilder.class);
        verify(taskExecutor).startSandboxProcess(pbCaptor.capture());
        List<String> jvmCmd = pbCaptor.getValue().command();
        assertTrue(jvmCmd.contains("-XX:+TieredCompilation"), "Sandbox JVM must enable TieredCompilation");
        assertTrue(jvmCmd.contains("-XX:CICompilerCount=2"), "Sandbox JVM must cap CICompilerCount=2");

        verify(s3ClientService).downloadUserCode(eq("code"), eq("mapper.class"), anyString());
        String expectedEssEndpoint = "unknown-node@@" + nodeIp + ":7337";

        verify(eventProducer).sendCompletionSignal(
                eq("job-123"),
                eq("map-0"),
                eq("test-secure-token"),
                eq("COMPLETED"),
                eq(expectedEssEndpoint),
                eq(2)
        );
    }

    @Test
    void testExecuteReducePhase_ShuffleFetchFailed_SendsErrorSignalAfterBoundedRetries() throws Throwable {
        String jsonPayload = """
                {
                    "jobId": "job-999",
                    "taskId": "0",
                    "jobToken": "error-auth-token",
                    "taskType": "REDUCE",
                    "className": "com.test.Reducer",
                    "workerEndpoints": ["10.244.2.15:7337"],
                    "attemptId": 3
                }
                """;

        ManagedChannel mockChannel = mock(ManagedChannel.class);
        when(mockChannel.shutdown()).thenReturn(mockChannel);
        StatusRuntimeException networkError = new StatusRuntimeException(
                Status.UNAVAILABLE.withDescription("ESS Node Offline"));

        doReturn(mockChannel).when(taskExecutor).createManagedChannel("10.244.2.15", 7337);
        doThrow(networkError).when(taskExecutor).createBlockingStub(mockChannel);

        taskExecutor.executeTask(jsonPayload);

        // 1 initial attempt + 3 retries = 4 total calls for UNAVAILABLE
        verify(taskExecutor, times(4)).createBlockingStub(mockChannel);

        verify(eventProducer).sendErrorSignal(
                eq("job-999"),
                eq("0"),
                eq("error-auth-token"),
                eq("FAILED"),
                eq("SHUFFLE_FETCH_FAILED_FROM_NODE:10.244.2.15:7337"),
                eq(3)
        );
    }

    @Test
    void testExecuteReducePhase_FailedPreconditionFailsImmediatelyWithoutRetryAndCancelsSiblings() throws Throwable {
        String jsonPayload = """
                {
                    "jobId": "job-fp",
                    "taskId": "0",
                    "jobToken": "tok-fp",
                    "taskType": "REDUCE",
                    "className": "com.test.Reducer",
                    "workerEndpoints": ["10.0.0.1:7337", "10.0.0.2:7337"],
                    "attemptId": 4
                }
                """;

        ManagedChannel failChannel = mock(ManagedChannel.class);
        ManagedChannel slowChannel = mock(ManagedChannel.class);
        when(failChannel.shutdown()).thenReturn(failChannel);
        when(slowChannel.shutdown()).thenReturn(slowChannel);

        CountDownLatch slowStarted = new CountDownLatch(1);
        CountDownLatch siblingCancelledLatch = new CountDownLatch(1);

        ShuffleServiceGrpc.ShuffleServiceBlockingStub failStub = mock(ShuffleServiceGrpc.ShuffleServiceBlockingStub.class);
        when(failStub.withDeadlineAfter(anyLong(), eq(TimeUnit.SECONDS))).thenReturn(failStub);
        when(failStub.withInterceptors(any(ClientInterceptor.class))).thenReturn(failStub);
        when(failStub.getPartition(any(PartitionRequest.class))).thenAnswer(inv -> {
            assertTrue(slowStarted.await(2, TimeUnit.SECONDS), "Slow sibling should be active before fail triggers");
            throw new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription("Degraded S3"));
        });

        ShuffleServiceGrpc.ShuffleServiceBlockingStub slowStub = mock(ShuffleServiceGrpc.ShuffleServiceBlockingStub.class);
        when(slowStub.withDeadlineAfter(anyLong(), eq(TimeUnit.SECONDS))).thenReturn(slowStub);
        when(slowStub.withInterceptors(any(ClientInterceptor.class))).thenReturn(slowStub);
        when(slowStub.getPartition(any(PartitionRequest.class))).thenAnswer(inv -> new Iterator<PartitionChunk>() {
            private int count = 0;

            @Override
            public boolean hasNext() {
                if (count == 0) return true;
                slowStarted.countDown();
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    siblingCancelledLatch.countDown();
                    Thread.currentThread().interrupt();
                }
                return false;
            }

            @Override
            public PartitionChunk next() {
                count++;
                return PartitionChunk.newBuilder()
                        .setContent(ByteString.copyFrom("partial", StandardCharsets.UTF_8))
                        .build();
            }
        });

        doReturn(failChannel).when(taskExecutor).createManagedChannel("10.0.0.1", 7337);
        doReturn(slowChannel).when(taskExecutor).createManagedChannel("10.0.0.2", 7337);
        doReturn(failStub).when(taskExecutor).createBlockingStub(failChannel);
        doReturn(slowStub).when(taskExecutor).createBlockingStub(slowChannel);

        taskExecutor.executeTask(jsonPayload);

        // FAILED_PRECONDITION must NOT be retried (only 1 call)
        verify(failStub, times(1)).getPartition(any(PartitionRequest.class));
        assertTrue(siblingCancelledLatch.await(2, TimeUnit.SECONDS),
                "Sibling fetch future must be immediately cancelled via cancel(true)");
        verify(eventProducer).sendErrorSignal(
                eq("job-fp"),
                eq("0"),
                eq("tok-fp"),
                eq("FAILED"),
                eq("SHUFFLE_FETCH_FAILED_FROM_NODE:10.0.0.1:7337"),
                eq(4)
        );
    }

    @Test
    void testExecuteReducePhase_TransientRetriesAndChannelReuseAcrossInvocations() throws Throwable {
        String jsonPayload = """
                {
                    "jobId": "job-retry",
                    "taskId": "0",
                    "jobToken": "tok-retry",
                    "taskType": "REDUCE",
                    "className": "com.test.Reducer",
                    "workerEndpoints": ["10.0.0.5:7337"],
                    "attemptId": 1
                }
                """;

        ManagedChannel mockChannel = mock(ManagedChannel.class);
        when(mockChannel.shutdown()).thenReturn(mockChannel);
        when(mockChannel.awaitTermination(anyLong(), any())).thenReturn(true);

        ShuffleServiceGrpc.ShuffleServiceBlockingStub mockStub = mock(ShuffleServiceGrpc.ShuffleServiceBlockingStub.class);
        when(mockStub.withDeadlineAfter(anyLong(), eq(TimeUnit.SECONDS))).thenReturn(mockStub);
        when(mockStub.withInterceptors(any(ClientInterceptor.class))).thenReturn(mockStub);

        AtomicInteger attemptCounter = new AtomicInteger(0);
        when(mockStub.getPartition(any(PartitionRequest.class))).thenAnswer(inv -> {
            int attempt = attemptCounter.getAndIncrement();
            if (attempt == 0) {
                throw new StatusRuntimeException(Status.RESOURCE_EXHAUSTED);
            }
            if (attempt == 1) {
                return new Iterator<PartitionChunk>() {
                    private int emitted = 0;

                    @Override
                    public boolean hasNext() {
                        if (emitted == 0) return true;
                        throw new StatusRuntimeException(Status.UNAVAILABLE);
                    }

                    @Override
                    public PartitionChunk next() {
                        emitted++;
                        return PartitionChunk.newBuilder()
                                .setContent(ByteString.copyFrom("corrupt-partial", StandardCharsets.UTF_8))
                                .build();
                    }
                };
            }
            return Collections.<PartitionChunk>emptyIterator();
        });

        Process mockProcess = mock(Process.class);
        when(mockProcess.waitFor()).thenReturn(0);

        doReturn(mockChannel).when(taskExecutor).createManagedChannel("10.0.0.5", 7337);
        doReturn(mockStub).when(taskExecutor).createBlockingStub(mockChannel);
        doReturn(mockProcess).when(taskExecutor).startSandboxProcess(any(ProcessBuilder.class));

        taskExecutor.executeTask(jsonPayload);
        taskExecutor.executeTask(jsonPayload);

        // ManagedChannel constructed ONCE and reused across retries & second task invocation
        verify(taskExecutor, times(1)).createManagedChannel("10.0.0.5", 7337);
        verify(mockStub, times(4)).getPartition(any(PartitionRequest.class));
        verify(eventProducer, times(2)).sendCompletionSignal(
                eq("job-retry"), eq("0"), eq("tok-retry"), eq("COMPLETED"), isNull(), eq(1));

        taskExecutor.close();
        verify(mockChannel).shutdown();
    }

    @Test
    void testExecuteReducePhase_ParallelFetchRespectsBoundedPoolConcurrency() throws Throwable {
        List<String> endpoints = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            endpoints.add("10.1.0." + i + ":7337");
        }
        String endpointsJson = endpoints.stream().map(e -> "\"" + e + "\"").collect(java.util.stream.Collectors.joining(","));
        String jsonPayload = String.format("""
                {
                    "jobId": "job-pool",
                    "taskId": "0",
                    "jobToken": "tok-pool",
                    "taskType": "REDUCE",
                    "className": "com.test.Reducer",
                    "workerEndpoints": [%s]
                }
                """, endpointsJson);

        Map<ManagedChannel, ShuffleServiceGrpc.ShuffleServiceBlockingStub> stubsByChannel = new ConcurrentHashMap<>();
        AtomicInteger active = new AtomicInteger(0);
        AtomicInteger peak = new AtomicInteger(0);

        doAnswer(inv -> {
            ManagedChannel ch = mock(ManagedChannel.class);
            when(ch.shutdown()).thenReturn(ch);
            ShuffleServiceGrpc.ShuffleServiceBlockingStub stub = mock(ShuffleServiceGrpc.ShuffleServiceBlockingStub.class);
            when(stub.withDeadlineAfter(anyLong(), eq(TimeUnit.SECONDS))).thenReturn(stub);
            when(stub.withInterceptors(any(ClientInterceptor.class))).thenReturn(stub);
            when(stub.getPartition(any(PartitionRequest.class))).thenAnswer(pInv -> {
                int now = active.incrementAndGet();
                peak.accumulateAndGet(now, Math::max);
                try {
                    Thread.sleep(40);
                } finally {
                    active.decrementAndGet();
                }
                return Collections.<PartitionChunk>emptyIterator();
            });
            stubsByChannel.put(ch, stub);
            return ch;
        }).when(taskExecutor).createManagedChannel(anyString(), anyInt());

        doAnswer(inv -> stubsByChannel.get(inv.getArgument(0)))
                .when(taskExecutor).createBlockingStub(any(ManagedChannel.class));

        Process mockProcess = mock(Process.class);
        when(mockProcess.waitFor()).thenReturn(0);
        doReturn(mockProcess).when(taskExecutor).startSandboxProcess(any(ProcessBuilder.class));

        taskExecutor.executeTask(jsonPayload);

        assertTrue(peak.get() >= 2 && peak.get() <= 4,
                "Bounded pool concurrency=4 should run >=2 in parallel and never exceed 4, observed peak=" + peak.get());
    }

    @Test
    void testExecuteReducePhase_AppliesGrpcDeadlineToBlockingStub() throws Throwable {
        String jsonPayload = """
                {
                    "jobId": "job-deadline",
                    "taskId": "0",
                    "jobToken": "deadline-token",
                    "taskType": "REDUCE",
                    "className": "com.test.Reducer",
                    "userCodeBucket": "code",
                    "userCodeObject": "reducer.class",
                    "workerEndpoints": ["10.244.2.15:7337"]
                }
                """;

        ManagedChannel mockChannel = mock(ManagedChannel.class);
        when(mockChannel.shutdown()).thenReturn(mockChannel);
        ShuffleServiceGrpc.ShuffleServiceBlockingStub mockStub = mock(ShuffleServiceGrpc.ShuffleServiceBlockingStub.class);
        Process mockProcess = mock(Process.class);
        when(mockProcess.waitFor()).thenReturn(0);

        doReturn(mockChannel).when(taskExecutor).createManagedChannel("10.244.2.15", 7337);
        doReturn(mockStub).when(taskExecutor).createBlockingStub(mockChannel);
        doReturn(mockProcess).when(taskExecutor).startSandboxProcess(any(ProcessBuilder.class));

        when(mockStub.withDeadlineAfter(anyLong(), eq(TimeUnit.SECONDS))).thenReturn(mockStub);
        when(mockStub.withInterceptors(any(ClientInterceptor.class))).thenReturn(mockStub);
        when(mockStub.getPartition(any(PartitionRequest.class))).thenReturn(Collections.<PartitionChunk>emptyIterator());

        taskExecutor.executeTask(jsonPayload);

        verify(mockStub).withDeadlineAfter(eq(TaskExecutor.resolveGrpcFetchDeadlineSeconds()), eq(TimeUnit.SECONDS));
        verify(mockStub).withInterceptors(any(ClientInterceptor.class));
        verify(eventProducer).sendCompletionSignal("job-deadline", "0", "deadline-token", "COMPLETED", null, 0);
    }

    @Test
    void testRunSandbox_Schedules30SecondHeartbeatsAndPreemptsWithDestroyEscalation() throws Throwable {
        String jsonPayload = """
                {
                    "jobId": "job-hb",
                    "taskId": "map-7",
                    "jobToken": "hb-token",
                    "taskType": "MAP",
                    "className": "com.example.Mapper",
                    "attemptId": 5
                }
                """;

        Path orphanTmp = tempDir.resolve("job-hb").resolve("0").resolve("map-7.attempt.5.tmp");
        Path otherAttemptTmp = tempDir.resolve("job-hb").resolve("0").resolve("map-7.attempt.50.tmp");
        Path siblingTaskTmp = tempDir.resolve("job-hb").resolve("0").resolve("map-8.attempt.1.tmp");
        Path otherTenantTmp = tempDir.resolve("other-job").resolve("0").resolve("map-0.attempt.1.tmp");
        Files.createDirectories(orphanTmp.getParent());
        Files.createDirectories(otherTenantTmp.getParent());
        Files.writeString(orphanTmp, "partial-active");
        Files.writeString(otherAttemptTmp, "partial-other-attempt");
        Files.writeString(siblingTaskTmp, "partial-sibling");
        Files.writeString(otherTenantTmp, "partial-other-tenant");

        ScheduledExecutorService mockScheduler = mock(ScheduledExecutorService.class);
        Process mockProcess = mock(Process.class);
        CountDownLatch processStarted = new CountDownLatch(1);
        CountDownLatch allowExit = new CountDownLatch(1);

        when(mockProcess.isAlive()).thenReturn(true);
        when(mockProcess.waitFor()).thenAnswer(inv -> {
            processStarted.countDown();
            allowExit.await(5, TimeUnit.SECONDS);
            return 143;
        });
        when(mockProcess.waitFor(50L, TimeUnit.MILLISECONDS)).thenReturn(false);
        when(mockProcess.waitFor(1_000L, TimeUnit.MILLISECONDS)).thenAnswer(inv -> {
            allowExit.countDown();
            return true;
        });

        doReturn(mockScheduler).when(taskExecutor).createHeartbeatScheduler();
        doReturn(mockProcess).when(taskExecutor).startSandboxProcess(any(ProcessBuilder.class));

        Thread execThread = new Thread(() -> {
            assertThrows(InterruptedException.class, () -> taskExecutor.executeTask(jsonPayload));
        });
        execThread.start();
        assertTrue(processStarted.await(2, TimeUnit.SECONDS));

        taskExecutor.preemptActiveTask(50L);
        execThread.join(2000);

        verify(mockScheduler).scheduleAtFixedRate(any(Runnable.class), eq(30L), eq(30L), eq(TimeUnit.SECONDS));
        InOrder order = inOrder(mockProcess);
        order.verify(mockProcess).destroy();
        order.verify(mockProcess).waitFor(50L, TimeUnit.MILLISECONDS);
        order.verify(mockProcess).destroyForcibly();
        assertTrue(taskExecutor.isPreempted());
        assertFalse(Files.exists(orphanTmp), "Active task's .tmp file must be deleted on preemption");
        assertTrue(Files.exists(otherAttemptTmp), "Other attemptId .tmp file must remain untouched");
        assertTrue(Files.exists(siblingTaskTmp), "Sibling task .tmp file in same job must remain untouched");
        assertTrue(Files.exists(otherTenantTmp), "Other tenant job .tmp file must remain untouched");

        // When idle (no active task), cleanupTempFiles must never touch shared baseShuffleDir files
        taskExecutor.cleanupTempFiles();
        assertTrue(Files.exists(siblingTaskTmp));
        assertTrue(Files.exists(otherTenantTmp));
    }

    @Test
    void testExecuteTask_SandboxCrash_ThrowsRuntimeException() throws Throwable {
        String jsonPayload = """
                {
                    "jobId": "job-fail",
                    "taskId": "map-002",
                    "jobToken": "fail-token",
                    "taskType": "MAP",
                    "className": "com.fail.Mapper"
                }
                """;

        Process mockProcess = mock(Process.class);
        when(mockProcess.waitFor()).thenReturn(1);
        doReturn(mockProcess).when(taskExecutor).startSandboxProcess(any(ProcessBuilder.class));

        RuntimeException exception = assertThrows(RuntimeException.class, () -> taskExecutor.executeTask(jsonPayload));
        assertTrue(exception.getMessage().contains("exit code: 1"));
    }

    @Test
    void testExecuteTask_UnknownTaskType_ThrowsIllegalArgumentException() {
        String jsonPayload = """
                {
                    "jobId": "job-invalid",
                    "taskType": "ILLEGAL_PHASE"
                }
                """;

        assertThrows(IllegalArgumentException.class, () -> taskExecutor.executeTask(jsonPayload));
    }

    @Test
    void testConfigParsersAndFullJitterBackoffBounds() {
        assertEquals(300L, TaskExecutor.parseGrpcFetchDeadlineSeconds(null));
        assertEquals(300L, TaskExecutor.parseGrpcFetchDeadlineSeconds(""));
        assertEquals(300L, TaskExecutor.parseGrpcFetchDeadlineSeconds("0"));
        assertEquals(300L, TaskExecutor.parseGrpcFetchDeadlineSeconds("-10"));
        assertEquals(300L, TaskExecutor.parseGrpcFetchDeadlineSeconds("not-a-number"));
        assertEquals(120L, TaskExecutor.parseGrpcFetchDeadlineSeconds("120"));

        assertEquals(8, TaskExecutor.parseReduceFetchConcurrency(null));
        assertEquals(8, TaskExecutor.parseReduceFetchConcurrency("invalid"));
        assertEquals(4, TaskExecutor.parseReduceFetchConcurrency("1"));
        assertEquals(6, TaskExecutor.parseReduceFetchConcurrency("6"));
        assertEquals(8, TaskExecutor.parseReduceFetchConcurrency("32"));

        for (int attempt = 0; attempt < 8; attempt++) {
            long maxExpected = Math.min(10_000L, 200L * (1L << attempt));
            long sleep = TaskExecutor.computeFullJitterBackoffMs(attempt, 200L, 10_000L);
            assertTrue(sleep >= 0L && sleep <= maxExpected,
                    "Jittered backoff out of [0, " + maxExpected + "]: " + sleep);
        }
    }
}
