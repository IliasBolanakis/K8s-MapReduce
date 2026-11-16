package com.iliasbolan.services;

import com.iliasbolan.engine.TaskExecutor;
import com.iliasbolan.infrastructure.RabbitMqConnectionManager;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Envelope;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit test suite for the {@link RabbitMqConsumer} control plane component.
 * <p>
 * This suite provides rigorous validation for the Worker node's primary event consumer.
 * It ensures that the worker correctly interfaces with the RabbitMQ broker,
 * manages task execution lifecycles, and maintains robust state reporting
 * back to the Orchestrator.
 * </p>
 * <p>
 * <b>Security & Signal Verification (Architecture v2.1):</b><br>
 * These tests specifically verify the <b>Zero-Trust security model</b> by ensuring
 * the consumer correctly extracts HMAC <code>jobToken</code> payloads and echoes
 * them in error signals. Note: Successful completion signaling is now exclusively
 * delegated to the {@link TaskExecutor} to prevent duplicate network events.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 2.2
 * @since 2026-04-25
 * @see com.iliasbolan.services.RabbitMqConsumer
 */
class RabbitMqConsumerTest {

    private RabbitMqConnectionManager mockManager;
    private Connection mockConnection;
    private Channel mockChannel;
    private TaskExecutor mockTaskExecutor;
    private RabbitMqProducer mockEventProducer;

    /**
     * Initializes the testing environment by creating mock objects for all
     * external infrastructure and engine dependencies.
     *
     * @throws Exception If mock setup or initial stubbing fails.
     */
    @BeforeEach
    void setUp() throws Exception {
        mockManager = Mockito.mock(RabbitMqConnectionManager.class);
        mockConnection = Mockito.mock(Connection.class);
        mockChannel = Mockito.mock(Channel.class);
        mockTaskExecutor = Mockito.mock(TaskExecutor.class);
        mockEventProducer = Mockito.mock(RabbitMqProducer.class);

        // Stubbing the connection factory chain for RabbitMQ
        when(mockManager.createConnection()).thenReturn(mockConnection);
        when(mockConnection.createChannel()).thenReturn(mockChannel);
    }

    /**
     * Validates that the consumer's idle watchdog correctly identifies inactivity
     * and triggers a graceful shutdown of the main execution thread.
     *
     * @throws Throwable If the internal consumer loop encounters an unhandled exception.
     */
    @Test
    void testStartConsuming_IdleTimeout_TriggersGracefulPodShutdown() throws Throwable {
        RabbitMqConsumer consumer = new RabbitMqConsumer(mockManager, "test-queue", 500, mockTaskExecutor, mockEventProducer);
        long startTime = System.currentTimeMillis();

        // Execute blocking consumption loop with a short timeout
        consumer.startConsuming();

        long duration = System.currentTimeMillis() - startTime;

        // Assert: Ensure the inactivity checker terminated the process within the threshold
        assertTrue(duration >= 500 && duration < 2500, "Idle checker failed to terminate within expected window.");
    }

    /**
     * Verifies the "Happy Path" lifecycle: successful message delivery, task execution,
     * and positive AMQP acknowledgment.
     *
     * @throws Throwable If thread management or mock verifications fail.
     */
    @Test
    @SuppressWarnings("unchecked")
    void testHandleDelivery_SuccessfulTaskExecution_IssuesPositiveAck() throws Throwable {
        RabbitMqConsumer consumer = new RabbitMqConsumer(mockManager, "test-queue", 5000, mockTaskExecutor, mockEventProducer);

        // Launch consumer in a background thread to simulate asynchronous delivery
        Thread consumerThread = new Thread(() -> {
            try { consumer.startConsuming(); } catch (Exception ignored) {}
        });
        consumerThread.start();
        Thread.sleep(200); // Allow thread to settle

        // Capture the deliver callback registered with the RabbitMQ channel
        ArgumentCaptor<DeliverCallback> callbackCaptor = ArgumentCaptor.forClass(DeliverCallback.class);
        verify(mockChannel).basicConsume(eq("test-queue"), eq(false), callbackCaptor.capture(), any(CancelCallback.class));

        DeliverCallback internalCallback = callbackCaptor.getValue();

        long deliveryTag = 12345L;
        Envelope envelope = new Envelope(deliveryTag, false, "exchange", "routingKey");
        // Simulate a secure task payload including the jobToken
        byte[] body = "{\"jobId\":\"job-001\", \"taskId\":\"reduce-0\", \"jobToken\":\"token-123\", \"taskType\":\"REDUCE\"}"
                .getBytes(StandardCharsets.UTF_8);

        // Act: Manually trigger a message delivery event
        internalCallback.handle("tag", new com.rabbitmq.client.Delivery(envelope, new AMQP.BasicProperties(), body));

        // Verify engine dispatch and protocol ACKs
        verify(mockTaskExecutor).executeTask(anyString());
        verify(mockChannel).basicAck(eq(deliveryTag), eq(false));

        // Note: We no longer verify sendCompletionSignal here because TaskExecutor handles it now.
        verify(mockEventProducer, never()).sendCompletionSignal(anyString(), anyString(), anyString(), anyString(), anyString());

        consumer.stopConsuming();
        consumerThread.join();
    }

    /**
     * Validates error handling logic when a task fails during execution.
     * Ensures that a FAILED signal is emitted to the Manager and the message is
     * NACKed (requeue=false) without any Node-side basicPublish retry loop.
     *
     * @throws Throwable If the mock execution fails to throw as expected.
     */
    @Test
    @SuppressWarnings("unchecked")
    void testHandleDelivery_TaskExecutionFailure_SignalsFailureAndNacksWithoutRepublish() throws Throwable {
        RabbitMqConsumer consumer = new RabbitMqConsumer(mockManager, "test-queue", 5000, mockTaskExecutor, mockEventProducer);
        // Simulate a transient computational failure
        doThrow(new RuntimeException("I/O Error")).when(mockTaskExecutor).executeTask(anyString());

        Thread consumerThread = new Thread(() -> {
            try { consumer.startConsuming(); } catch (Exception ignored) {}
        });
        consumerThread.start();
        Thread.sleep(200);

        ArgumentCaptor<DeliverCallback> callbackCaptor = ArgumentCaptor.forClass(DeliverCallback.class);
        verify(mockChannel).basicConsume(eq("test-queue"), eq(false), callbackCaptor.capture(), any(CancelCallback.class));

        DeliverCallback internalCallback = callbackCaptor.getValue();

        long deliveryTag = 9999L;
        Envelope envelope = new Envelope(deliveryTag, false, "exchange", "routingKey");
        byte[] body = "{\"jobId\":\"job-002\", \"taskId\":\"map-1\", \"jobToken\":\"token-456\"}"
                .getBytes(StandardCharsets.UTF_8);

        internalCallback.handle("tag", new com.rabbitmq.client.Delivery(envelope, new AMQP.BasicProperties(), body));

        // Verify error signaling includes diagnostic information, auth token, and attemptId
        verify(mockEventProducer).sendErrorSignal(
                eq("job-002"),
                eq("map-1"),
                eq("token-456"),
                eq("FAILED"),
                eq("RuntimeException"),
                eq(0)
        );

        // Verify Manager owns retries: Node NACKs (requeue=false) and never republishes or ACKs
        verify(mockChannel).basicNack(eq(deliveryTag), eq(false), eq(false));
        verify(mockChannel, never()).basicPublish(anyString(), anyString(), any(), any());
        verify(mockChannel, never()).basicAck(anyLong(), anyBoolean());

        consumer.stopConsuming();
        consumerThread.join();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testHandleDelivery_TaskFailureWithSignalError_StillNacksWithoutRepublish() throws Throwable {
        RabbitMqConsumer consumer = new RabbitMqConsumer(mockManager, "test-queue", 5000, mockTaskExecutor, mockEventProducer);
        doThrow(new RuntimeException("Task Error")).when(mockTaskExecutor).executeTask(anyString());
        doThrow(new RuntimeException("Event Queue Down")).when(mockEventProducer)
                .sendErrorSignal(anyString(), anyString(), anyString(), anyString(), anyString(), anyInt());

        Thread consumerThread = new Thread(() -> {
            try { consumer.startConsuming(); } catch (Exception ignored) {}
        });
        consumerThread.start();
        Thread.sleep(200);

        ArgumentCaptor<DeliverCallback> callbackCaptor = ArgumentCaptor.forClass(DeliverCallback.class);
        verify(mockChannel).basicConsume(eq("test-queue"), eq(false), callbackCaptor.capture(), any(CancelCallback.class));

        long deliveryTag = 7777L;
        Envelope envelope = new Envelope(deliveryTag, false, "exchange", "routingKey");
        byte[] body = "{\"jobId\":\"job-003\", \"taskId\":\"map-3\", \"jobToken\":\"token-999\"}"
                .getBytes(StandardCharsets.UTF_8);

        callbackCaptor.getValue().handle("tag", new com.rabbitmq.client.Delivery(envelope, new AMQP.BasicProperties(), body));

        verify(mockChannel).basicNack(eq(deliveryTag), eq(false), eq(false));
        verify(mockChannel, never()).basicPublish(anyString(), anyString(), any(), any());
        verify(mockChannel, never()).basicAck(anyLong(), anyBoolean());

        consumer.stopConsuming();
        consumerThread.join();
    }

    /**
     * Validates the <b>Poison Pill Protection</b> protocol. Ensures that messages
     * exceeding the retry threshold are discarded (NACKed without requeue) and
     * reported as failures to prevent infinite execution loops.
     *
     * @throws Throwable If internal messaging logic fails.
     */
    @Test
    @SuppressWarnings("unchecked")
    void testHandleDelivery_MaxRetriesExceeded_SignalsFailureAndDropsMessage() throws Throwable {
        RabbitMqConsumer consumer = new RabbitMqConsumer(mockManager, "test-queue", 5000, mockTaskExecutor, mockEventProducer);

        Thread consumerThread = new Thread(() -> {
            try { consumer.startConsuming(); } catch (Exception ignored) {}
        });
        consumerThread.start();
        Thread.sleep(200);

        ArgumentCaptor<DeliverCallback> callbackCaptor = ArgumentCaptor.forClass(DeliverCallback.class);
        verify(mockChannel).basicConsume(eq("test-queue"), eq(false), callbackCaptor.capture(), any(CancelCallback.class));

        DeliverCallback internalCallback = callbackCaptor.getValue();

        long deliveryTag = 8888L;
        Envelope envelope = new Envelope(deliveryTag, false, "exchange", "routingKey");
        byte[] body = "{\"jobId\":\"poison-pill\", \"taskId\":\"map-2\", \"jobToken\":\"token-789\", \"attemptId\":3}"
                .getBytes(StandardCharsets.UTF_8);

        // --- FIX: Inject the NEW custom header instead of the old quorum queue header ---
        java.util.Map<String, Object> headers = new java.util.HashMap<>();
        headers.put("x-custom-retry-count", 5);
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder().headers(headers).build();
        // ------------------------------------------------------------------------------

        internalCallback.handle("tag", new com.rabbitmq.client.Delivery(envelope, properties, body));

        // Verify NACK with requeue=false to discard the poison pill to the DLX
        verify(mockChannel).basicNack(eq(deliveryTag), eq(false), eq(false));

        // Verify terminal failure signal includes attemptId
        verify(mockEventProducer).sendErrorSignal(
                eq("poison-pill"),
                eq("map-2"),
                eq("token-789"),
                eq("FAILED"),
                eq("MAX_RETRIES_EXCEEDED"),
                eq(3)
        );

        // Ensure the engine was never invoked for a poison pill
        verify(mockTaskExecutor, never()).executeTask(anyString());

        consumer.stopConsuming();
        consumerThread.join();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testSinglePartitionMode_ExitsImmediatelyAfterAckWithoutIdleWait() throws Throwable {
        when(mockChannel.basicConsume(eq("reduce_tasks_job-1_0"), eq(false), any(DeliverCallback.class), any(CancelCallback.class)))
                .thenReturn("ctag-0");

        RabbitMqConsumer consumer = new RabbitMqConsumer(
                mockManager, "reduce_tasks_job-1_0", 10_000, mockTaskExecutor, mockEventProducer, true);

        Thread consumerThread = new Thread(() -> {
            try { consumer.startConsuming(); } catch (Exception ignored) {}
        });
        long startMs = System.currentTimeMillis();
        consumerThread.start();
        Thread.sleep(150);

        ArgumentCaptor<DeliverCallback> callbackCaptor = ArgumentCaptor.forClass(DeliverCallback.class);
        verify(mockChannel).basicConsume(eq("reduce_tasks_job-1_0"), eq(false), callbackCaptor.capture(), any(CancelCallback.class));

        long deliveryTag = 501L;
        Envelope envelope = new Envelope(deliveryTag, false, "ex", "rk");
        byte[] body = "{\"jobId\":\"job-1\", \"taskId\":\"0\", \"jobToken\":\"tok\", \"taskType\":\"REDUCE\", \"attemptId\":2}"
                .getBytes(StandardCharsets.UTF_8);

        callbackCaptor.getValue().handle("ctag-0", new com.rabbitmq.client.Delivery(envelope, new AMQP.BasicProperties(), body));

        consumerThread.join(2000);
        long elapsedMs = System.currentTimeMillis() - startMs;

        assertFalse(consumerThread.isAlive(), "Single-partition consumer must exit right after ACK");
        assertTrue(elapsedMs < 3000, "Must exit well before 10s IDLE_TIMEOUT_MILLIS, took: " + elapsedMs + "ms");
        verify(mockChannel).basicAck(deliveryTag, false);
        verify(mockChannel).basicCancel("ctag-0");
    }

    @Test
    @SuppressWarnings("unchecked")
    void testGracefulDrain_PreemptsStuckTaskAndNacksWithRequeueTrueBeforeChannelClose() throws Throwable {
        when(mockChannel.basicConsume(eq("map_tasks_job-drain"), eq(false), any(DeliverCallback.class), any(CancelCallback.class)))
                .thenReturn("ctag-drain");

        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch allowTaskThrow = new CountDownLatch(1);

        doAnswer(inv -> {
            taskStarted.countDown();
            allowTaskThrow.await(5, TimeUnit.SECONDS);
            throw new InterruptedException("Preempted");
        }).when(mockTaskExecutor).executeTask(anyString());

        when(mockTaskExecutor.isPreempted()).thenReturn(true);
        doAnswer(inv -> {
            allowTaskThrow.countDown();
            return null;
        }).when(mockTaskExecutor).preemptActiveTask(50L);

        RabbitMqConsumer consumer = new RabbitMqConsumer(
                mockManager, "map_tasks_job-drain", 10_000, mockTaskExecutor, mockEventProducer, false);

        Thread consumerThread = new Thread(() -> {
            try { consumer.startConsuming(); } catch (Exception ignored) {}
        });
        consumerThread.start();
        Thread.sleep(150);

        ArgumentCaptor<DeliverCallback> callbackCaptor = ArgumentCaptor.forClass(DeliverCallback.class);
        verify(mockChannel).basicConsume(eq("map_tasks_job-drain"), eq(false), callbackCaptor.capture(), any(CancelCallback.class));

        long deliveryTag = 777L;
        Envelope envelope = new Envelope(deliveryTag, false, "ex", "rk");
        byte[] body = "{\"jobId\":\"job-drain\", \"taskId\":\"map-0\", \"jobToken\":\"tok\", \"taskType\":\"MAP\", \"attemptId\":2}"
                .getBytes(StandardCharsets.UTF_8);

        Thread deliveryThread = new Thread(() -> {
            try {
                callbackCaptor.getValue().handle("ctag-drain", new com.rabbitmq.client.Delivery(envelope, new AMQP.BasicProperties(), body));
            } catch (Exception ignored) {}
        });
        deliveryThread.start();
        assertTrue(taskStarted.await(2, TimeUnit.SECONDS));

        consumer.initiateGracefulDrain(50L, 50L);
        deliveryThread.join(2000);
        consumerThread.join(2000);

        InOrder inOrder = inOrder(mockChannel, mockTaskExecutor);
        inOrder.verify(mockChannel).basicCancel("ctag-drain");
        inOrder.verify(mockTaskExecutor).preemptActiveTask(50L);
        inOrder.verify(mockChannel).basicNack(deliveryTag, false, true);
        inOrder.verify(mockChannel).close();

        verify(mockEventProducer, never()).sendErrorSignal(anyString(), anyString(), anyString(), anyString(), anyString(), anyInt());
    }
}