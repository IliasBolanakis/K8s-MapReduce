package com.iliasbolan;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link App} deterministic queue resolution ({@code JOB_COMPLETION_INDEX} /
 * {@code PARTITION_ID}) and {@code /health} readiness probe drain flipping.
 */
class AppTest {

    @Test
    void testResolveQueueConfig_IndexedReduceUsesJobCompletionIndex() {
        App.QueueConfig config = App.resolveQueueConfig(Map.of(
                "JOB_ID", "job-abc",
                "PHASE", "reduce",
                "JOB_COMPLETION_INDEX", "3"
        ));

        assertEquals("reduce_tasks_job-abc_3", config.taskQueue());
        assertTrue(config.singlePartitionMode());
    }

    @Test
    void testResolveQueueConfig_PartitionIdOverridesJobCompletionIndex() {
        App.QueueConfig config = App.resolveQueueConfig(Map.of(
                "JOB_ID", "job-abc",
                "PHASE", "REDUCE",
                "PARTITION_ID", "7",
                "JOB_COMPLETION_INDEX", "0"
        ));

        assertEquals("reduce_tasks_job-abc_7", config.taskQueue());
        assertTrue(config.singlePartitionMode());
    }

    @Test
    void testResolveQueueConfig_ReduceWithoutIndexFallsBackToSharedQueue() {
        App.QueueConfig config = App.resolveQueueConfig(Map.of(
                "JOB_ID", "job-abc",
                "PHASE", "reduce"
        ));

        assertEquals("reduce_tasks_job-abc", config.taskQueue());
        assertFalse(config.singlePartitionMode());
    }

    @Test
    void testResolveQueueConfig_MapAndUnsetJobIdPreserveLegacyQueues() {
        App.QueueConfig mapConfig = App.resolveQueueConfig(Map.of(
                "JOB_ID", "job-abc",
                "PHASE", "map",
                "JOB_COMPLETION_INDEX", "1"
        ));
        assertEquals("map_tasks_job-abc", mapConfig.taskQueue());
        assertFalse(mapConfig.singlePartitionMode());

        App.QueueConfig localFallback = App.resolveQueueConfig(Map.of(
                "RABBITMQ_QUEUE", "custom_local_queue"
        ));
        assertEquals("custom_local_queue", localFallback.taskQueue());
        assertFalse(localFallback.singlePartitionMode());
    }

    @Test
    void testHealthHandler_FlipsFrom200To503OnShutdownFlag() throws Exception {
        AtomicBoolean shuttingDown = new AtomicBoolean(false);
        HttpHandler handler = App.createHealthHandler(shuttingDown);

        ByteArrayOutputStream okBody = new ByteArrayOutputStream();
        HttpExchange okExchange = mock(HttpExchange.class);
        when(okExchange.getResponseBody()).thenReturn(okBody);
        when(okExchange.getResponseHeaders()).thenReturn(new Headers());
        when(okExchange.getRemoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 1234));

        handler.handle(okExchange);
        verify(okExchange).sendResponseHeaders(200, 2L);
        assertEquals("OK", okBody.toString(StandardCharsets.UTF_8));

        shuttingDown.set(true);

        ByteArrayOutputStream failBody = new ByteArrayOutputStream();
        HttpExchange failExchange = mock(HttpExchange.class);
        when(failExchange.getResponseBody()).thenReturn(failBody);

        handler.handle(failExchange);
        verify(failExchange).sendResponseHeaders(503, 11L);
        assertEquals("UNAVAILABLE", failBody.toString(StandardCharsets.UTF_8));
    }
}
