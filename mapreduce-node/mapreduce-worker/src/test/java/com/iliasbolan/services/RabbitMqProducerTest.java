package com.iliasbolan.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iliasbolan.infrastructure.RabbitMqConnectionManager;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for persistent publisher-confirm {@link RabbitMqProducer} signaling,
 * channel/connection recovery, and {@code attemptId} payload propagation.
 */
class RabbitMqProducerTest {

    private RabbitMqConnectionManager mockManager;
    private Connection mockConnection;
    private Channel mockChannel;
    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setUp() throws Exception {
        mockManager = mock(RabbitMqConnectionManager.class);
        mockConnection = mock(Connection.class);
        mockChannel = mock(Channel.class);

        when(mockManager.createConnection()).thenReturn(mockConnection);
        when(mockConnection.isOpen()).thenReturn(true);
        when(mockConnection.createChannel()).thenReturn(mockChannel);
        when(mockChannel.isOpen()).thenReturn(true);
    }

    @Test
    void testPersistentChannelReuse_AndPublisherConfirmsAcrossAllSignalTypes() throws Exception {
        try (RabbitMqProducer producer = new RabbitMqProducer(mockManager, "job_events_queue")) {
            producer.sendProgressSignal("job-1", "map-0", "tok-1", 2);
            producer.sendCompletionSignal("job-1", "map-0", "tok-1", "COMPLETED", "node1@@10.0.0.1:7337", 2);
            producer.sendErrorSignal("job-1", "0", "tok-1", "FAILED", "SHUFFLE_FETCH_FAILED_FROM_NODE:10.0.0.1:7337", 3);
        }

        // Single connection and channel created and reused across all 3 publishes
        verify(mockManager, times(1)).createConnection();
        verify(mockConnection, times(1)).createChannel();
        verify(mockChannel, times(1)).confirmSelect();
        verify(mockChannel, times(3)).waitForConfirmsOrDie(RabbitMqProducer.CONFIRM_TIMEOUT_MS);

        ArgumentCaptor<byte[]> bodyCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(mockChannel, times(3)).basicPublish(eq(""), eq("job_events_queue"), isNull(), bodyCaptor.capture());

        JsonNode progressJson = mapper.readTree(bodyCaptor.getAllValues().get(0));
        assertEquals("IN_PROGRESS", progressJson.get("status").asText());
        assertEquals(2, progressJson.get("attemptId").asInt());

        JsonNode completedJson = mapper.readTree(bodyCaptor.getAllValues().get(1));
        assertEquals("COMPLETED", completedJson.get("status").asText());
        assertEquals("node1@@10.0.0.1:7337", completedJson.get("workerBindAddress").asText());
        assertEquals(2, completedJson.get("attemptId").asInt());

        JsonNode errorJson = mapper.readTree(bodyCaptor.getAllValues().get(2));
        assertEquals("FAILED", errorJson.get("status").asText());
        assertEquals(3, errorJson.get("attemptId").asInt());

        // AutoCloseable closes both channel and connection cleanly
        verify(mockChannel).close();
        verify(mockConnection).close();
    }

    @Test
    void testChannelRecovery_RecreatesBrokenChannelAndRetriesConfirm() throws Exception {
        Channel brokenChannel = mock(Channel.class);
        Channel healthyChannel = mock(Channel.class);
        when(brokenChannel.isOpen()).thenReturn(true);
        when(healthyChannel.isOpen()).thenReturn(true);
        doThrow(new IOException("Channel closed mid-confirm"))
                .when(brokenChannel).waitForConfirmsOrDie(anyLong());

        when(mockConnection.createChannel()).thenReturn(brokenChannel, healthyChannel);

        try (RabbitMqProducer producer = new RabbitMqProducer(mockManager, "job_events_queue")) {
            producer.sendCompletionSignal("job-2", "1", "tok-2", "COMPLETED", null, 4);
        }

        verify(brokenChannel).confirmSelect();
        verify(healthyChannel).confirmSelect();
        verify(healthyChannel).basicPublish(eq(""), eq("job_events_queue"), isNull(), any(byte[].class));
        verify(healthyChannel).waitForConfirmsOrDie(RabbitMqProducer.CONFIRM_TIMEOUT_MS);
    }
}
