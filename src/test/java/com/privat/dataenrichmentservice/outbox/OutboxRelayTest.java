package com.privat.dataenrichmentservice.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.privat.dataenrichmentservice.config.AppProperties;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

@ExtendWith(MockitoExtension.class)
class OutboxRelayTest {

    private static final int BATCH_SIZE = 5;
    private static final String EXCHANGE = "enrichment.result.exchange";
    private static final String ROUTING_KEY = "enrichment.result";

    @Mock
    private OutboxRepository outboxRepository;

    @Mock
    private RabbitTemplate rabbitTemplate;

    private OutboxRelay relay;

    @BeforeEach
    void setUp() {
        AppProperties properties = new AppProperties(
                null, null, new AppProperties.Outbox(Duration.ofMillis(100), BATCH_SIZE, Duration.ofMillis(200)));
        relay = new OutboxRelay(outboxRepository, rabbitTemplate, properties);
    }

    @Test
    void publishesRawPayloadAndMarksSentOnAck() {
        OutboxEntity row = pendingRow();
        when(outboxRepository.findPendingBatch(BATCH_SIZE)).thenReturn(List.of(row));
        answerConfirm(true);

        relay.relayPendingBatch();

        ArgumentCaptor<Message> published = ArgumentCaptor.forClass(Message.class);
        verify(rabbitTemplate).send(eq(EXCHANGE), eq(ROUTING_KEY), published.capture(), any(CorrelationData.class));
        assertThat(published.getValue().getBody()).isEqualTo(row.getPayload().getBytes(StandardCharsets.UTF_8));
        assertThat(published.getValue().getMessageProperties().getContentType())
                .isEqualTo(MessageProperties.CONTENT_TYPE_JSON);

        assertThat(row.getStatus()).isEqualTo(OutboxStatus.SENT);
        assertThat(row.getSentAt()).isNotNull();
        assertThat(row.getAttempts()).isZero();
    }

    @Test
    void nackLeavesRowPendingAndIncrementsAttempts() {
        OutboxEntity row = pendingRow();
        when(outboxRepository.findPendingBatch(BATCH_SIZE)).thenReturn(List.of(row));
        answerConfirm(false);

        relay.relayPendingBatch();

        assertThat(row.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(row.getSentAt()).isNull();
        assertThat(row.getAttempts()).isEqualTo(1);
    }

    @Test
    void confirmTimeoutLeavesRowPending() {
        OutboxEntity row = pendingRow();
        when(outboxRepository.findPendingBatch(BATCH_SIZE)).thenReturn(List.of(row));
        // No answer stubbing: the confirm future is never completed, so the relay waits out its
        // 200 ms confirm timeout.

        relay.relayPendingBatch();

        assertThat(row.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(row.getAttempts()).isEqualTo(1);
    }

    @Test
    void unroutableReturnLeavesRowPending() {
        OutboxEntity row = pendingRow();
        when(outboxRepository.findPendingBatch(BATCH_SIZE)).thenReturn(List.of(row));
        doAnswer(invocation -> {
                    CorrelationData correlation = invocation.getArgument(3);
                    correlation.setReturned(new org.springframework.amqp.core.ReturnedMessage(
                            new Message(new byte[0]), 312, "NO_ROUTE", EXCHANGE, ROUTING_KEY));
                    correlation.getFuture().complete(new CorrelationData.Confirm(true, null));
                    return null;
                })
                .when(rabbitTemplate)
                .send(eq(EXCHANGE), eq(ROUTING_KEY), any(Message.class), any(CorrelationData.class));

        relay.relayPendingBatch();

        assertThat(row.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(row.getAttempts()).isEqualTo(1);
    }

    @Test
    void failingRowDoesNotAffectTheRestOfTheBatch() {
        OutboxEntity failing = pendingRow();
        OutboxEntity healthy = pendingRow();
        when(outboxRepository.findPendingBatch(BATCH_SIZE)).thenReturn(List.of(failing, healthy));
        doThrow(new AmqpException("channel closed"))
                .doAnswer(invocation -> {
                    CorrelationData correlation = invocation.getArgument(3);
                    correlation.getFuture().complete(new CorrelationData.Confirm(true, null));
                    return null;
                })
                .when(rabbitTemplate)
                .send(eq(EXCHANGE), eq(ROUTING_KEY), any(Message.class), any(CorrelationData.class));

        relay.relayPendingBatch();

        assertThat(failing.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(failing.getAttempts()).isEqualTo(1);
        assertThat(healthy.getStatus()).isEqualTo(OutboxStatus.SENT);
        assertThat(healthy.getSentAt()).isNotNull();
    }

    @Test
    void fetchesBatchWithConfiguredSize() {
        when(outboxRepository.findPendingBatch(BATCH_SIZE)).thenReturn(List.of());

        relay.relayPendingBatch();

        verify(outboxRepository).findPendingBatch(BATCH_SIZE);
    }

    private void answerConfirm(boolean ack) {
        doAnswer(invocation -> {
                    CorrelationData correlation = invocation.getArgument(3);
                    correlation.getFuture().complete(new CorrelationData.Confirm(ack, ack ? null : "no space"));
                    return null;
                })
                .when(rabbitTemplate)
                .send(eq(EXCHANGE), eq(ROUTING_KEY), any(Message.class), any(CorrelationData.class));
    }

    private static OutboxEntity pendingRow() {
        return OutboxEntity.builder()
                .id(1L)
                .messageId(UUID.randomUUID())
                .exchange(EXCHANGE)
                .routingKey(ROUTING_KEY)
                .payload("{\"logId\":42,\"messageId\":\"2b6a72cd-0f36-4e1a-9b4c-6f2f3a1d8e57\",\"result\":true}")
                .build();
    }
}
