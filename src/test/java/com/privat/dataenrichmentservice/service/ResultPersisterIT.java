package com.privat.dataenrichmentservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;

import com.privat.dataenrichmentservice.TestcontainersConfiguration;
import com.privat.dataenrichmentservice.client.dto.EnrichmentResponse;
import com.privat.dataenrichmentservice.messaging.dto.IncomingMessage;
import com.privat.dataenrichmentservice.outbox.OutboxEntity;
import com.privat.dataenrichmentservice.outbox.OutboxWriter;
import com.privat.dataenrichmentservice.persistence.ResultRepository;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ResultPersisterIT {

    @Autowired
    private ResultPersister resultPersister;

    @Autowired
    private ResultRepository resultRepository;

    @MockitoBean
    private OutboxWriter outboxWriter;

    @Test
    void outboxFailureRollsBackResultInsert() {
        IncomingMessage message = incomingMessage();
        doThrow(new RuntimeException("outbox write failed")).when(outboxWriter).enqueue(any(), any(), any(), any());

        assertThatThrownBy(() -> resultPersister.persist(message, new EnrichmentResponse(message.userId(), true)))
                .isInstanceOf(RuntimeException.class);

        assertThat(resultRepository.existsByMessageId(message.messageId())).isFalse();
    }

    @Test
    void persistRunsInsideAnActiveTransaction() {
        IncomingMessage message = incomingMessage();
        AtomicBoolean transactionActiveDuringEnqueue = new AtomicBoolean(false);
        doAnswer(invocation -> {
                    transactionActiveDuringEnqueue.set(TransactionSynchronizationManager.isActualTransactionActive());
                    return OutboxEntity.builder()
                            .messageId(message.messageId())
                            .exchange(invocation.getArgument(1))
                            .routingKey(invocation.getArgument(2))
                            .payload("{}")
                            .build();
                })
                .when(outboxWriter)
                .enqueue(any(), any(), any(), any());

        assertThat(resultPersister.persist(message, new EnrichmentResponse(message.userId(), true)))
                .isPresent();

        assertThat(transactionActiveDuringEnqueue).isTrue();
        assertThat(resultRepository.existsByMessageId(message.messageId())).isTrue();
    }

    private static IncomingMessage incomingMessage() {
        return new IncomingMessage(UUID.randomUUID(), 12345678L, "request", LocalDateTime.of(2026, 7, 1, 10, 0));
    }
}
