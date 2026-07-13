package com.privat.dataenrichmentservice.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

import com.privat.dataenrichmentservice.messaging.dto.ResultMessage;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.json.JsonMapper;

@ExtendWith(MockitoExtension.class)
class OutboxWriterTest {

    private static final UUID MESSAGE_ID = UUID.fromString("2b6a72cd-0f36-4e1a-9b4c-6f2f3a1d8e57");

    @Mock
    private OutboxRepository outboxRepository;

    @Test
    void serializesPayloadAndSavesPendingRow() {
        OutboxWriter writer =
                new OutboxWriter(outboxRepository, JsonMapper.builder().build());

        writer.enqueue(
                MESSAGE_ID,
                "enrichment.result.exchange",
                "enrichment.result",
                new ResultMessage(42L, MESSAGE_ID, true));

        ArgumentCaptor<OutboxEntity> savedEntity = ArgumentCaptor.forClass(OutboxEntity.class);
        verify(outboxRepository).save(savedEntity.capture());

        OutboxEntity entity = savedEntity.getValue();
        assertThat(entity.getMessageId()).isEqualTo(MESSAGE_ID);
        assertThat(entity.getExchange()).isEqualTo("enrichment.result.exchange");
        assertThat(entity.getRoutingKey()).isEqualTo("enrichment.result");
        assertThat(entity.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(entity.getAttempts()).isZero();
        assertThat(entity.getPayload())
                .isEqualTo("{\"logId\":42,\"messageId\":\"" + MESSAGE_ID + "\",\"result\":true}");
    }
}
