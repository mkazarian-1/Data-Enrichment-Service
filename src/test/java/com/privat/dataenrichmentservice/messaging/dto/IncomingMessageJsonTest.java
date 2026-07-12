package com.privat.dataenrichmentservice.messaging.dto;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class IncomingMessageJsonTest {

    private final JsonMapper mapper = JsonMapper.builder().build();

    @Test
    void deserializesExactPrdSample() {
        String json =
                """
                {
                  "messageId": "123e4567-e89b-12d3-a456-426614174000",
                  "userId": 12345678,
                  "action": "request",
                  "timestamp": "2026-07-01 10:00:00.0"
                }
                """;

        IncomingMessage message = mapper.readValue(json, IncomingMessage.class);

        assertThat(message.messageId()).isEqualTo(UUID.fromString("123e4567-e89b-12d3-a456-426614174000"));
        assertThat(message.userId()).isEqualTo(12345678L);
        assertThat(message.action()).isEqualTo("request");
        assertThat(message.timestamp()).isEqualTo(LocalDateTime.of(2026, 7, 1, 10, 0, 0));
    }

    @Test
    void missingFieldsSurfaceAsNullsForValidation() {
        IncomingMessage message = mapper.readValue("{\"action\": \"request\"}", IncomingMessage.class);

        assertThat(message.messageId()).isNull();
        assertThat(message.userId()).isNull();
        assertThat(message.timestamp()).isNull();
        assertThat(message.action()).isEqualTo("request");
    }
}
