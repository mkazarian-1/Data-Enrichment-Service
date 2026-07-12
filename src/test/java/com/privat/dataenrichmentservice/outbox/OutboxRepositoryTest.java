package com.privat.dataenrichmentservice.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import com.privat.dataenrichmentservice.TestcontainersConfiguration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.context.annotation.Import;
import tools.jackson.databind.ObjectMapper;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(TestcontainersConfiguration.class)
class OutboxRepositoryTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Autowired
    private OutboxRepository repository;

    @Test
    void savesPayloadAsJsonb() {
        String payload =
                "{\"logId\": 654321, \"messageId\": \"123e4567-e89b-12d3-a456-426614174000\", \"result\": true}";
        OutboxEntity saved = repository.saveAndFlush(pendingRow(payload));

        assertThat(saved.getId()).isNotNull();
        assertThat(saved.getCreatedAt()).isNotNull();
        assertThat(saved.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(saved.getAttempts()).isZero();
        assertThat(saved.getSentAt()).isNull();

        OutboxEntity found = repository.findById(saved.getId()).orElseThrow();
        // jsonb normalizes formatting/key order — compare trees, not strings
        assertThat(JSON.readTree(found.getPayload())).isEqualTo(JSON.readTree(payload));
    }

    @Test
    void findPendingBatchHonorsLimitAndOrder() {
        OutboxEntity first = repository.save(pendingRow("{\"n\": 1}"));
        OutboxEntity second = repository.save(pendingRow("{\"n\": 2}"));
        OutboxEntity third = repository.save(pendingRow("{\"n\": 3}"));
        OutboxEntity sent = pendingRow("{\"n\": 4}");
        sent.setStatus(OutboxStatus.SENT);
        repository.save(sent);
        repository.flush();

        List<OutboxEntity> batch = repository.findPendingBatch(2);
        assertThat(batch).extracting(OutboxEntity::getId).containsExactly(first.getId(), second.getId());

        List<OutboxEntity> all = repository.findPendingBatch(10);
        assertThat(all)
                .extracting(OutboxEntity::getId)
                .containsExactly(first.getId(), second.getId(), third.getId())
                .doesNotContain(sent.getId());
    }

    private static OutboxEntity pendingRow(String payload) {
        return OutboxEntity.builder()
                .messageId(UUID.randomUUID())
                .exchange("enrichment.result.exchange")
                .routingKey("enrichment.result")
                .payload(payload)
                .build();
    }
}
