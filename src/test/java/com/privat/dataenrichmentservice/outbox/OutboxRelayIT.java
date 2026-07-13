package com.privat.dataenrichmentservice.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.privat.dataenrichmentservice.TestcontainersConfiguration;
import com.privat.dataenrichmentservice.config.AppProperties;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
// Closed after the class so its live listeners cannot steal messages from later IT classes
// sharing the same broker container (each IT has a unique context anyway — nothing is re-cached).
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class OutboxRelayIT {

    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(10);

    private static final JsonMapper JSON = JsonMapper.builder().build();

    @DynamicPropertySource
    static void fastRelayProperties(DynamicPropertyRegistry registry) {
        registry.add("app.outbox.poll-interval", () -> "100ms");
        // A publish to a missing exchange has no confirm to wait for — keep the timeout short so
        // the poison-row test observes the attempts increment quickly.
        registry.add("app.outbox.confirm-timeout", () -> "1s");
    }

    @Autowired
    private OutboxRepository outboxRepository;

    @Autowired
    private OutboxRelay outboxRelay;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private AmqpAdmin amqpAdmin;

    @Autowired
    private AppProperties properties;

    @BeforeEach
    void cleanState() {
        outboxRepository.deleteAll();
        amqpAdmin.purgeQueue(properties.rabbit().resultQueue(), false);
    }

    @Test
    void pendingRowIsPublishedAndMarkedSent() {
        UUID messageId = UUID.randomUUID();
        String payload = "{\"logId\":1,\"messageId\":\"%s\",\"result\":true}".formatted(messageId);
        OutboxEntity row = outboxRepository.save(resultRow(messageId, payload));

        awaitMessageWithJson(payload);

        await().atMost(AWAIT_TIMEOUT).untilAsserted(() -> {
            OutboxEntity sent = outboxRepository.findById(row.getId()).orElseThrow();
            assertThat(sent.getStatus()).isEqualTo(OutboxStatus.SENT);
            assertThat(sent.getSentAt()).isNotNull();
        });
    }

    @Test
    void rowTargetingMissingExchangeStaysPendingAndRelaySurvives() {
        UUID poisonId = UUID.randomUUID();
        OutboxEntity poison = outboxRepository.save(OutboxEntity.builder()
                .messageId(poisonId)
                .exchange("no.such.exchange")
                .routingKey("whatever")
                .payload("{\"logId\":2,\"messageId\":\"%s\",\"result\":true}".formatted(poisonId))
                .build());

        await().atMost(AWAIT_TIMEOUT).untilAsserted(() -> {
            OutboxEntity reloaded = outboxRepository.findById(poison.getId()).orElseThrow();
            assertThat(reloaded.getStatus()).isEqualTo(OutboxStatus.PENDING);
            assertThat(reloaded.getAttempts()).isGreaterThanOrEqualTo(1);
        });

        // The scheduler must have survived the poison row: a healthy row still goes through.
        UUID healthyId = UUID.randomUUID();
        String healthyPayload = "{\"logId\":3,\"messageId\":\"%s\",\"result\":true}".formatted(healthyId);
        outboxRepository.save(resultRow(healthyId, healthyPayload));

        awaitMessageWithJson(healthyPayload);
    }

    @Test
    void concurrentDrainPublishesEachRowExactlyOnce() {
        List<OutboxEntity> rows = IntStream.range(0, 10)
                .mapToObj(i -> {
                    UUID messageId = UUID.randomUUID();
                    return resultRow(
                            messageId, "{\"logId\":%d,\"messageId\":\"%s\",\"result\":true}".formatted(i, messageId));
                })
                .toList();
        outboxRepository.saveAll(rows);

        // Two manual drains racing each other (and the scheduled ticks): SKIP LOCKED must ensure
        // every row is claimed and published by exactly one of them.
        CompletableFuture.allOf(
                        CompletableFuture.runAsync(outboxRelay::relayPendingBatch),
                        CompletableFuture.runAsync(outboxRelay::relayPendingBatch))
                .join();

        await().atMost(AWAIT_TIMEOUT).untilAsserted(() -> assertThat(outboxRepository.findAll())
                .allSatisfy(row -> assertThat(row.getStatus()).isEqualTo(OutboxStatus.SENT)));

        // Count deliveries per payload, ignoring messages from other cached test contexts whose
        // relays share this broker: each of OUR rows must arrive exactly once.
        Map<JsonNode, Integer> deliveredCounts = new HashMap<>();
        Message received;
        while ((received = rabbitTemplate.receive(properties.rabbit().resultQueue(), 1000)) != null) {
            deliveredCounts.merge(JSON.readTree(received.getBody()), 1, Integer::sum);
        }
        for (OutboxEntity row : rows) {
            assertThat(deliveredCounts.getOrDefault(JSON.readTree(row.getPayload()), 0))
                    .as("payload %s", row.getPayload())
                    .isEqualTo(1);
        }
    }

    /**
     * Receives from the shared result queue until a message carrying this JSON arrives. Compared
     * structurally, not byte-wise: the payload column is jsonb, so Postgres returns a canonical
     * rendering (different whitespace) of what was stored. Foreign messages published by relays of
     * other cached Spring test contexts are discarded instead of failing the assertion.
     */
    private void awaitMessageWithJson(String expectedJson) {
        JsonNode expected = JSON.readTree(expectedJson);
        await().atMost(AWAIT_TIMEOUT).until(() -> {
            Message message = rabbitTemplate.receive(properties.rabbit().resultQueue());
            return message != null && JSON.readTree(message.getBody()).equals(expected);
        });
    }

    private OutboxEntity resultRow(UUID messageId, String payload) {
        return OutboxEntity.builder()
                .messageId(messageId)
                .exchange(properties.rabbit().resultExchange())
                .routingKey(properties.rabbit().resultRoutingKey())
                .payload(payload)
                .build();
    }
}
