package com.privat.dataenrichmentservice;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.privat.dataenrichmentservice.config.AppProperties;
import com.privat.dataenrichmentservice.outbox.OutboxRepository;
import com.privat.dataenrichmentservice.persistence.ResultEntity;
import com.privat.dataenrichmentservice.persistence.ResultRepository;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
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
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class EndToEndFlowIT {

    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(15);
    private static final long GRACE_RECEIVE_MILLIS = 1500;
    private static final long USER_ID = 12345678L;

    private static final JsonMapper JSON = JsonMapper.builder().build();

    private static final WireMockServer WIRE_MOCK = new WireMockServer(options().dynamicPort());

    @DynamicPropertySource
    static void testProperties(DynamicPropertyRegistry registry) {
        WIRE_MOCK.start();
        registry.add("app.enrichment.base-url", WIRE_MOCK::baseUrl);
        registry.add("app.rabbit.retry.initial-interval", () -> "50ms");
        registry.add("app.rabbit.retry.max-interval", () -> "200ms");
        registry.add("app.outbox.poll-interval", () -> "100ms");
    }

    @AfterAll
    static void stopWireMock() {
        WIRE_MOCK.stop();
    }

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private AmqpAdmin amqpAdmin;

    @Autowired
    private AppProperties properties;

    @Autowired
    private ResultRepository resultRepository;

    @Autowired
    private OutboxRepository outboxRepository;

    @BeforeEach
    void cleanState() {
        WIRE_MOCK.resetAll();
        amqpAdmin.purgeQueue(properties.rabbit().incomingQueue(), false);
        amqpAdmin.purgeQueue(properties.rabbit().dlq(), false);
        amqpAdmin.purgeQueue(properties.rabbit().resultQueue(), false);
        outboxRepository.deleteAll();
        resultRepository.deleteAll();
    }

    @Test
    void happyPathPersistsResultAndPublishesExactContract() {
        stubEnrichmentSuccess();
        UUID messageId = UUID.randomUUID();

        publishIncoming(incomingJson(messageId));

        ResultEntity row = awaitResultRow(messageId);
        assertThat(row.getUserId()).isEqualTo(USER_ID);
        assertThat(row.getAction()).isEqualTo("request");
        assertThat(row.isResult()).isTrue();
        assertThat(row.getCreatedAt()).isNotNull();

        awaitResultEvent(expectedEvent(row.getId(), messageId));
    }

    @Test
    void duplicateMessageProducesExactlyOneRowAndOneEvent() {
        stubEnrichmentSuccess();
        UUID messageId = UUID.randomUUID();

        publishIncoming(incomingJson(messageId));
        publishIncoming(incomingJson(messageId));

        await().atMost(AWAIT_TIMEOUT).untilAsserted(() -> WIRE_MOCK.verify(2, postRequestedFor(urlEqualTo("/enrich"))));

        ResultEntity row = awaitResultRow(messageId);
        assertThat(resultRepository.count()).isEqualTo(1);
        assertThat(outboxRepository.count()).isEqualTo(1);

        awaitResultEvent(expectedEvent(row.getId(), messageId));
        assertThat(rabbitTemplate.receive(properties.rabbit().resultQueue(), GRACE_RECEIVE_MILLIS))
                .as("no second outgoing event within the grace window")
                .isNull();
    }

    @Test
    void enrichmentFailureLeavesNoPartialEffectsAndDeadLetters() {
        WIRE_MOCK.stubFor(post("/enrich").willReturn(aResponse().withStatus(500)));
        UUID messageId = UUID.randomUUID();

        publishIncoming(incomingJson(messageId));

        Message deadLetter = await().atMost(AWAIT_TIMEOUT)
                .until(() -> rabbitTemplate.receive(properties.rabbit().dlq()), Objects::nonNull);

        assertThat(deadLetter.getMessageProperties().getHeaders()).containsKey("x-death");
        assertThat(resultRepository.count()).isZero();
        assertThat(outboxRepository.count()).isZero();
        assertThat(rabbitTemplate.receive(properties.rabbit().resultQueue(), GRACE_RECEIVE_MILLIS))
                .as("no outgoing event for a failed message")
                .isNull();
    }

    private void stubEnrichmentSuccess() {
        WIRE_MOCK.stubFor(post("/enrich").willReturn(okJson("{\"userId\": %d, \"result\": true}".formatted(USER_ID))));
    }

    private void publishIncoming(String body) {
        MessageProperties messageProperties = new MessageProperties();
        messageProperties.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        rabbitTemplate.send(
                properties.rabbit().incomingExchange(),
                properties.rabbit().incomingRoutingKey(),
                new Message(body.getBytes(StandardCharsets.UTF_8), messageProperties));
    }

    private ResultEntity awaitResultRow(UUID messageId) {
        return await().atMost(AWAIT_TIMEOUT)
                .until(
                        () -> resultRepository.findAll().stream()
                                .filter(row -> messageId.equals(row.getMessageId()))
                                .findFirst(),
                        Optional::isPresent)
                .orElseThrow();
    }

    private void awaitResultEvent(JsonNode expected) {
        await().atMost(AWAIT_TIMEOUT).until(() -> {
            Message message = rabbitTemplate.receive(properties.rabbit().resultQueue());
            return message != null && JSON.readTree(message.getBody()).equals(expected);
        });
    }

    private static JsonNode expectedEvent(long logId, UUID messageId) {
        return JSON.readTree("{\"logId\":%d,\"messageId\":\"%s\",\"result\":true}".formatted(logId, messageId));
    }

    private static String incomingJson(UUID messageId) {
        return "{\"messageId\":\"%s\",\"userId\":%d,\"action\":\"request\",\"timestamp\":\"2026-07-01 10:00:00.0\"}"
                .formatted(messageId, USER_ID);
    }
}
