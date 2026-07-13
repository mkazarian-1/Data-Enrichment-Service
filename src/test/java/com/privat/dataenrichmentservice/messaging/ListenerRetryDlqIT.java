package com.privat.dataenrichmentservice.messaging;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.privat.dataenrichmentservice.TestcontainersConfiguration;
import com.privat.dataenrichmentservice.config.AppProperties;
import com.privat.dataenrichmentservice.outbox.OutboxRepository;
import com.privat.dataenrichmentservice.persistence.ResultRepository;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
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

@SpringBootTest
@Import(TestcontainersConfiguration.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ListenerRetryDlqIT {

    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(10);

    private static final WireMockServer WIRE_MOCK = new WireMockServer(options().dynamicPort());

    @DynamicPropertySource
    static void testProperties(DynamicPropertyRegistry registry) {
        WIRE_MOCK.start();
        registry.add("app.enrichment.base-url", WIRE_MOCK::baseUrl);
        registry.add("app.rabbit.retry.initial-interval", () -> "50ms");
        registry.add("app.rabbit.retry.max-interval", () -> "200ms");
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
        outboxRepository.deleteAll();
        resultRepository.deleteAll();
    }

    @Test
    void malformedPayloadDeadLettersWithoutRetryOrEnrichment() {
        publishRaw("this is not json");

        Message deadLetter = awaitDlqMessage();

        assertThat(deadLetter.getBody()).isEqualTo("this is not json".getBytes(StandardCharsets.UTF_8));
        WIRE_MOCK.verify(0, postRequestedFor(urlEqualTo("/enrich")));
    }

    @Test
    void validationFailureDeadLettersWithoutEnrichmentCall() {
        publishRaw(incomingJson(UUID.randomUUID(), -1L));

        awaitDlqMessage();

        WIRE_MOCK.verify(0, postRequestedFor(urlEqualTo("/enrich")));
    }

    @Test
    void persistentTransientFailureDeadLettersAfterMaxAttempts() {
        WIRE_MOCK.stubFor(post("/enrich").willReturn(aResponse().withStatus(500)));
        UUID messageId = UUID.randomUUID();

        publishRaw(incomingJson(messageId, 12345678L));

        Message deadLetter = awaitDlqMessage();
        WIRE_MOCK.verify(properties.rabbit().retry().maxAttempts(), postRequestedFor(urlEqualTo("/enrich")));
        assertThat(deadLetter.getMessageProperties().getHeaders()).containsKey("x-death");
        assertThat(resultRepository.existsByMessageId(messageId)).isFalse();
    }

    @Test
    void recoversOnThirdAttemptAfterTwoTransientFailures() {
        WIRE_MOCK.stubFor(post("/enrich")
                .inScenario("flaky")
                .whenScenarioStateIs(STARTED)
                .willReturn(aResponse().withStatus(500))
                .willSetStateTo("one failure"));
        WIRE_MOCK.stubFor(post("/enrich")
                .inScenario("flaky")
                .whenScenarioStateIs("one failure")
                .willReturn(aResponse().withStatus(500))
                .willSetStateTo("two failures"));
        WIRE_MOCK.stubFor(post("/enrich")
                .inScenario("flaky")
                .whenScenarioStateIs("two failures")
                .willReturn(okJson("{\"userId\": 12345678, \"result\": true}")));
        UUID messageId = UUID.randomUUID();

        publishRaw(incomingJson(messageId, 12345678L));

        await().atMost(AWAIT_TIMEOUT).untilAsserted(() -> assertThat(resultRepository.existsByMessageId(messageId))
                .isTrue());
        WIRE_MOCK.verify(3, postRequestedFor(urlEqualTo("/enrich")));
        assertThat(Objects.requireNonNull(
                                amqpAdmin.getQueueInfo(properties.rabbit().dlq()))
                        .getMessageCount())
                .isZero();
    }

    private void publishRaw(String body) {
        MessageProperties messageProperties = new MessageProperties();
        messageProperties.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        rabbitTemplate.send(
                properties.rabbit().incomingExchange(),
                properties.rabbit().incomingRoutingKey(),
                new Message(body.getBytes(StandardCharsets.UTF_8), messageProperties));
    }

    private Message awaitDlqMessage() {
        return await().atMost(AWAIT_TIMEOUT)
                .until(() -> rabbitTemplate.receive(properties.rabbit().dlq()), Objects::nonNull);
    }

    private static String incomingJson(UUID messageId, long userId) {
        return "{\"messageId\":\"%s\",\"userId\":%d,\"action\":\"request\",\"timestamp\":\"2026-07-01 10:00:00.0\"}"
                .formatted(messageId, userId);
    }
}
