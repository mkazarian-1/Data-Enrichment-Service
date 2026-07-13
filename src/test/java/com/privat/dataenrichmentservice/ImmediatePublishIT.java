package com.privat.dataenrichmentservice;

import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.awaitility.Awaitility.await;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.privat.dataenrichmentservice.config.AppProperties;
import com.privat.dataenrichmentservice.outbox.OutboxRepository;
import com.privat.dataenrichmentservice.persistence.ResultRepository;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
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
class ImmediatePublishIT {

    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(10);
    private static final long USER_ID = 12345678L;

    private static final JsonMapper JSON = JsonMapper.builder().build();

    private static final WireMockServer WIRE_MOCK = new WireMockServer(options().dynamicPort());

    @DynamicPropertySource
    static void testProperties(DynamicPropertyRegistry registry) {
        WIRE_MOCK.start();
        registry.add("app.enrichment.base-url", WIRE_MOCK::baseUrl);
        registry.add("app.outbox.poll-interval", () -> "1h");
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
        amqpAdmin.purgeQueue(properties.rabbit().resultQueue(), false);
        outboxRepository.deleteAll();
        resultRepository.deleteAll();
    }

    @Test
    void resultEventIsPublishedImmediatelyWithoutTheScheduledRelay() {
        WIRE_MOCK.stubFor(post("/enrich").willReturn(okJson("{\"userId\": %d, \"result\": true}".formatted(USER_ID))));
        UUID messageId = UUID.randomUUID();

        publishIncoming(
                "{\"messageId\":\"%s\",\"userId\":%d,\"action\":\"request\",\"timestamp\":\"2026-07-01 10:00:00.0\"}"
                        .formatted(messageId, USER_ID));

        JsonNode expectedId = JSON.readTree("\"%s\"".formatted(messageId));
        await().atMost(AWAIT_TIMEOUT).until(() -> {
            Message event = rabbitTemplate.receive(properties.rabbit().resultQueue());
            return event != null
                    && expectedId.equals(JSON.readTree(event.getBody()).get("messageId"));
        });
    }

    private void publishIncoming(String body) {
        MessageProperties messageProperties = new MessageProperties();
        messageProperties.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        rabbitTemplate.send(
                properties.rabbit().incomingExchange(),
                properties.rabbit().incomingRoutingKey(),
                new Message(body.getBytes(StandardCharsets.UTF_8), messageProperties));
    }
}
