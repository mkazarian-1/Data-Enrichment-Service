package com.privat.dataenrichmentservice.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class AppPropertiesTest {

    private static final String[] VALID_PROPERTIES = {
        "app.enrichment.base-url=http://localhost:8081",
        "app.enrichment.connect-timeout=2s",
        "app.enrichment.read-timeout=5s",
        "app.rabbit.incoming-exchange=enrichment.incoming.exchange",
        "app.rabbit.incoming-queue=enrichment.incoming.queue",
        "app.rabbit.incoming-routing-key=enrichment.request",
        "app.rabbit.dlx=enrichment.incoming.dlx",
        "app.rabbit.dlq=enrichment.incoming.dlq",
        "app.rabbit.result-exchange=enrichment.result.exchange",
        "app.rabbit.result-queue=enrichment.result.queue",
        "app.rabbit.result-routing-key=enrichment.result",
        "app.rabbit.retry.max-attempts=4",
        "app.rabbit.retry.initial-interval=1s",
        "app.rabbit.retry.multiplier=2.0",
        "app.rabbit.retry.max-interval=10s",
        "app.outbox.poll-interval=500ms",
        "app.outbox.batch-size=100"
    };

    private final ApplicationContextRunner runner =
            new ApplicationContextRunner().withUserConfiguration(PropertiesHolder.class);

    @Test
    void bindsAllPropertiesToTypedRecord() {
        runner.withPropertyValues(VALID_PROPERTIES).run(context -> {
            AppProperties props = context.getBean(AppProperties.class);

            assertThat(props.enrichment().baseUrl()).isEqualTo("http://localhost:8081");
            assertThat(props.enrichment().connectTimeout()).isEqualTo(Duration.ofSeconds(2));
            assertThat(props.enrichment().readTimeout()).isEqualTo(Duration.ofSeconds(5));

            assertThat(props.rabbit().incomingExchange()).isEqualTo("enrichment.incoming.exchange");
            assertThat(props.rabbit().incomingQueue()).isEqualTo("enrichment.incoming.queue");
            assertThat(props.rabbit().incomingRoutingKey()).isEqualTo("enrichment.request");
            assertThat(props.rabbit().dlx()).isEqualTo("enrichment.incoming.dlx");
            assertThat(props.rabbit().dlq()).isEqualTo("enrichment.incoming.dlq");
            assertThat(props.rabbit().resultExchange()).isEqualTo("enrichment.result.exchange");
            assertThat(props.rabbit().resultQueue()).isEqualTo("enrichment.result.queue");
            assertThat(props.rabbit().resultRoutingKey()).isEqualTo("enrichment.result");

            assertThat(props.rabbit().retry().maxAttempts()).isEqualTo(4);
            assertThat(props.rabbit().retry().initialInterval()).isEqualTo(Duration.ofSeconds(1));
            assertThat(props.rabbit().retry().multiplier()).isEqualTo(2.0);
            assertThat(props.rabbit().retry().maxInterval()).isEqualTo(Duration.ofSeconds(10));

            assertThat(props.outbox().pollInterval()).isEqualTo(Duration.ofMillis(500));
            assertThat(props.outbox().batchSize()).isEqualTo(100);
        });
    }

    @Test
    void blankQueueNameFailsStartup() {
        runner.withPropertyValues(VALID_PROPERTIES)
                .withPropertyValues("app.rabbit.incoming-queue=")
                .run(context -> assertThat(context).hasFailed());
    }

    @EnableConfigurationProperties(AppProperties.class)
    static class PropertiesHolder {}
}
