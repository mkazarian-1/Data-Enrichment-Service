package com.privat.dataenrichmentservice.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Single typed home for every tunable of the service (PRD §8). Validation runs at startup,
 * so a misconfigured deployment fails fast instead of misbehaving at runtime.
 */
@Validated
@ConfigurationProperties(prefix = "app")
public record AppProperties(
        @NotNull @Valid Enrichment enrichment, @NotNull @Valid Rabbit rabbit, @NotNull @Valid Outbox outbox) {

    public record Enrichment(
            @NotBlank String baseUrl, @NotNull Duration connectTimeout, @NotNull Duration readTimeout) {}

    public record Rabbit(
            @NotBlank String incomingExchange,
            @NotBlank String incomingQueue,
            @NotBlank String incomingRoutingKey,
            @NotBlank String dlx,
            @NotBlank String dlq,
            @NotBlank String resultExchange,
            @NotBlank String resultQueue,
            @NotBlank String resultRoutingKey,
            @NotNull @Valid Retry retry) {

        public record Retry(
                @Min(1) int maxAttempts,
                @NotNull Duration initialInterval,
                @DecimalMin("1.0") double multiplier,
                @NotNull Duration maxInterval) {}
    }

    public record Outbox(@NotNull Duration pollInterval, @Positive int batchSize) {}
}
