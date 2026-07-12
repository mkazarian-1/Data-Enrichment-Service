package com.privat.dataenrichmentservice.client;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.privat.dataenrichmentservice.client.dto.EnrichmentResponse;
import com.privat.dataenrichmentservice.config.AppProperties;
import com.privat.dataenrichmentservice.config.RestClientConfig;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EnrichmentClientTest {

    private static final Duration TIMEOUT = Duration.ofMillis(500);

    private static WireMockServer wireMock;

    private EnrichmentClient client;

    @BeforeAll
    static void startServer() {
        wireMock = new WireMockServer(options().dynamicPort());
        wireMock.start();
    }

    @AfterAll
    static void stopServer() {
        wireMock.stop();
    }

    @BeforeEach
    void setUp() {
        wireMock.resetAll();
        AppProperties properties =
                new AppProperties(new AppProperties.Enrichment(wireMock.baseUrl(), TIMEOUT, TIMEOUT), null, null);
        client = new EnrichmentClient(new RestClientConfig().enrichmentRestClient(properties));
    }

    @Test
    void sendsUserIdAndActionWithExactJsonFieldNames() {
        wireMock.stubFor(post("/enrich").willReturn(okJson("{\"userId\": 12345678, \"result\": true}")));

        client.enrich(12345678L, "request");

        wireMock.verify(postRequestedFor(urlEqualTo("/enrich"))
                .withRequestBody(equalToJson("{\"userId\": 12345678, \"action\": \"request\"}")));
    }

    @Test
    void mapsSuccessfulResponse() {
        wireMock.stubFor(post("/enrich").willReturn(okJson("{\"userId\": 12345678, \"result\": true}")));

        EnrichmentResponse response = client.enrich(12345678L, "request");

        assertThat(response.userId()).isEqualTo(12345678L);
        assertThat(response.result()).isTrue();
    }

    @Test
    void serverErrorIsTransient() {
        wireMock.stubFor(post("/enrich").willReturn(aResponse().withStatus(500)));

        assertThatThrownBy(() -> client.enrich(1L, "request"))
                .isInstanceOf(EnrichmentTransientException.class)
                .hasMessageContaining("500");
    }

    @Test
    void clientErrorIsFatalAndCapturesBody() {
        wireMock.stubFor(
                post("/enrich").willReturn(aResponse().withStatus(400).withBody("{\"error\": \"unknown action\"}")));

        assertThatThrownBy(() -> client.enrich(1L, "bogus"))
                .isInstanceOf(EnrichmentFatalException.class)
                .hasMessageContaining("400")
                .hasMessageContaining("unknown action");
    }

    @Test
    void readTimeoutIsTransient() {
        wireMock.stubFor(post("/enrich")
                .willReturn(okJson("{\"userId\": 1, \"result\": true}")
                        .withFixedDelay((int) TIMEOUT.plusSeconds(1).toMillis())));

        assertThatThrownBy(() -> client.enrich(1L, "request")).isInstanceOf(EnrichmentTransientException.class);
    }
}
