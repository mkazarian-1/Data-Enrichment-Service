package com.privat.dataenrichmentservice.client;

import com.privat.dataenrichmentservice.client.dto.EnrichmentRequest;
import com.privat.dataenrichmentservice.client.dto.EnrichmentResponse;
import java.nio.charset.StandardCharsets;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

@Component
public class EnrichmentClient {

    private final RestClient restClient;

    public EnrichmentClient(RestClient enrichmentRestClient) {
        this.restClient = enrichmentRestClient;
    }

    public EnrichmentResponse enrich(Long userId, String action) {
        EnrichmentResponse response;
        try {
            response = restClient
                    .post()
                    .uri("/enrich")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(new EnrichmentRequest(userId, action))
                    .retrieve()
                    .onStatus(
                            HttpStatusCode::is4xxClientError,
                            (request, res) ->
                                    fatal(res.getStatusCode(), res.getBody().readAllBytes()))
                    .onStatus(HttpStatusCode::isError, (request, res) -> {
                        throw new EnrichmentTransientException("Enrichment API responded with " + res.getStatusCode());
                    })
                    .body(EnrichmentResponse.class);
        } catch (ResourceAccessException e) {
            throw new EnrichmentTransientException("Enrichment API unreachable: " + e.getMessage(), e);
        }
        if (response == null) {
            throw new EnrichmentTransientException("Enrichment API returned an empty body");
        }
        return response;
    }

    private static void fatal(HttpStatusCode status, byte[] body) {
        throw new EnrichmentFatalException("Enrichment API rejected the request: %s, body: %s"
                .formatted(status, new String(body, StandardCharsets.UTF_8)));
    }
}
