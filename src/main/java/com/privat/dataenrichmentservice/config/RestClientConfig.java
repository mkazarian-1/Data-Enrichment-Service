package com.privat.dataenrichmentservice.config;

import java.net.http.HttpClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

@Configuration(proxyBeanMethods = false)
public class RestClientConfig {

    @Bean
    public RestClient enrichmentRestClient(AppProperties properties) {
        AppProperties.Enrichment enrichment = properties.enrichment();

        JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory(HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(enrichment.connectTimeout())
                .build());
        requestFactory.setReadTimeout(enrichment.readTimeout());
        return RestClient.builder()
                .baseUrl(enrichment.baseUrl())
                .requestFactory(requestFactory)
                .build();
    }
}
