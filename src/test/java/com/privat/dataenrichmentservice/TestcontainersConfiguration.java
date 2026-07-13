package com.privat.dataenrichmentservice;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistrar;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.rabbitmq.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

    private static final PostgreSQLContainer POSTGRES =
            new PostgreSQLContainer(DockerImageName.parse("postgres:16-alpine"));

    private static final RabbitMQContainer RABBIT_MQ =
            new RabbitMQContainer(DockerImageName.parse("rabbitmq:3.13-management"));

    static {
        POSTGRES.start();
        RABBIT_MQ.start();
    }

    @Bean
    DynamicPropertyRegistrar testcontainersProperties() {
        return registry -> {
            registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
            registry.add("spring.datasource.username", POSTGRES::getUsername);
            registry.add("spring.datasource.password", POSTGRES::getPassword);
            registry.add("spring.rabbitmq.host", RABBIT_MQ::getHost);
            registry.add("spring.rabbitmq.port", RABBIT_MQ::getAmqpPort);
            registry.add("spring.rabbitmq.username", RABBIT_MQ::getAdminUsername);
            registry.add("spring.rabbitmq.password", RABBIT_MQ::getAdminPassword);
        };
    }
}
