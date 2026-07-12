package com.privat.dataenrichmentservice.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.json.JsonMapper;

/**
 * Messaging topology (PRD §7.1) and JSON conversion. Everything is declared idempotently
 * on the first broker connection; names come exclusively from {@link AppProperties}.
 */
@Configuration(proxyBeanMethods = false)
class RabbitConfig {

    @Bean
    Declarables rabbitTopology(AppProperties properties) {
        AppProperties.Rabbit rabbit = properties.rabbit();

        DirectExchange incomingExchange = ExchangeBuilder.directExchange(rabbit.incomingExchange()).build();
        Queue incomingQueue = QueueBuilder.durable(rabbit.incomingQueue())
                .deadLetterExchange(rabbit.dlx())
                .deadLetterRoutingKey(rabbit.incomingRoutingKey())
                .build();
        Binding incomingBinding = BindingBuilder.bind(incomingQueue).to(incomingExchange)
                .with(rabbit.incomingRoutingKey());

        DirectExchange deadLetterExchange = ExchangeBuilder.directExchange(rabbit.dlx()).build();
        Queue deadLetterQueue = QueueBuilder.durable(rabbit.dlq()).build();
        Binding deadLetterBinding = BindingBuilder.bind(deadLetterQueue).to(deadLetterExchange)
                .with(rabbit.incomingRoutingKey());

        TopicExchange resultExchange = ExchangeBuilder.topicExchange(rabbit.resultExchange()).build();
        Queue resultQueue = QueueBuilder.durable(rabbit.resultQueue()).build();
        Binding resultBinding = BindingBuilder.bind(resultQueue).to(resultExchange)
                .with(rabbit.resultRoutingKey());

        return new Declarables(
                incomingExchange, incomingQueue, incomingBinding,
                deadLetterExchange, deadLetterQueue, deadLetterBinding,
                resultExchange, resultQueue, resultBinding);
    }

    /**
     * Picked up by Boot for both the listener container factory and the {@code RabbitTemplate}.
     * Jackson 3 ships java-time support out of the box and writes dates as ISO strings by default.
     */
    @Bean
    MessageConverter jsonMessageConverter(JsonMapper jsonMapper) {
        return new JacksonJsonMessageConverter(jsonMapper);
    }
}
