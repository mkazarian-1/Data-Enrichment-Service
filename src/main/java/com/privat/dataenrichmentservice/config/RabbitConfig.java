package com.privat.dataenrichmentservice.config;

import com.privat.dataenrichmentservice.messaging.EnrichmentFatalExceptionStrategy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.listener.ConditionalRejectingErrorHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListenerConfigurer;
import org.springframework.amqp.rabbit.config.RetryInterceptorBuilder;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.config.StatelessRetryOperationsInterceptor;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.retry.RejectAndDontRequeueRecoverer;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.amqp.autoconfigure.RabbitTemplateCustomizer;
import org.springframework.boot.amqp.autoconfigure.SimpleRabbitListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.retry.RetryPolicy;
import org.springframework.util.ErrorHandler;
import org.springframework.validation.Validator;
import tools.jackson.databind.json.JsonMapper;

@Slf4j
@Configuration(proxyBeanMethods = false)
class RabbitConfig {

    @Bean
    Declarables rabbitTopology(AppProperties properties) {
        AppProperties.Rabbit rabbit = properties.rabbit();

        DirectExchange incomingExchange =
                ExchangeBuilder.directExchange(rabbit.incomingExchange()).build();
        Queue incomingQueue = QueueBuilder.durable(rabbit.incomingQueue())
                .deadLetterExchange(rabbit.dlx())
                .deadLetterRoutingKey(rabbit.incomingRoutingKey())
                .build();
        Binding incomingBinding =
                BindingBuilder.bind(incomingQueue).to(incomingExchange).with(rabbit.incomingRoutingKey());

        DirectExchange deadLetterExchange =
                ExchangeBuilder.directExchange(rabbit.dlx()).build();
        Queue deadLetterQueue = QueueBuilder.durable(rabbit.dlq()).build();
        Binding deadLetterBinding =
                BindingBuilder.bind(deadLetterQueue).to(deadLetterExchange).with(rabbit.incomingRoutingKey());

        TopicExchange resultExchange =
                ExchangeBuilder.topicExchange(rabbit.resultExchange()).build();
        Queue resultQueue = QueueBuilder.durable(rabbit.resultQueue()).build();
        Binding resultBinding =
                BindingBuilder.bind(resultQueue).to(resultExchange).with(rabbit.resultRoutingKey());

        return new Declarables(
                incomingExchange,
                incomingQueue,
                incomingBinding,
                deadLetterExchange,
                deadLetterQueue,
                deadLetterBinding,
                resultExchange,
                resultQueue,
                resultBinding);
    }

    @Bean
    MessageConverter jsonMessageConverter(JsonMapper jsonMapper) {
        return new JacksonJsonMessageConverter(jsonMapper);
    }

    @Bean
    EnrichmentFatalExceptionStrategy fatalExceptionStrategy() {
        return new EnrichmentFatalExceptionStrategy();
    }

    @Bean
    ErrorHandler rabbitErrorHandler(EnrichmentFatalExceptionStrategy fatalExceptionStrategy) {
        return new ConditionalRejectingErrorHandler(fatalExceptionStrategy);
    }

    @Bean
    StatelessRetryOperationsInterceptor listenerRetryInterceptor(
            AppProperties properties, EnrichmentFatalExceptionStrategy fatalExceptionStrategy) {
        AppProperties.Rabbit.Retry retry = properties.rabbit().retry();
        RetryPolicy retryPolicy = RetryPolicy.builder()
                .maxRetries(retry.maxAttempts() - 1)
                .delay(retry.initialInterval())
                .multiplier(retry.multiplier())
                .maxDelay(retry.maxInterval())
                .predicate(throwable -> !fatalExceptionStrategy.isFatal(throwable))
                .build();
        return RetryInterceptorBuilder.stateless()
                .retryPolicy(retryPolicy)
                .recoverer(new RejectAndDontRequeueRecoverer())
                .build();
    }

    @Bean
    SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            SimpleRabbitListenerContainerFactoryConfigurer configurer,
            ConnectionFactory connectionFactory,
            StatelessRetryOperationsInterceptor listenerRetryInterceptor,
            ErrorHandler rabbitErrorHandler) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        configurer.configure(factory, connectionFactory);
        factory.setAdviceChain(listenerRetryInterceptor);
        factory.setErrorHandler(rabbitErrorHandler);
        factory.setDefaultRequeueRejected(false);
        return factory;
    }

    @Bean
    RabbitListenerConfigurer rabbitListenerConfigurer(Validator validator) {
        return registrar -> registrar.setValidator(validator);
    }

    @Bean
    RabbitTemplateCustomizer returnedMessageLoggingCustomizer() {
        return template -> template.setReturnsCallback(returned -> log.error(
                "Message returned as unroutable: exchange={}, routingKey={}, replyText={}",
                returned.getExchange(),
                returned.getRoutingKey(),
                returned.getReplyText()));
    }
}
