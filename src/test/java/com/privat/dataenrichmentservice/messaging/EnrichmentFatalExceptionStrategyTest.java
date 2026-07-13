package com.privat.dataenrichmentservice.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import com.privat.dataenrichmentservice.client.EnrichmentFatalException;
import com.privat.dataenrichmentservice.client.EnrichmentTransientException;
import com.privat.dataenrichmentservice.messaging.dto.IncomingMessage;
import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.listener.ListenerExecutionFailedException;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.core.MethodParameter;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.messaging.handler.annotation.support.MethodArgumentNotValidException;
import org.springframework.messaging.support.GenericMessage;

class EnrichmentFatalExceptionStrategyTest {

    private final EnrichmentFatalExceptionStrategy strategy = new EnrichmentFatalExceptionStrategy();

    @Test
    void enrichmentFatalExceptionIsFatal() {
        assertThat(strategy.isFatal(listenerFailure(new EnrichmentFatalException("Enrichment API rejected: 400"))))
                .isTrue();
    }

    @Test
    void messageConversionFailureIsFatal() {
        assertThat(strategy.isFatal(listenerFailure(new MessageConversionException("garbage payload"))))
                .isTrue();
    }

    @Test
    void payloadValidationFailureIsFatal() throws NoSuchMethodException {
        Method listenerMethod = IncomingMessageListener.class.getMethod("onIncomingMessage", IncomingMessage.class);
        MethodArgumentNotValidException validationFailure = new MethodArgumentNotValidException(
                new GenericMessage<>(new byte[0]), new MethodParameter(listenerMethod, 0));

        assertThat(strategy.isFatal(listenerFailure(validationFailure))).isTrue();
    }

    @Test
    void transientEnrichmentFailureIsNotFatal() {
        assertThat(strategy.isFatal(listenerFailure(new EnrichmentTransientException("Enrichment API 503"))))
                .isFalse();
    }

    @Test
    void databaseFailureIsNotFatal() {
        assertThat(strategy.isFatal(listenerFailure(new DataAccessResourceFailureException("connection lost"))))
                .isFalse();
    }

    private static ListenerExecutionFailedException listenerFailure(Throwable cause) {
        // The container adapter always wraps listener exceptions this way before they reach the
        // retry advice and the error handler.
        return new ListenerExecutionFailedException("Listener threw exception", cause, new Message(new byte[0]));
    }
}
