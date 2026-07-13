package com.privat.dataenrichmentservice.messaging;

import com.privat.dataenrichmentservice.client.EnrichmentFatalException;
import org.springframework.amqp.listener.ConditionalRejectingErrorHandler;

public class EnrichmentFatalExceptionStrategy extends ConditionalRejectingErrorHandler.DefaultExceptionStrategy {

    @Override
    protected boolean isUserCauseFatal(Throwable cause) {
        return cause instanceof EnrichmentFatalException;
    }
}
