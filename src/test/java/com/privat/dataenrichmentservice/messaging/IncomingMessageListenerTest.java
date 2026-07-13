package com.privat.dataenrichmentservice.messaging;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import com.privat.dataenrichmentservice.client.EnrichmentTransientException;
import com.privat.dataenrichmentservice.messaging.dto.IncomingMessage;
import com.privat.dataenrichmentservice.service.MessageProcessingService;
import java.time.LocalDateTime;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IncomingMessageListenerTest {

    private static final IncomingMessage MESSAGE =
            new IncomingMessage(UUID.randomUUID(), 12345678L, "request", LocalDateTime.of(2026, 7, 1, 10, 0));

    @Mock
    private MessageProcessingService messageProcessingService;

    @InjectMocks
    private IncomingMessageListener listener;

    @Test
    void delegatesPayloadToService() {
        listener.onIncomingMessage(MESSAGE);

        verify(messageProcessingService).process(MESSAGE);
    }

    @Test
    void exceptionsPropagateUntouched() {
        doThrow(new EnrichmentTransientException("Enrichment API responded with 503"))
                .when(messageProcessingService)
                .process(MESSAGE);

        assertThatThrownBy(() -> listener.onIncomingMessage(MESSAGE)).isInstanceOf(EnrichmentTransientException.class);
    }
}
