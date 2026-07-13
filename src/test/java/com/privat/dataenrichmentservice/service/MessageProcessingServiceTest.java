package com.privat.dataenrichmentservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.privat.dataenrichmentservice.client.EnrichmentClient;
import com.privat.dataenrichmentservice.client.EnrichmentTransientException;
import com.privat.dataenrichmentservice.client.dto.EnrichmentResponse;
import com.privat.dataenrichmentservice.config.AppProperties;
import com.privat.dataenrichmentservice.messaging.dto.IncomingMessage;
import com.privat.dataenrichmentservice.messaging.dto.ResultMessage;
import com.privat.dataenrichmentservice.outbox.OutboxWriter;
import com.privat.dataenrichmentservice.persistence.ResultEntity;
import com.privat.dataenrichmentservice.persistence.ResultRepository;
import com.privat.dataenrichmentservice.persistence.mapper.ResultMapperImpl;
import java.time.LocalDateTime;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;

@ExtendWith(MockitoExtension.class)
class MessageProcessingServiceTest {

    private static final UUID MESSAGE_ID = UUID.fromString("2b6a72cd-0f36-4e1a-9b4c-6f2f3a1d8e57");
    private static final long GENERATED_ID = 42L;
    private static final IncomingMessage MESSAGE =
            new IncomingMessage(MESSAGE_ID, 12345678L, "request", LocalDateTime.of(2026, 7, 1, 10, 0));
    private static final EnrichmentResponse ENRICHMENT = new EnrichmentResponse(12345678L, true);

    private static final String RESULT_EXCHANGE = "enrichment.result.exchange";
    private static final String RESULT_ROUTING_KEY = "enrichment.result";

    @Mock
    private EnrichmentClient enrichmentClient;

    @Mock
    private ResultRepository resultRepository;

    @Mock
    private OutboxWriter outboxWriter;

    private MessageProcessingService service;

    @BeforeEach
    void setUp() {
        AppProperties properties = new AppProperties(
                null,
                new AppProperties.Rabbit(
                        "enrichment.incoming.exchange",
                        "enrichment.incoming.queue",
                        "enrichment.request",
                        "enrichment.incoming.dlx",
                        "enrichment.incoming.dlq",
                        RESULT_EXCHANGE,
                        "enrichment.result.queue",
                        RESULT_ROUTING_KEY,
                        null),
                null);

        service = new MessageProcessingService(
                enrichmentClient,
                new ResultPersister(resultRepository, new ResultMapperImpl(), outboxWriter, properties));
    }

    @Test
    void processHappyPath() {
        when(enrichmentClient.enrich(12345678L, "request")).thenReturn(ENRICHMENT);
        when(resultRepository.existsByMessageId(MESSAGE_ID)).thenReturn(false);
        when(resultRepository.save(any())).thenAnswer(invocation -> {
            ResultEntity entity = invocation.getArgument(0);
            entity.setId(GENERATED_ID);
            return entity;
        });

        service.process(MESSAGE);

        verify(enrichmentClient).enrich(12345678L, "request");

        ArgumentCaptor<ResultEntity> savedEntity = ArgumentCaptor.forClass(ResultEntity.class);
        InOrder inOrder = inOrder(resultRepository, outboxWriter);
        inOrder.verify(resultRepository).save(savedEntity.capture());
        inOrder.verify(outboxWriter)
                .enqueue(
                        MESSAGE_ID,
                        RESULT_EXCHANGE,
                        RESULT_ROUTING_KEY,
                        new ResultMessage(GENERATED_ID, MESSAGE_ID, true));

        assertThat(savedEntity.getValue().getMessageId()).isEqualTo(MESSAGE_ID);
        assertThat(savedEntity.getValue().getUserId()).isEqualTo(12345678L);
        assertThat(savedEntity.getValue().getAction()).isEqualTo("request");
        assertThat(savedEntity.getValue().isResult()).isTrue();
    }

    @Test
    void duplicateDetectedByPreCheck() {
        when(enrichmentClient.enrich(12345678L, "request")).thenReturn(ENRICHMENT);
        when(resultRepository.existsByMessageId(MESSAGE_ID)).thenReturn(true);

        service.process(MESSAGE);

        verify(resultRepository, never()).save(any());
        verifyNoInteractions(outboxWriter);
    }

    @Test
    void duplicateRaceOnUniqueConstraint() {
        when(enrichmentClient.enrich(12345678L, "request")).thenReturn(ENRICHMENT);
        when(resultRepository.existsByMessageId(MESSAGE_ID)).thenReturn(false);
        when(resultRepository.save(any()))
                .thenThrow(new DataIntegrityViolationException("duplicate key value violates unique constraint"));

        service.process(MESSAGE);

        verifyNoInteractions(outboxWriter);
    }

    @Test
    void enrichmentFailurePropagates() {
        when(enrichmentClient.enrich(12345678L, "request"))
                .thenThrow(new EnrichmentTransientException("Enrichment API responded with 503"));

        assertThatThrownBy(() -> service.process(MESSAGE)).isInstanceOf(EnrichmentTransientException.class);

        verifyNoInteractions(resultRepository, outboxWriter);
    }

    @Test
    void dbFailurePropagates() {
        when(enrichmentClient.enrich(12345678L, "request")).thenReturn(ENRICHMENT);
        when(resultRepository.existsByMessageId(MESSAGE_ID)).thenReturn(false);
        when(resultRepository.save(any())).thenThrow(new DataAccessResourceFailureException("connection lost"));

        assertThatThrownBy(() -> service.process(MESSAGE)).isInstanceOf(DataAccessException.class);

        verifyNoInteractions(outboxWriter);
    }
}
