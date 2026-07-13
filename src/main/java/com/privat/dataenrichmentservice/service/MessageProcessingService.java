package com.privat.dataenrichmentservice.service;

import com.privat.dataenrichmentservice.client.EnrichmentClient;
import com.privat.dataenrichmentservice.client.dto.EnrichmentResponse;
import com.privat.dataenrichmentservice.messaging.dto.IncomingMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class MessageProcessingService {

    private final EnrichmentClient enrichmentClient;
    private final ResultPersister resultPersister;

    public MessageProcessingService(EnrichmentClient enrichmentClient, ResultPersister resultPersister) {
        this.enrichmentClient = enrichmentClient;
        this.resultPersister = resultPersister;
    }

    public void process(IncomingMessage message) {
        EnrichmentResponse enrichment = enrichmentClient.enrich(message.userId(), message.action());
        try {
            resultPersister.persist(message, enrichment);
        } catch (DataIntegrityViolationException e) {
            log.warn("Duplicate message hit the unique constraint, skipping: messageId={}", message.messageId());
        }
    }
}
