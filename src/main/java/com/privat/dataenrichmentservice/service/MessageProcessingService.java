package com.privat.dataenrichmentservice.service;

import com.privat.dataenrichmentservice.client.EnrichmentClient;
import com.privat.dataenrichmentservice.client.dto.EnrichmentResponse;
import com.privat.dataenrichmentservice.messaging.dto.IncomingMessage;
import com.privat.dataenrichmentservice.outbox.OutboxEntity;
import com.privat.dataenrichmentservice.outbox.OutboxRelay;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class MessageProcessingService {

    private final EnrichmentClient enrichmentClient;
    private final ResultPersister resultPersister;
    private final OutboxRelay outboxRelay;

    public MessageProcessingService(
            EnrichmentClient enrichmentClient, ResultPersister resultPersister, OutboxRelay outboxRelay) {
        this.enrichmentClient = enrichmentClient;
        this.resultPersister = resultPersister;
        this.outboxRelay = outboxRelay;
    }

    public void process(IncomingMessage message) {
        EnrichmentResponse enrichment = enrichmentClient.enrich(message.userId(), message.action());
        try {
            resultPersister.persist(message, enrichment).ifPresent(this::publishImmediately);
        } catch (DataIntegrityViolationException e) {
            log.warn("Duplicate message hit the unique constraint, skipping: messageId={}", message.messageId());
        }
    }

    private void publishImmediately(OutboxEntity row) {
        try {
            outboxRelay.publishNow(row.getId());
        } catch (Exception e) {
            log.warn(
                    "Immediate publish failed, leaving row for the scheduled relay: messageId={}",
                    row.getMessageId(),
                    e);
        }
    }
}
