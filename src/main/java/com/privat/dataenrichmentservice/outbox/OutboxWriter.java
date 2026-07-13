package com.privat.dataenrichmentservice.outbox;

import java.util.UUID;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

@Component
public class OutboxWriter {

    private final OutboxRepository outboxRepository;
    private final JsonMapper jsonMapper;

    public OutboxWriter(OutboxRepository outboxRepository, JsonMapper jsonMapper) {
        this.outboxRepository = outboxRepository;
        this.jsonMapper = jsonMapper;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void enqueue(UUID messageId, String exchange, String routingKey, Object payload) {
        outboxRepository.save(OutboxEntity.builder()
                .messageId(messageId)
                .exchange(exchange)
                .routingKey(routingKey)
                .payload(jsonMapper.writeValueAsString(payload))
                .build());
    }
}
