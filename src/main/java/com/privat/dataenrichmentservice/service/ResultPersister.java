package com.privat.dataenrichmentservice.service;

import com.privat.dataenrichmentservice.client.dto.EnrichmentResponse;
import com.privat.dataenrichmentservice.config.AppProperties;
import com.privat.dataenrichmentservice.messaging.dto.IncomingMessage;
import com.privat.dataenrichmentservice.messaging.dto.ResultMessage;
import com.privat.dataenrichmentservice.outbox.OutboxEntity;
import com.privat.dataenrichmentservice.outbox.OutboxWriter;
import com.privat.dataenrichmentservice.persistence.ResultEntity;
import com.privat.dataenrichmentservice.persistence.ResultRepository;
import com.privat.dataenrichmentservice.persistence.mapper.ResultMapper;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
public class ResultPersister {

    private final ResultRepository resultRepository;
    private final ResultMapper resultMapper;
    private final OutboxWriter outboxWriter;
    private final AppProperties.Rabbit rabbit;

    ResultPersister(
            ResultRepository resultRepository,
            ResultMapper resultMapper,
            OutboxWriter outboxWriter,
            AppProperties properties) {
        this.resultRepository = resultRepository;
        this.resultMapper = resultMapper;
        this.outboxWriter = outboxWriter;
        this.rabbit = properties.rabbit();
    }

    @Transactional
    public Optional<OutboxEntity> persist(IncomingMessage message, EnrichmentResponse enrichment) {
        if (resultRepository.existsByMessageId(message.messageId())) {
            log.warn("Message already processed, skipping: messageId={}", message.messageId());
            return Optional.empty();
        }
        ResultEntity saved = resultRepository.save(resultMapper.toEntity(message, enrichment));
        OutboxEntity outboxRow = outboxWriter.enqueue(
                message.messageId(),
                rabbit.resultExchange(),
                rabbit.resultRoutingKey(),
                new ResultMessage(saved.getId(), message.messageId(), saved.isResult()));
        log.info(
                "Result persisted and outbox event enqueued: messageId={}, logId={}",
                message.messageId(),
                saved.getId());
        return Optional.of(outboxRow);
    }
}
