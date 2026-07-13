package com.privat.dataenrichmentservice.outbox;

import com.privat.dataenrichmentservice.config.AppProperties;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
public class OutboxRelay {

    static final int ATTEMPTS_ALERT_THRESHOLD = 10;

    private final OutboxRepository outboxRepository;
    private final RabbitTemplate rabbitTemplate;
    private final AppProperties.Outbox outbox;

    public OutboxRelay(OutboxRepository outboxRepository, RabbitTemplate rabbitTemplate, AppProperties properties) {
        this.outboxRepository = outboxRepository;
        this.rabbitTemplate = rabbitTemplate;
        this.outbox = properties.outbox();
    }

    @Scheduled(fixedDelayString = "${app.outbox.poll-interval}")
    @Transactional
    public void relayPendingBatch() {
        List<OutboxEntity> batch = outboxRepository.findPendingBatch(outbox.batchSize());
        for (OutboxEntity row : batch) {
            publishAndRecord(row);
        }
    }

    @Transactional
    public void publishNow(long outboxId) {
        outboxRepository.findPendingForUpdate(outboxId).ifPresent(this::publishAndRecord);
    }

    private void publishAndRecord(OutboxEntity row) {
        if (publishWithConfirm(row)) {
            row.setStatus(OutboxStatus.SENT);
            row.setSentAt(OffsetDateTime.now());
        } else {
            row.setAttempts(row.getAttempts() + 1);
            if (row.getAttempts() >= ATTEMPTS_ALERT_THRESHOLD) {
                log.error(
                        "Outbox row keeps failing and needs attention: messageId={}, attempts={}",
                        row.getMessageId(),
                        row.getAttempts());
            }
        }
    }

    private boolean publishWithConfirm(OutboxEntity row) {
        CorrelationData correlation = new CorrelationData(row.getMessageId().toString());
        try {
            MessageProperties messageProperties = new MessageProperties();
            messageProperties.setContentType(MessageProperties.CONTENT_TYPE_JSON);
            Message message = new Message(row.getPayload().getBytes(StandardCharsets.UTF_8), messageProperties);
            rabbitTemplate.send(row.getExchange(), row.getRoutingKey(), message, correlation);

            CorrelationData.Confirm confirm =
                    correlation.getFuture().get(outbox.confirmTimeout().toMillis(), TimeUnit.MILLISECONDS);
            if (!confirm.ack()) {
                log.warn("Broker nacked outbox row: messageId={}, reason={}", row.getMessageId(), confirm.reason());
                return false;
            }
            if (correlation.getReturned() != null) {
                log.warn(
                        "Outbox row unroutable, returned by broker: messageId={}, exchange={}, routingKey={}",
                        row.getMessageId(),
                        row.getExchange(),
                        row.getRoutingKey());
                return false;
            }
            return true;
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.warn("Publish failed for outbox row: messageId={}", row.getMessageId(), e);
            return false;
        }
    }
}
