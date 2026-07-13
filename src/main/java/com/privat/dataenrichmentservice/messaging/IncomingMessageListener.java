package com.privat.dataenrichmentservice.messaging;

import com.privat.dataenrichmentservice.messaging.dto.IncomingMessage;
import com.privat.dataenrichmentservice.service.MessageProcessingService;
import jakarta.validation.Valid;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class IncomingMessageListener {

    private final MessageProcessingService messageProcessingService;

    public IncomingMessageListener(MessageProcessingService messageProcessingService) {
        this.messageProcessingService = messageProcessingService;
    }

    @RabbitListener(queues = "${app.rabbit.incoming-queue}")
    public void onIncomingMessage(@Payload @Valid IncomingMessage message) {
        messageProcessingService.process(message);
    }
}
