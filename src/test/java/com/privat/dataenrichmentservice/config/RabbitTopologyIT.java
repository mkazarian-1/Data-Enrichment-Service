package com.privat.dataenrichmentservice.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.privat.dataenrichmentservice.TestcontainersConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.Queue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class RabbitTopologyIT {

    @Autowired
    private AmqpAdmin amqpAdmin;

    @Autowired
    private AppProperties properties;

    @Autowired
    private Declarables declarables;

    @Test
    void allQueuesExistOnBroker() {
        assertThat(amqpAdmin.getQueueProperties(properties.rabbit().incomingQueue()))
                .isNotNull();
        assertThat(amqpAdmin.getQueueProperties(properties.rabbit().dlq())).isNotNull();
        assertThat(amqpAdmin.getQueueProperties(properties.rabbit().resultQueue()))
                .isNotNull();
    }

    @Test
    void incomingQueueDeadLettersToConfiguredDlx() {
        Queue incoming = declarables.getDeclarablesByType(Queue.class).stream()
                .filter(queue -> queue.getName().equals(properties.rabbit().incomingQueue()))
                .findFirst()
                .orElseThrow();

        assertThat(incoming.getArguments())
                .containsEntry("x-dead-letter-exchange", properties.rabbit().dlx())
                .containsEntry("x-dead-letter-routing-key", properties.rabbit().incomingRoutingKey());

        assertThat(amqpAdmin.declareQueue(incoming))
                .isEqualTo(properties.rabbit().incomingQueue());
    }
}
