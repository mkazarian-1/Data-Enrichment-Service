package com.privat.dataenrichmentservice.persistence;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.privat.dataenrichmentservice.TestcontainersConfiguration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(TestcontainersConfiguration.class)
class ResultRepositoryTest {

    @Autowired
    private ResultRepository repository;

    /**
     * The UNIQUE constraint on message_id (V1__init.sql) is the idempotency backstop:
     * duplicate-race handling in the processing service relies on this exact exception.
     */
    @Test
    void duplicateMessageIdRejected() {
        UUID messageId = UUID.randomUUID();
        repository.saveAndFlush(entityWith(messageId));

        assertThatThrownBy(() -> repository.saveAndFlush(entityWith(messageId)))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    private static ResultEntity entityWith(UUID messageId) {
        return ResultEntity.builder()
                .messageId(messageId)
                .userId(1L)
                .action("request")
                .result(false)
                .build();
    }
}
