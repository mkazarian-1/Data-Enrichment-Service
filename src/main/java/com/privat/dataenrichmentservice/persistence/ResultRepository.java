package com.privat.dataenrichmentservice.persistence;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ResultRepository extends JpaRepository<ResultEntity, Long> {

    boolean existsByMessageId(UUID messageId);
}
