package com.privat.dataenrichmentservice.persistence;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface ResultRepository extends JpaRepository<ResultEntity, Long> {

    boolean existsByMessageId(UUID messageId);
}
