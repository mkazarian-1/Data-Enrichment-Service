package com.privat.dataenrichmentservice.outbox;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface OutboxRepository extends JpaRepository<OutboxEntity, Long> {

    @Query(
            value = "SELECT * FROM outbox WHERE status = 'PENDING' ORDER BY id LIMIT :limit FOR UPDATE SKIP LOCKED",
            nativeQuery = true)
    List<OutboxEntity> findPendingBatch(@Param("limit") int limit);

    @Query(
            value = "SELECT * FROM outbox WHERE id = :id AND status = 'PENDING' FOR UPDATE SKIP LOCKED",
            nativeQuery = true)
    Optional<OutboxEntity> findPendingForUpdate(@Param("id") long id);
}
