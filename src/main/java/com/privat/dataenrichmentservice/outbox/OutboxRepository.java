package com.privat.dataenrichmentservice.outbox;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface OutboxRepository extends JpaRepository<OutboxEntity, Long> {

    /**
     * Claims the oldest PENDING rows for publishing. {@code FOR UPDATE SKIP LOCKED} lets
     * concurrent relay instances drain the table without double-claiming a row.
     */
    @Query(
            value = "SELECT * FROM outbox WHERE status = 'PENDING' ORDER BY id LIMIT :limit FOR UPDATE SKIP LOCKED",
            nativeQuery = true)
    List<OutboxEntity> findPendingBatch(@Param("limit") int limit);
}
