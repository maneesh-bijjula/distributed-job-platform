package com.maneesh.jobplatform.messaging;

import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OutboxRelay {
    private final NamedParameterJdbcTemplate jdbc;
    private final JobPublisher publisher;

    public OutboxRelay(NamedParameterJdbcTemplate jdbc, JobPublisher publisher) {
        this.jdbc = jdbc;
        this.publisher = publisher;
    }

    @Transactional(timeout = 120)
    public int dispatchBatch() {
        var ids = jdbc.query("""
                SELECT job_id FROM job_outbox ORDER BY created_at, job_id
                LIMIT 20 FOR UPDATE SKIP LOCKED
                """, Map.of(), (rs, row) -> rs.getObject("job_id", UUID.class));
        for (UUID id : ids) {
            // A crash after confirmation can redeliver; the worker locks and checks job state.
            publisher.publish(id);
            jdbc.update("DELETE FROM job_outbox WHERE job_id = :id", Map.of("id", id));
        }
        return ids.size();
    }
}
