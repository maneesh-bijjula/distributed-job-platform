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
        var messages = jdbc.query("""
                SELECT job_id, priority FROM job_outbox WHERE available_at <= now()
                ORDER BY priority DESC, available_at, created_at, job_id
                LIMIT 20 FOR UPDATE SKIP LOCKED
                """, Map.of(), (rs, row) -> new PendingJob(rs.getObject("job_id", UUID.class), rs.getInt("priority")));
        for (PendingJob message : messages) {
            // A crash after confirmation can redeliver; the worker locks and checks job state.
            publisher.publish(message.id(), message.priority());
            jdbc.update("DELETE FROM job_outbox WHERE job_id = :id", Map.of("id", message.id()));
        }
        return messages.size();
    }

    private record PendingJob(UUID id, int priority) {}
}
