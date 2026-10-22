package com.maneesh.jobplatform.messaging;

import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import com.maneesh.jobplatform.config.DispatchProperties;

@Service
public class OutboxRelay {
    private final NamedParameterJdbcTemplate jdbc;
    private final JobPublisher publisher;
    private final DispatchProperties properties;

    public OutboxRelay(NamedParameterJdbcTemplate jdbc, JobPublisher publisher, DispatchProperties properties) {
        this.jdbc = jdbc;
        this.publisher = publisher;
        this.properties = properties;
    }

    @Transactional(timeout = 120)
    public int dispatchBatch() {
        var messages = jdbc.query("""
                SELECT job_id, priority, generation FROM job_outbox WHERE available_at <= now()
                ORDER BY priority DESC, available_at, created_at, job_id
                LIMIT :batch FOR UPDATE SKIP LOCKED
                """, Map.of("batch",properties.batchSize()), (rs, row) -> new JobPublisher.Delivery(rs.getObject("job_id", UUID.class), rs.getInt("priority"), rs.getInt("generation")));
        if(messages.isEmpty()) return 0;
        // Every message must be confirmed and routed before any outbox removal can commit.
        // Partial delivery or a crash still permits duplicates, never at-most-once deletion.
        publisher.publishBatch(messages);
        var keys=messages.stream().map(message->new Object[]{message.id(),message.generation()}).toList();
        jdbc.update("DELETE FROM job_outbox WHERE (job_id,generation) IN (:keys)",Map.of("keys",keys));
        return messages.size();
    }

}
