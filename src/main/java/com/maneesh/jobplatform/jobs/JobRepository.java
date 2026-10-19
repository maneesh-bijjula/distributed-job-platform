package com.maneesh.jobplatform.jobs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;
import java.time.Instant;
import java.sql.Timestamp;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class JobRepository {
    private final NamedParameterJdbcTemplate jdbc;
    private final ObjectMapper mapper;

    public JobRepository(NamedParameterJdbcTemplate jdbc, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    public Optional<UUID> insert(UUID id, JobType type, String text, String key, int priority,
                                 Instant scheduledAt, UUID retryOf) {
        Map<String, Object> params = new HashMap<>();
        params.put("id", id);
        params.put("type", type.name());
        params.put("text", text);
        params.put("key", key);
        params.put("priority", priority);
        params.put("scheduledAt", scheduledAt == null ? null : Timestamp.from(scheduledAt));
        params.put("retryOf", retryOf);
        return jdbc.query("""
                INSERT INTO jobs (id, type, input_text, idempotency_key, priority, scheduled_at, retry_of)
                VALUES (:id, :type, :text, :key, :priority, :scheduledAt, :retryOf)
                ON CONFLICT (idempotency_key) DO NOTHING RETURNING id
                """, params, (rs, row) -> rs.getObject("id", UUID.class)).stream().findFirst();
    }

    public void enqueue(UUID id) {
        jdbc.update("""
                INSERT INTO job_outbox (job_id, priority, available_at)
                SELECT id, priority, COALESCE(scheduled_at, created_at) FROM jobs WHERE id = :id
                """, Map.of("id", id));
    }

    public Optional<Job> find(UUID id) {
        return jdbc.query("SELECT * FROM jobs WHERE id = :id", Map.of("id", id), this::map)
                .stream().findFirst();
    }

    public Optional<Job> findByKey(String key) {
        return jdbc.query("SELECT * FROM jobs WHERE idempotency_key = :key", Map.of("key", key), this::map)
                .stream().findFirst();
    }

    public Optional<Job> lock(UUID id) {
        return jdbc.query("SELECT * FROM jobs WHERE id = :id FOR UPDATE", Map.of("id", id), this::map)
                .stream().findFirst();
    }

    public List<Job> list(JobStatus status, int page, int size) {
        Map<String, Object> params = new HashMap<>();
        params.put("size", size + 1);
        params.put("offset", (long) page * size);
        String filter = "";
        if (status != null) {
            filter = " WHERE status = :status";
            params.put("status", status.name());
        }
        return jdbc.query("SELECT * FROM jobs" + filter
                + " ORDER BY created_at DESC, id DESC LIMIT :size OFFSET :offset", params, this::map);
    }

    public void succeed(UUID id, JsonNode result) {
        jdbc.update("""
                UPDATE jobs SET status = 'SUCCEEDED', result = CAST(:result AS jsonb), completed_at = clock_timestamp()
                WHERE id = :id
                """, Map.of("id", id, "result", result.toString()));
    }

    public void fail(UUID id, String error) {
        jdbc.update("""
                UPDATE jobs SET status = 'FAILED', error = :error, completed_at = clock_timestamp() WHERE id = :id
                """, Map.of("id", id, "error", error));
    }

    public void cancel(UUID id) {
        jdbc.update("""
                UPDATE jobs SET status = 'CANCELLED', completed_at = clock_timestamp() WHERE id = :id
                """, Map.of("id", id));
        jdbc.update("DELETE FROM job_outbox WHERE job_id = :id", Map.of("id", id));
    }

    public void recordEvent(UUID id, JobEvent.Type type, JobStatus status, UUID relatedJobId) {
        Map<String, Object> params = new HashMap<>();
        params.put("id", id);
        params.put("type", type.name());
        params.put("status", status.name());
        params.put("related", relatedJobId);
        jdbc.update("""
                INSERT INTO job_events (job_id, event_type, status, related_job_id)
                VALUES (:id, :type, :status, :related)
                """, params);
    }

    public List<JobEvent> events(UUID id, int page, int size) {
        return jdbc.query("""
                SELECT * FROM job_events WHERE job_id = :id ORDER BY id LIMIT :size OFFSET :offset
                """, Map.of("id", id, "size", size + 1, "offset", (long) page * size),
                (rs, row) -> new JobEvent(rs.getLong("id"), rs.getObject("job_id", UUID.class),
                        JobEvent.Type.valueOf(rs.getString("event_type")),
                        JobStatus.valueOf(rs.getString("status")), rs.getObject("related_job_id", UUID.class),
                        rs.getTimestamp("occurred_at").toInstant()));
    }

    private Job map(ResultSet rs, int row) throws SQLException {
        try {
            String result = rs.getString("result");
            var completed = rs.getTimestamp("completed_at");
            var scheduled = rs.getTimestamp("scheduled_at");
            return new Job(rs.getObject("id", UUID.class), JobType.valueOf(rs.getString("type")),
                    rs.getString("input_text"), JobStatus.valueOf(rs.getString("status")),
                    result == null ? null : mapper.readTree(result), rs.getString("error"),
                    rs.getTimestamp("created_at").toInstant(), completed == null ? null : completed.toInstant(),
                    rs.getInt("priority"), scheduled == null ? null : scheduled.toInstant(),
                    rs.getObject("retry_of", UUID.class));
        } catch (JsonProcessingException exception) {
            throw new SQLException("Invalid stored job result", exception);
        }
    }
}
