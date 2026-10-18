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

    public Optional<UUID> insert(UUID id, JobType type, String text, String key) {
        Map<String, Object> params = new HashMap<>();
        params.put("id", id);
        params.put("type", type.name());
        params.put("text", text);
        params.put("key", key);
        return jdbc.query("""
                INSERT INTO jobs (id, type, input_text, idempotency_key)
                VALUES (:id, :type, :text, :key)
                ON CONFLICT (idempotency_key) DO NOTHING RETURNING id
                """, params, (rs, row) -> rs.getObject("id", UUID.class)).stream().findFirst();
    }

    public void enqueue(UUID id) {
        jdbc.update("INSERT INTO job_outbox (job_id) VALUES (:id)", Map.of("id", id));
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
                UPDATE jobs SET status = 'SUCCEEDED', result = CAST(:result AS jsonb), completed_at = now()
                WHERE id = :id
                """, Map.of("id", id, "result", result.toString()));
    }

    public void fail(UUID id, String error) {
        jdbc.update("""
                UPDATE jobs SET status = 'FAILED', error = :error, completed_at = now() WHERE id = :id
                """, Map.of("id", id, "error", error));
    }

    private Job map(ResultSet rs, int row) throws SQLException {
        try {
            String result = rs.getString("result");
            var completed = rs.getTimestamp("completed_at");
            return new Job(rs.getObject("id", UUID.class), JobType.valueOf(rs.getString("type")),
                    rs.getString("input_text"), JobStatus.valueOf(rs.getString("status")),
                    result == null ? null : mapper.readTree(result), rs.getString("error"),
                    rs.getTimestamp("created_at").toInstant(), completed == null ? null : completed.toInstant());
        } catch (JsonProcessingException exception) {
            throw new SQLException("Invalid stored job result", exception);
        }
    }
}
