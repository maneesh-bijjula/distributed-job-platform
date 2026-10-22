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
import com.maneesh.jobplatform.auth.ApiPrincipal;
import com.maneesh.jobplatform.observability.LifecycleMetrics;

@Repository
public class JobRepository {
    private final NamedParameterJdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private final LifecycleMetrics metrics;

    public JobRepository(NamedParameterJdbcTemplate jdbc, ObjectMapper mapper, LifecycleMetrics metrics) {
        this.jdbc = jdbc;
        this.mapper = mapper;
        this.metrics = metrics;
    }

    public Optional<UUID> insert(UUID id, JobType type, String text, String key, int priority,
                                 Instant scheduledAt, UUID retryOf, int timeoutSeconds) {
        return insert(id, type, text, key, priority, scheduledAt, retryOf, timeoutSeconds, RetryPolicy.DEFAULT);
    }

    public Optional<UUID> insert(UUID id, JobType type, String text, String key, int priority,
                                 Instant scheduledAt, UUID retryOf, int timeoutSeconds, RetryPolicy policy) {
        return insert(ApiPrincipal.LEGACY_OWNER,id,type,text,key,priority,scheduledAt,retryOf,timeoutSeconds,policy);
    }

    public Optional<UUID> insert(UUID owner, UUID id, JobType type, String text, String key, int priority,
                                 Instant scheduledAt, UUID retryOf, int timeoutSeconds, RetryPolicy policy) {
        Map<String, Object> params = new HashMap<>();
        params.put("owner",owner);
        params.put("id", id);
        params.put("type", type.name());
        params.put("text", text);
        params.put("key", key);
        params.put("priority", priority);
        params.put("scheduledAt", scheduledAt == null ? null : Timestamp.from(scheduledAt));
        params.put("retryOf", retryOf);
        params.put("timeout", timeoutSeconds);
        params.put("maxAttempts", policy.maxAttempts());
        params.put("initialBackoff", policy.initialBackoffSeconds());
        params.put("maxBackoff", policy.maxBackoffSeconds());
        return jdbc.query("""
                INSERT INTO jobs (id, type, input_text, idempotency_key, priority, scheduled_at, retry_of, timeout_seconds,
                    max_attempts, initial_backoff_seconds, max_backoff_seconds, owner_id, eligible_at)
                VALUES (:id, :type, :text, :key, :priority, :scheduledAt, :retryOf, :timeout, :maxAttempts, :initialBackoff, :maxBackoff, :owner,
                    greatest(clock_timestamp(),CAST(:scheduledAt AS timestamptz)))
                ON CONFLICT (owner_id,idempotency_key) DO NOTHING RETURNING id
                """, params, (rs, row) -> rs.getObject("id", UUID.class)).stream().findFirst();
    }

    public void enqueue(UUID id) {
        jdbc.update("""
                INSERT INTO job_outbox (job_id, priority, available_at, generation)
                SELECT id, priority, COALESCE(scheduled_at, created_at), generation FROM jobs WHERE id = :id
                """, Map.of("id", id));
    }

    public Optional<Job> find(UUID id) {
        return jdbc.query("SELECT * FROM jobs WHERE id = :id", Map.of("id", id), this::map)
                .stream().findFirst();
    }

    public Optional<Job> findByKey(String key) {
        return findByKey(ApiPrincipal.LEGACY_OWNER,key);
    }

    public Optional<Job> findByKey(UUID owner, String key) {
        return jdbc.query("SELECT * FROM jobs WHERE owner_id=:owner AND idempotency_key = :key", Map.of("owner",owner,"key", key), this::map)
                .stream().findFirst();
    }

    public Optional<Job> findOwned(UUID owner, UUID id) {
        return jdbc.query("SELECT * FROM jobs WHERE owner_id=:owner AND id=:id",Map.of("owner",owner,"id",id),this::map).stream().findFirst();
    }

    public List<Job> listOwned(UUID owner, JobStatus status, int page, int size) {
        var params = new HashMap<String,Object>();
        params.put("owner",owner); params.put("size",size+1); params.put("offset",(long)page*size);
        String filter = "";
        if (status != null) { filter=" AND status=:status"; params.put("status",status.name()); }
        return jdbc.query("SELECT * FROM jobs WHERE owner_id=:owner"+filter+" ORDER BY created_at DESC,id DESC LIMIT :size OFFSET :offset",params,this::map);
    }

    public boolean legacyRetryPolicy(UUID id) {
        return Boolean.TRUE.equals(jdbc.queryForObject("SELECT legacy_retry_policy FROM jobs WHERE id=:id", Map.of("id", id), Boolean.class));
    }

    public Optional<Job> lock(UUID id) {
        return jdbc.query("SELECT * FROM jobs WHERE id = :id FOR UPDATE", Map.of("id", id), this::map)
                .stream().findFirst();
    }

    public Optional<Job> lockClaim(UUID id, UUID attempt, UUID token, int generation) {
        return jdbc.query("""
                SELECT j.* FROM jobs j JOIN job_attempts a ON a.id=j.active_attempt_id
                WHERE j.id=:id AND j.status='RUNNING' AND j.generation=:generation
                    AND a.id=:attempt AND a.claim_token=:token AND a.status='RUNNING'
                FOR UPDATE OF j
                """,Map.of("id",id,"attempt",attempt,"token",token,"generation",generation),this::map).stream().findFirst();
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

    public int succeed(UUID id, JsonNode result) {
        return jdbc.update("""
                UPDATE jobs SET status = 'SUCCEEDED', result = CAST(:result AS jsonb),
                    completed_at = clock_timestamp(), active_attempt_id = NULL
                WHERE id = :id AND status = 'RUNNING' AND EXISTS (
                    SELECT 1 FROM job_attempts a WHERE a.id = jobs.active_attempt_id
                    AND a.status = 'RUNNING' AND a.lease_until > clock_timestamp() AND a.deadline_at > clock_timestamp())
                """, Map.of("id", id, "result", result.toString()));
    }

    public int fail(UUID id, String error) {
        return jdbc.update("""
                UPDATE jobs SET status = 'FAILED', error = :error, completed_at = clock_timestamp(), active_attempt_id = NULL
                WHERE id = :id AND status = 'RUNNING' AND EXISTS (
                    SELECT 1 FROM job_attempts a WHERE a.id = jobs.active_attempt_id
                    AND a.status = 'RUNNING' AND a.lease_until > clock_timestamp() AND a.deadline_at > clock_timestamp())
                """, Map.of("id", id, "error", error));
    }

    public void cancel(UUID id) {
        jdbc.update("""
                UPDATE jobs SET status = 'CANCELLED', completed_at = clock_timestamp(), active_attempt_id = NULL, retry_at = NULL WHERE id = :id
                """, Map.of("id", id));
        jdbc.update("DELETE FROM job_outbox WHERE job_id = :id", Map.of("id", id));
    }

    public void recordEvent(UUID id, JobEvent.Type type, JobStatus status, UUID relatedJobId) {
        recordEvent(id, type, status, relatedJobId, null);
    }

    public void recordEvent(UUID id, JobEvent.Type type, JobStatus status, UUID relatedJobId, UUID attemptId) {
        Map<String, Object> params = new HashMap<>();
        params.put("id", id);
        params.put("type", type.name());
        params.put("status", status.name());
        params.put("related", relatedJobId);
        params.put("attempt", attemptId);
        // Return metric inputs with the event write instead of performing two extra JDBC round trips.
        var sample=jdbc.queryForObject("""
                WITH inserted AS (
                    INSERT INTO job_events (job_id,event_type,status,related_job_id,attempt_id)
                    VALUES (:id,:type,:status,:related,:attempt) RETURNING job_id,attempt_id)
                SELECT j.type,a.queue_delay_seconds,a.status AS attempt_status,
                    CASE WHEN a.finished_at IS NULL THEN NULL
                         ELSE greatest(0,extract(epoch FROM (a.finished_at-a.started_at))) END::double precision AS execution_seconds
                FROM inserted e JOIN jobs j ON j.id=e.job_id LEFT JOIN job_attempts a ON a.id=e.attempt_id
                """,params,(rs,row)->new LifecycleMetrics.EventSample(JobType.valueOf(rs.getString("type")),
                        rs.getObject("queue_delay_seconds",Double.class),rs.getString("attempt_status"),
                        rs.getObject("execution_seconds",Double.class)));
        metrics.record(id,type,status,attemptId,sample);
    }

    public List<JobEvent> events(UUID id, int page, int size) {
        return jdbc.query("""
                SELECT * FROM job_events WHERE job_id = :id ORDER BY id LIMIT :size OFFSET :offset
                """, Map.of("id", id, "size", size + 1, "offset", (long) page * size),
                (rs, row) -> new JobEvent(rs.getLong("id"), rs.getObject("job_id", UUID.class),
                        JobEvent.Type.valueOf(rs.getString("event_type")),
                        JobStatus.valueOf(rs.getString("status")), rs.getObject("related_job_id", UUID.class),
                        rs.getTimestamp("occurred_at").toInstant(), rs.getObject("attempt_id", UUID.class)));
    }

    private Job map(ResultSet rs, int row) throws SQLException {
        try {
            String result = rs.getString("result");
            var completed = rs.getTimestamp("completed_at");
            var scheduled = rs.getTimestamp("scheduled_at");
            var deadline = rs.getTimestamp("deadline_at");
            return new Job(rs.getObject("id", UUID.class), JobType.valueOf(rs.getString("type")),
                    rs.getString("input_text"), JobStatus.valueOf(rs.getString("status")),
                    result == null ? null : mapper.readTree(result), rs.getString("error"),
                    rs.getTimestamp("created_at").toInstant(), completed == null ? null : completed.toInstant(),
                    rs.getInt("priority"), scheduled == null ? null : scheduled.toInstant(),
                    rs.getObject("retry_of", UUID.class), rs.getInt("timeout_seconds"),
                    rs.getInt("attempt_count"), rs.getObject("active_attempt_id", UUID.class),
                    deadline == null ? null : deadline.toInstant(), rs.getInt("generation"),
                    new RetryPolicy(rs.getInt("max_attempts"), rs.getInt("initial_backoff_seconds"), rs.getInt("max_backoff_seconds")),
                    rs.getTimestamp("retry_at") == null ? null : rs.getTimestamp("retry_at").toInstant(), rs.getObject("owner_id",UUID.class));
        } catch (JsonProcessingException exception) {
            throw new SQLException("Invalid stored job result", exception);
        }
    }
}
