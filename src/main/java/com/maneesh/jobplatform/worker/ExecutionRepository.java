package com.maneesh.jobplatform.worker;

import com.maneesh.jobplatform.jobs.Job;
import com.maneesh.jobplatform.jobs.JobAttempt;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class ExecutionRepository {
    private final NamedParameterJdbcTemplate jdbc;

    public ExecutionRepository(NamedParameterJdbcTemplate jdbc) { this.jdbc = jdbc; }

    public record Window(boolean leaseLive, boolean deadlineLive, long leaseMillis, long deadlineMillis) {}

    public JobAttempt start(Job job, UUID id, UUID token, String workerId, int leaseSeconds) {
        Map<String, Object> params = new HashMap<>();
        params.put("job", job.id());
        params.put("id", id);
        params.put("token", token);
        params.put("worker", workerId);
        params.put("number", job.attemptCount() + 1);
        params.put("generation", job.generation());
        params.put("lease", leaseSeconds);
        params.put("timeout", job.timeoutSeconds());
        params.put("deadline", job.deadlineAt() == null ? null : Timestamp.from(job.deadlineAt()));
        JobAttempt attempt = jdbc.query("""
                WITH timing AS MATERIALIZED (SELECT clock_timestamp() AS at),
                     deadline AS (SELECT at, COALESCE(CAST(:deadline AS timestamptz), at + make_interval(secs => :timeout)) AS until FROM timing)
                INSERT INTO job_attempts(id,job_id,attempt_number,generation,claim_token,worker_id,status,
                    started_at,heartbeat_at,lease_until,deadline_at)
                SELECT :id,:job,:number,:generation,:token,:worker,'RUNNING',at,at,
                    LEAST(at + make_interval(secs => :lease),until),until FROM deadline RETURNING *
                """, params, this::map).getFirst();
        jdbc.update("""
                UPDATE jobs SET status='RUNNING',active_attempt_id=:attempt,attempt_count=attempt_count+1,
                    deadline_at=:deadline WHERE id=:id
                """, Map.of("id", job.id(), "attempt", id, "deadline", Timestamp.from(attempt.deadlineAt())));
        return attempt;
    }

    public Optional<JobAttempt> owned(UUID id, UUID token) {
        return jdbc.query("SELECT * FROM job_attempts WHERE id=:id AND claim_token=:token AND status='RUNNING'",
                Map.of("id", id, "token", token), this::map).stream().findFirst();
    }

    public Optional<JobAttempt> find(UUID id) {
        return jdbc.query("SELECT * FROM job_attempts WHERE id=:id", Map.of("id", id), this::map).stream().findFirst();
    }

    public Window window(UUID id) {
        return jdbc.query("""
                WITH timing AS MATERIALIZED (SELECT clock_timestamp() AS at)
                SELECT lease_until > at AS lease_live, deadline_at > at AS deadline_live,
                    greatest(0,extract(epoch FROM (lease_until-at))*1000)::bigint AS lease_ms,
                    greatest(0,extract(epoch FROM (deadline_at-at))*1000)::bigint AS deadline_ms
                FROM job_attempts,timing WHERE id=:id
                """, Map.of("id", id), (rs, n) -> new Window(rs.getBoolean("lease_live"), rs.getBoolean("deadline_live"),
                rs.getLong("lease_ms"), rs.getLong("deadline_ms"))).getFirst();
    }

    public boolean renew(UUID id, UUID token, int leaseSeconds) {
        return jdbc.update("""
                WITH timing AS MATERIALIZED (SELECT clock_timestamp() AS at)
                UPDATE job_attempts SET heartbeat_at=at,lease_until=LEAST(at+make_interval(secs => :lease),deadline_at)
                FROM timing WHERE id=:id AND claim_token=:token AND status='RUNNING'
                    AND lease_until>at AND deadline_at>at
                """, Map.of("id", id, "token", token, "lease", leaseSeconds)) == 1;
    }

    public void finish(UUID id, JobAttempt.Status status, String error) {
        Map<String, Object> params = new HashMap<>();
        params.put("id", id);
        params.put("status", status.name());
        params.put("error", error);
        jdbc.update("UPDATE job_attempts SET status=:status,error=:error,finished_at=clock_timestamp() WHERE id=:id AND status='RUNNING'", params);
    }

    public boolean due(UUID id) {
        return Boolean.TRUE.equals(jdbc.queryForObject("SELECT coalesce(scheduled_at,created_at)<=clock_timestamp() FROM jobs WHERE id=:id", Map.of("id", id), Boolean.class));
    }

    public boolean deadlineExpired(UUID id) {
        return Boolean.TRUE.equals(jdbc.queryForObject("SELECT deadline_at<=clock_timestamp() FROM jobs WHERE id=:id", Map.of("id", id), Boolean.class));
    }

    public void failJob(UUID id, String error) {
        jdbc.update("UPDATE jobs SET status='FAILED',error=:error,completed_at=clock_timestamp(),active_attempt_id=NULL WHERE id=:id", Map.of("id", id, "error", error));
    }

    public int recoveryCount(UUID id) {
        return jdbc.queryForObject("SELECT recovery_count FROM jobs WHERE id=:id", Map.of("id", id), Integer.class);
    }

    public void requeue(UUID id) {
        jdbc.update("UPDATE jobs SET status='QUEUED',active_attempt_id=NULL,generation=generation+1,recovery_count=recovery_count+1 WHERE id=:id", Map.of("id", id));
        // The outbox replacement and generation advance commit together.
        jdbc.update("""
                INSERT INTO job_outbox(job_id,priority,available_at,generation)
                SELECT id,priority,clock_timestamp(),generation FROM jobs WHERE id=:id
                ON CONFLICT(job_id) DO UPDATE SET generation=EXCLUDED.generation,available_at=EXCLUDED.available_at,
                    priority=EXCLUDED.priority
                """, Map.of("id", id));
    }

    public List<UUID> expiredJobs() {
        return jdbc.query("""
                SELECT j.id FROM jobs j LEFT JOIN job_attempts a ON a.id=j.active_attempt_id
                WHERE (j.status='RUNNING' AND (a.lease_until<=clock_timestamp() OR a.deadline_at<=clock_timestamp()))
                   OR (j.status='QUEUED' AND j.deadline_at<=clock_timestamp())
                ORDER BY j.deadline_at,j.id LIMIT 20 FOR UPDATE OF j SKIP LOCKED
                """, Map.of(), (rs, n) -> rs.getObject("id", UUID.class));
    }

    public List<JobAttempt> list(UUID job, int page, int size) {
        return jdbc.query("SELECT * FROM job_attempts WHERE job_id=:job ORDER BY attempt_number LIMIT :size OFFSET :offset",
                Map.of("job", job, "size", size + 1, "offset", (long) page * size), this::map);
    }

    private JobAttempt map(ResultSet rs, int n) throws SQLException {
        Timestamp finished = rs.getTimestamp("finished_at");
        return new JobAttempt(rs.getObject("id", UUID.class), rs.getObject("job_id", UUID.class),
                rs.getInt("attempt_number"), rs.getInt("generation"), rs.getString("worker_id"),
                JobAttempt.Status.valueOf(rs.getString("status")), rs.getTimestamp("started_at").toInstant(),
                rs.getTimestamp("heartbeat_at").toInstant(), rs.getTimestamp("lease_until").toInstant(),
                rs.getTimestamp("deadline_at").toInstant(), finished == null ? null : finished.toInstant(), rs.getString("error"));
    }
}
