package com.maneesh.jobplatform.deadletter;

import com.maneesh.jobplatform.jobs.Job;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class DeadLetterRepository {
    private final NamedParameterJdbcTemplate jdbc;
    public DeadLetterRepository(NamedParameterJdbcTemplate jdbc) { this.jdbc = jdbc; }

    public void execution(Job job, DeadLetter.Reason reason) {
        archive("execution:" + job.id() + ":" + job.generation(), DeadLetter.Source.EXECUTION,
                reason, job.id(), job.generation(), null, null);
    }

    public UUID archive(String dedup, DeadLetter.Source source, DeadLetter.Reason reason,
                        UUID jobId, Integer generation, String hash, Integer bytes) {
        var params = new HashMap<String, Object>();
        params.put("id", UUID.randomUUID());
        params.put("dedup", dedup);
        params.put("source", source.name());
        params.put("reason", reason.name());
        params.put("job", jobId);
        params.put("generation", generation);
        params.put("hash", hash);
        params.put("bytes", bytes);
        // A redelivery after commit-before-ack must retain the original record and resolution.
        jdbc.update("""
                INSERT INTO dead_letters(id,dedup_key,source,reason,job_id,generation,payload_sha256,payload_bytes)
                VALUES (:id,:dedup,:source,:reason,:job,:generation,:hash,:bytes)
                ON CONFLICT(dedup_key) DO NOTHING
                """, params);
        return jdbc.queryForObject("SELECT id FROM dead_letters WHERE dedup_key=:dedup", params, UUID.class);
    }

    public Optional<DeadLetter> find(UUID id) { return query("SELECT * FROM dead_letters WHERE id=:id", Map.of("id", id)); }
    public Optional<DeadLetter> lock(UUID id) { return query("SELECT * FROM dead_letters WHERE id=:id FOR UPDATE", Map.of("id", id)); }
    public Optional<DeadLetter> resolution(String key) {
        return query("SELECT * FROM dead_letters WHERE resolution_key=:key", Map.of("key", key));
    }

    public void lockKey(String key) {
        // Serialize reuse of one global resolution key before locking an inbox row.
        jdbc.query("SELECT pg_advisory_xact_lock(hashtextextended(:key,0))", Map.of("key", key), rs -> {});
    }

    public void resolve(UUID id, DeadLetter.State state, String key, UUID job) {
        var params = new HashMap<String, Object>();
        params.put("id", id); params.put("state", state.name()); params.put("key", key); params.put("job", job);
        jdbc.update("""
                UPDATE dead_letters SET state=:state,resolution_key=:key,resolved_at=clock_timestamp(),recovery_job_id=:job
                WHERE id=:id AND state='OPEN'
                """, params);
    }

    public List<DeadLetter> list(DeadLetter.State state, int page, int size) {
        var params = new HashMap<String, Object>();
        params.put("size", size + 1); params.put("offset", (long) page * size);
        String filter = "";
        if (state != null) { filter = " WHERE state=:state"; params.put("state", state.name()); }
        return jdbc.query("SELECT * FROM dead_letters" + filter + " ORDER BY captured_at,id LIMIT :size OFFSET :offset", params, this::map);
    }

    private Optional<DeadLetter> query(String sql, Map<String, ?> params) {
        return jdbc.query(sql, params, this::map).stream().findFirst();
    }

    private DeadLetter map(ResultSet rs, int row) throws SQLException {
        var resolved = rs.getTimestamp("resolved_at");
        return new DeadLetter(rs.getObject("id", UUID.class), DeadLetter.Source.valueOf(rs.getString("source")),
                DeadLetter.Reason.valueOf(rs.getString("reason")), rs.getObject("job_id", UUID.class),
                rs.getObject("generation", Integer.class), rs.getString("payload_sha256"), rs.getObject("payload_bytes", Integer.class),
                rs.getTimestamp("captured_at").toInstant(), DeadLetter.State.valueOf(rs.getString("state")),
                resolved == null ? null : resolved.toInstant(), rs.getObject("recovery_job_id", UUID.class));
    }
}
