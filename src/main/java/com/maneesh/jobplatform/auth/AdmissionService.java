package com.maneesh.jobplatform.auth;

import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
public class AdmissionService {
    private final NamedParameterJdbcTemplate jdbc;
    public AdmissionService(NamedParameterJdbcTemplate jdbc) { this.jdbc=jdbc; }

    /** Charges only new jobs, inside their creation transaction. One row per owner across all API instances. */
    @Transactional(propagation = Propagation.MANDATORY)
    public void acquire(UUID owner) {
        var admitted = jdbc.query("""
                WITH timing AS MATERIALIZED (SELECT date_trunc('minute',clock_timestamp()) AS minute)
                INSERT INTO submission_usage(owner_id,window_start,used)
                SELECT o.id,t.minute,1 FROM owners o CROSS JOIN timing t WHERE o.id=:owner
                ON CONFLICT(owner_id) DO UPDATE SET window_start=EXCLUDED.window_start,
                    used=CASE WHEN submission_usage.window_start=EXCLUDED.window_start THEN submission_usage.used+1 ELSE 1 END
                WHERE submission_usage.window_start<>EXCLUDED.window_start
                   OR submission_usage.used<(SELECT submission_limit_per_minute FROM owners WHERE id=:owner)
                RETURNING used
                """, Map.of("owner",owner), (rs,n) -> rs.getInt("used"));
        if (admitted.isEmpty()) {
            int retryAfter = jdbc.queryForObject("SELECT greatest(1,ceil(extract(epoch FROM (date_trunc('minute',clock_timestamp())+interval '1 minute'-clock_timestamp()))))::int", Map.of(), Integer.class);
            throw new Limited(retryAfter);
        }
    }
    public static final class Limited extends RuntimeException {
        private final int retryAfter;
        public Limited(int retryAfter) { super("Owner submission limit exceeded"); this.retryAfter=retryAfter; }
        public int retryAfter() { return retryAfter; }
    }
}
