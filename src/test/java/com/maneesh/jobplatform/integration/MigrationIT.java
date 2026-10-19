package com.maneesh.jobplatform.integration;

import java.util.UUID;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.test.annotation.DirtiesContext;
import static org.assertj.core.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
        "jobs.dispatcher.enabled=false", "jobs.worker.enabled=false"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class MigrationIT {
    @Autowired DataSource dataSource;
    @Autowired JdbcTemplate jdbc;

    @Test
    void upgradeRetainsExistingJobsAndRecordsOnlyTheirKnownBaseline() throws Exception {
        IntegrationDatabase.requireTestDatabase(jdbc);
        String schema = "upgrade_" + UUID.randomUUID().toString().replace("-", "");
        jdbc.execute("CREATE SCHEMA " + schema);
        try {
            Flyway.configure().dataSource(dataSource).schemas(schema).defaultSchema(schema).target("2")
                    .load().migrate();
            try (var connection = dataSource.getConnection()) {
                String originalSchema = connection.getSchema();
                try {
                    connection.setSchema(schema);
                    var isolated = new JdbcTemplate(new SingleConnectionDataSource(connection, true));
                    UUID queued = UUID.randomUUID();
                    UUID succeeded = UUID.randomUUID();
                    UUID failed = UUID.randomUUID();
                    isolated.update("""
                            INSERT INTO jobs (id,type,input_text,idempotency_key,priority,scheduled_at,created_at)
                            VALUES (?, 'SHA256','retained input','retained-key',7,'2030-01-01Z','2020-01-01Z')
                            """, queued);
                    isolated.update("INSERT INTO job_outbox(job_id,priority,available_at) VALUES (?,7,'2030-01-01Z')", queued);
                    isolated.update("""
                            INSERT INTO jobs(id,type,input_text,status,result,created_at,completed_at)
                            VALUES (?, 'UPPERCASE','hello','SUCCEEDED','{"text":"HELLO"}','2020-01-01Z','2020-01-02Z')
                            """, succeeded);
                    isolated.update("""
                            INSERT INTO jobs(id,type,input_text,status,error,completed_at)
                            VALUES (?, 'SHA256','failed','FAILED','Job handler failed',now())
                            """, failed);
                    Flyway.configure().dataSource(dataSource).schemas(schema).defaultSchema(schema).load().migrate();
                    assertThat(isolated.queryForObject("SELECT count(*) FROM jobs", Integer.class)).isEqualTo(3);
                    assertThat(isolated.queryForObject("SELECT input_text FROM jobs WHERE id=?", String.class, queued)).isEqualTo("retained input");
                    assertThat(isolated.queryForObject("SELECT priority FROM jobs WHERE id=?", Integer.class, queued)).isEqualTo(7);
                    assertThat(isolated.queryForObject("SELECT result->>'text' FROM jobs WHERE id=?", String.class, succeeded)).isEqualTo("HELLO");
                    assertThat(isolated.queryForObject("SELECT error FROM jobs WHERE id=?", String.class, failed)).isEqualTo("Job handler failed");
                    assertThat(isolated.queryForObject("SELECT count(*) FROM job_outbox", Integer.class)).isEqualTo(1);
                    assertThat(isolated.queryForList("SELECT event_type FROM job_events", String.class)).containsOnly("BASELINE").hasSize(3);
                    assertThat(isolated.queryForList("SELECT status FROM job_events", String.class)).containsExactlyInAnyOrder("QUEUED", "SUCCEEDED", "FAILED");
                    assertThat(isolated.queryForObject("SELECT count(*) FROM jobs WHERE retry_of IS NOT NULL", Integer.class)).isZero();
                    assertThat(isolated.queryForObject("SELECT count(*) FROM jobs WHERE max_attempts=25 AND legacy_retry_policy", Integer.class)).isEqualTo(3);
                    assertThat(isolated.queryForObject("SELECT count(*) FROM jobs WHERE owner_id='00000000-0000-0000-0000-000000000001'",Integer.class)).isEqualTo(3);
                    assertThat(isolated.queryForObject("SELECT count(*) FROM dead_letters", Integer.class)).isZero();
                    isolated.update("UPDATE jobs SET status='CANCELLED',completed_at=clock_timestamp() WHERE id=?", queued);
                    assertThat(isolated.queryForObject("SELECT status FROM jobs WHERE id=?", String.class, queued)).isEqualTo("CANCELLED");
                    Flyway.configure().dataSource(dataSource).schemas(schema).defaultSchema(schema).load().migrate();
                    assertThat(isolated.queryForObject("SELECT count(*) FROM job_events", Integer.class)).isEqualTo(3);
                } finally {
                    connection.setSchema(originalSchema);
                }
            }
        } finally {
            jdbc.execute("DROP SCHEMA " + schema + " CASCADE");
        }
    }

    @Test
    void phaseOneRunningAttemptAndOutboxSurviveRetryPolicyUpgrade() throws Exception {
        IntegrationDatabase.requireTestDatabase(jdbc);
        String schema = "upgrade_" + UUID.randomUUID().toString().replace("-", "");
        jdbc.execute("CREATE SCHEMA " + schema);
        try {
            Flyway.configure().dataSource(dataSource).schemas(schema).defaultSchema(schema).target("4").load().migrate();
            try (var connection = dataSource.getConnection()) {
                String original = connection.getSchema();
                try {
                    connection.setSchema(schema);
                    var isolated = new JdbcTemplate(new SingleConnectionDataSource(connection, true));
                    UUID job = UUID.randomUUID();
                    UUID attempt = UUID.randomUUID();
                    UUID token = UUID.randomUUID();
                    isolated.update("INSERT INTO jobs(id,type,input_text,generation) VALUES (?,'UPPERCASE','preserve running job',2)", job);
                    isolated.update("""
                            INSERT INTO job_attempts(id,job_id,attempt_number,generation,claim_token,worker_id,status,
                                started_at,heartbeat_at,lease_until,deadline_at)
                            VALUES (?,?,3,2,?,'existing-worker','RUNNING',clock_timestamp(),clock_timestamp(),
                                clock_timestamp()+interval '10 seconds',clock_timestamp()+interval '30 seconds')
                            """, attempt, job, token);
                    isolated.update("""
                            UPDATE jobs SET status='RUNNING',active_attempt_id=?,attempt_count=3,recovery_count=2,
                                deadline_at=(SELECT deadline_at FROM job_attempts WHERE id=?) WHERE id=?
                            """, attempt, attempt, job);
                    isolated.update("INSERT INTO job_outbox(job_id,priority,generation) VALUES (?,0,2)", job);
                    var before = isolated.queryForMap("SELECT status,active_attempt_id,attempt_count,recovery_count,deadline_at,generation FROM jobs WHERE id=?", job);
                    var attemptBefore = isolated.queryForMap("SELECT * FROM job_attempts WHERE id=?", attempt);
                    Flyway.configure().dataSource(dataSource).schemas(schema).defaultSchema(schema).load().migrate();
                    assertThat(isolated.queryForMap("SELECT status,active_attempt_id,attempt_count,recovery_count,deadline_at,generation FROM jobs WHERE id=?", job)).isEqualTo(before);
                    var attemptAfter = isolated.queryForMap("SELECT * FROM job_attempts WHERE id=?", attempt);
                    assertThat(attemptAfter.remove("failure_kind")).isNull();
                    assertThat(attemptAfter).isEqualTo(attemptBefore);
                    assertThat(isolated.queryForObject("SELECT max_attempts FROM jobs WHERE id=?", Integer.class, job)).isEqualTo(25);
                    assertThat(isolated.queryForObject("SELECT generation FROM job_outbox WHERE job_id=?", Integer.class, job)).isEqualTo(2);
                    assertThat(isolated.queryForObject("SELECT owner_id FROM jobs WHERE id=?",UUID.class,job)).isEqualTo(com.maneesh.jobplatform.auth.ApiPrincipal.LEGACY_OWNER);
                    assertThat(isolated.queryForObject("SELECT count(*) FROM dead_letters", Integer.class)).isZero();
                } finally { connection.setSchema(original); }
            }
        } finally { jdbc.execute("DROP SCHEMA " + schema + " CASCADE"); }
    }
}
