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
}
