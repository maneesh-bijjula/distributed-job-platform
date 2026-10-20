package com.maneesh.jobplatform.integration;

import com.maneesh.jobplatform.jobs.JobService;
import com.maneesh.jobplatform.jobs.JobType;
import com.maneesh.jobplatform.messaging.OutboxRelay;
import com.maneesh.jobplatform.config.CapacityProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment=SpringBootTest.WebEnvironment.NONE,properties={
        "jobs.dispatcher.enabled=false","jobs.worker.enabled=false","jobs.dispatcher.batch-size=3",
        "jobs.capacity.database-pool-size=6","jobs.capacity.prefetch=1","jobs.capacity.worker-concurrency=1"})
@DirtiesContext(classMode=DirtiesContext.ClassMode.AFTER_CLASS)
class DispatchCapacityIT {
    @Autowired JobService jobs;
    @Autowired OutboxRelay relay;
    @Autowired JdbcTemplate jdbc;
    @Autowired CapacityProperties capacity;
    @BeforeEach void reset() { IntegrationDatabase.reset(jdbc); }

    @Test void configuredBatchBoundsRealDispatchAndPreservesRemainingWork() {
        for(int i=0;i<8;i++) jobs.submit(JobType.SHA256,"bounded relay "+i,null);
        assertThat(relay.dispatchBatch()).isEqualTo(3);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM job_outbox",Integer.class)).isEqualTo(5);
        assertThat(relay.dispatchBatch()).isEqualTo(3);
        assertThat(relay.dispatchBatch()).isEqualTo(2);
        assertThat(relay.dispatchBatch()).isZero();
    }
    @Test void validatedCapacityReachesTheActualConnectionPool() {
        var hikari=(com.zaxxer.hikari.HikariDataSource)jdbc.getDataSource();
        assertThat(hikari.getMaximumPoolSize()).isEqualTo(6);
        assertThat(capacity.prefetch()).isEqualTo(1);
        assertThat(capacity.workerConcurrency()).isEqualTo(1);
    }
}
