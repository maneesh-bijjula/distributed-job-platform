package com.maneesh.jobplatform.integration;

import com.maneesh.jobplatform.config.RabbitConfiguration;
import com.maneesh.jobplatform.jobs.JobService;
import com.maneesh.jobplatform.jobs.JobStatus;
import com.maneesh.jobplatform.jobs.JobType;
import com.maneesh.jobplatform.messaging.JobPublisher;
import com.maneesh.jobplatform.messaging.OutboxRelay;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
        "jobs.dispatcher.enabled=false", "jobs.worker.enabled=false"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class SchedulingIT {
    @Autowired JobService service;
    @Autowired OutboxRelay relay;
    @Autowired JobPublisher publisher;
    @Autowired RabbitTemplate rabbit;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach
    void reset() {
        jdbc.execute("TRUNCATE job_outbox, jobs");
        var admin = new RabbitAdmin(rabbit.getConnectionFactory());
        admin.initialize();
        admin.purgeQueue(RabbitConfiguration.QUEUE);
        admin.purgeQueue(RabbitConfiguration.LEGACY_QUEUE);
    }

    @Test
    void futureJobsStayInOutboxUntilDue() {
        var future = service.submit(JobType.SHA256, "future", null, 9, Instant.now().plusSeconds(3600)).job();
        var immediate = service.submit(JobType.SHA256, "ready", null, 0, null).job();
        assertThat(relay.dispatchBatch()).isEqualTo(1);
        assertThat(rabbit.receiveAndConvert(RabbitConfiguration.QUEUE)).isEqualTo(immediate.id().toString());
        assertThat(rabbit.receive(RabbitConfiguration.QUEUE)).isNull();
        assertThat(service.get(future.id()).status()).isEqualTo(JobStatus.QUEUED);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM job_outbox", Integer.class)).isEqualTo(1);

        // Move the outbox deadline instead of waiting an hour in a test.
        jdbc.update("UPDATE job_outbox SET available_at = now() - interval '1 second' WHERE job_id = ?", future.id());
        assertThat(relay.dispatchBatch()).isEqualTo(1);
        assertThat(rabbit.receiveAndConvert(RabbitConfiguration.QUEUE)).isEqualTo(future.id().toString());
    }

    @Test
    void relaySelectsHighestPriorityAcrossBatchBoundary() {
        for (int i = 0; i < 20; i++) service.submit(JobType.SHA256, "low " + i, null, 0, null);
        var urgent = service.submit(JobType.SHA256, "urgent", null, 9, null).job();
        assertThat(relay.dispatchBatch()).isEqualTo(20);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM job_outbox WHERE job_id = ?",
                Integer.class, urgent.id())).isZero();
        var first = rabbit.receive(RabbitConfiguration.QUEUE);
        assertThat(first).isNotNull();
        assertThat(new String(first.getBody(), java.nio.charset.StandardCharsets.UTF_8))
                .isEqualTo(urgent.id().toString());
        assertThat(first.getMessageProperties().getPriority()).isEqualTo(9);
    }

    @Test
    void rabbitMqReordersWaitingMessagesByPriority() {
        UUID low = UUID.randomUUID();
        UUID high = UUID.randomUUID();
        publisher.publish(low, 0);
        publisher.publish(high, 9);
        assertThat(rabbit.receiveAndConvert(RabbitConfiguration.QUEUE)).isEqualTo(high.toString());
        assertThat(rabbit.receiveAndConvert(RabbitConfiguration.QUEUE)).isEqualTo(low.toString());
        assertThat(rabbit.receive(RabbitConfiguration.LEGACY_QUEUE)).isNull();
    }
}
