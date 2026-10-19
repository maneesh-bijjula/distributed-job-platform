package com.maneesh.jobplatform.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.maneesh.jobplatform.config.RabbitConfiguration;
import com.maneesh.jobplatform.jobs.JobRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.dao.DataAccessResourceFailureException;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "jobs.dispatcher.delay-ms=100", "jobs.worker.enabled=true", "jobs.dispatcher.enabled=true",
        "jobs.execution.lease-seconds=3", "jobs.execution.heartbeat-millis=100"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@AutoConfigureObservability
class PipelineIT {
    @Autowired TestRestTemplate http;
    @Autowired JdbcTemplate jdbc;
    @Autowired RabbitTemplate rabbit;
    @MockitoSpyBean JobRepository repository;
    @Autowired com.maneesh.jobplatform.worker.ExecutionService execution;
    @MockitoSpyBean com.maneesh.jobplatform.worker.JobProcessor processor;

    @BeforeEach
    void resetDatabase() {
        IntegrationDatabase.reset(jdbc);
    }

    private JsonNode completed(String location) {
        await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(100)).untilAsserted(() ->
                assertThat(http.getForObject(location, JsonNode.class).get("status").asText())
                        .isEqualTo("SUCCEEDED"));
        return http.getForObject(location, JsonNode.class);
    }

    @ParameterizedTest
    @CsvSource({"WORD_COUNT,hello distributed world,wordCount,3", "UPPERCASE,hello,text,HELLO",
            "SHA256,abc,sha256,ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"})
    void submittedJobTravelsThroughPostgresRabbitMqAndWorker(String type, String text, String field, String expected) {
        var response = http.postForEntity("/api/v1/jobs", Map.of("type", type, "text", text), JsonNode.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getHeaders().getLocation()).isNotNull();
        JsonNode done = completed(response.getHeaders().getLocation().toString());
        assertThat(done.get("result").get(field).asText()).isEqualTo(expected);
        assertThat(done.get("completedAt").isNull()).isFalse();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(jdbc.queryForObject("SELECT count(*) FROM job_outbox", Integer.class)).isZero());
    }

    @Test
    void idempotentHttpReplayReturnsExistingJobAndChangedInputConflicts() {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Idempotency-Key", "http-replay");
        var body = Map.of("type", "WORD_COUNT", "text", "one two");
        var first = http.postForEntity("/api/v1/jobs", new HttpEntity<>(body, headers), JsonNode.class);
        var replay = http.postForEntity("/api/v1/jobs", new HttpEntity<>(body, headers), JsonNode.class);
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(replay.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(replay.getBody().get("id")).isEqualTo(first.getBody().get("id"));
        var changed = http.postForEntity("/api/v1/jobs",
                new HttpEntity<>(Map.of("type", "WORD_COUNT", "text", "changed"), headers), JsonNode.class);
        assertThat(changed.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        completed(first.getHeaders().getLocation().toString());
    }

    @Test
    void unknownJobReturns404AndMalformedIdReturns400() {
        assertThat(http.getForEntity("/api/v1/jobs/" + UUID.randomUUID(), JsonNode.class).getStatusCode())
                .isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(http.getForEntity("/api/v1/jobs/bad-id", JsonNode.class).getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void healthAndPrometheusAreAvailable() {
        assertThat(http.getForEntity("/actuator/health", JsonNode.class).getStatusCode()).isEqualTo(HttpStatus.OK);
        var metrics = http.getForEntity("/actuator/prometheus", String.class);
        assertThat(metrics.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(metrics.getBody()).contains("jvm_memory_used_bytes");
    }

    @Test
    void malformedQueueMessagesReachDeadLetterQueue() {
        var admin = new RabbitAdmin(rabbit.getConnectionFactory());
        admin.purgeQueue(RabbitConfiguration.DEAD_QUEUE);
        rabbit.convertAndSend(RabbitConfiguration.EXCHANGE, RabbitConfiguration.ROUTING_KEY, "invalid-uuid");
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(admin.getQueueInfo(RabbitConfiguration.DEAD_QUEUE).getMessageCount()).isEqualTo(1));
        assertThat(rabbit.receiveAndConvert(RabbitConfiguration.DEAD_QUEUE)).isEqualTo("invalid-uuid");
    }

    @Test
    void scheduledHttpJobExecutesOnlyAfterItsDeadline() {
        Instant scheduled = Instant.now().plusSeconds(3).truncatedTo(java.time.temporal.ChronoUnit.MICROS);
        var response = http.postForEntity("/api/v1/jobs", Map.of("type", "UPPERCASE", "text", "later",
                "priority", 7, "scheduledAt", scheduled.toString()), JsonNode.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        String location = response.getHeaders().getLocation().toString();
        assertThat(http.getForObject(location, JsonNode.class).get("status").asText()).isEqualTo("QUEUED");
        JsonNode done = completed(location);
        assertThat(done.get("result").get("text").asText()).isEqualTo("LATER");
        assertThat(done.get("priority").asInt()).isEqualTo(7);
        assertThat(Instant.parse(done.get("completedAt").asText())).isAfterOrEqualTo(scheduled);
    }

    @Test
    void legacyQueueJobsAreStillProcessedAfterUpgrade() {
        UUID id = UUID.randomUUID();
        jdbc.update("INSERT INTO jobs (id, type, input_text) VALUES (?, 'UPPERCASE', 'legacy')", id);
        rabbit.convertAndSend(RabbitConfiguration.EXCHANGE, RabbitConfiguration.LEGACY_ROUTING_KEY, id.toString());
        assertThat(completed("/api/v1/jobs/" + id).get("result").get("text").asText()).isEqualTo("LEGACY");
    }

    @Test
    void cancellationRetryAndHistoryTravelThroughTheRealHttpAndWorkerPipeline() {
        var created = http.postForEntity("/api/v1/jobs", Map.of("type", "UPPERCASE", "text", "retry this",
                "priority", 8, "scheduledAt", Instant.now().plusSeconds(3600).toString()), JsonNode.class);
        String original = created.getHeaders().getLocation().toString();
        assertThat(http.postForEntity(original + "/cancel", null, JsonNode.class).getBody().get("status").asText())
                .isEqualTo("CANCELLED");
        HttpHeaders headers = new HttpHeaders();
        headers.set("Idempotency-Key", "pipeline-retry");
        var retry = http.postForEntity(original + "/retry", new HttpEntity<>(headers), JsonNode.class);
        assertThat(retry.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        String child = retry.getHeaders().getLocation().toString();
        assertThat(retry.getBody().get("retryOf")).isEqualTo(created.getBody().get("id"));
        assertThat(completed(child).get("result").get("text").asText()).isEqualTo("RETRY THIS");
        var replay = http.postForEntity(original + "/retry", new HttpEntity<>(headers), JsonNode.class);
        assertThat(replay.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(replay.getHeaders().getLocation()).isEqualTo(retry.getHeaders().getLocation());
        assertThat(http.postForEntity(original + "/cancel", null, JsonNode.class).getStatusCode()).isEqualTo(HttpStatus.OK);
        var history = http.getForObject(original + "/events?size=2", JsonNode.class);
        assertThat(history.get("items")).hasSize(2);
        assertThat(history.get("hasNext").asBoolean()).isTrue();
        assertThat(history.get("items").get(0).get("type").asText()).isEqualTo("SUBMITTED");
        assertThat(history.get("items").get(1).get("type").asText()).isEqualTo("CANCELLED");
        var last = http.getForObject(original + "/events?page=1&size=2", JsonNode.class);
        assertThat(last.get("items")).hasSize(1);
        assertThat(last.get("items").get(0).get("type").asText()).isEqualTo("RETRY_REQUESTED");
        assertThat(last.get("items").get(0).get("relatedJobId")).isEqualTo(retry.getBody().get("id"));
        var cancelled = http.getForObject("/api/v1/jobs?status=CANCELLED", JsonNode.class);
        assertThat(cancelled.get("items")).hasSize(1);
        var childHistory = http.getForObject(child + "/events", JsonNode.class).get("items");
        assertThat(childHistory).hasSize(3);
        assertThat(childHistory.get(2).get("type").asText()).isEqualTo("SUCCEEDED");
    }

    @Test
    void controlAndHistoryEndpointsReturn404ForMissingJobs() {
        String missing = "/api/v1/jobs/" + UUID.randomUUID();
        assertThat(http.postForEntity(missing + "/cancel", null, JsonNode.class).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        HttpHeaders headers = new HttpHeaders();
        headers.set("Idempotency-Key", "missing-retry");
        assertThat(http.postForEntity(missing + "/retry", new HttpEntity<>(headers), JsonNode.class).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(http.getForEntity(missing + "/events", JsonNode.class).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void deadLetteredJobRecoversThroughCancelAndRetryAfterInfrastructureIsFixed() {
        var admin = new RabbitAdmin(rabbit.getConnectionFactory());
        admin.purgeQueue(RabbitConfiguration.DEAD_QUEUE);
        doThrow(new DataAccessResourceFailureException("simulated persistence outage"))
                .when(repository).lock(any());
        var response = http.postForEntity("/api/v1/jobs", Map.of("type", "UPPERCASE", "text", "recover me"), JsonNode.class);
        String source = response.getHeaders().getLocation().toString();
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(admin.getQueueInfo(RabbitConfiguration.DEAD_QUEUE).getMessageCount()).isEqualTo(1));
        assertThat(http.getForObject(source, JsonNode.class).get("status").asText()).isEqualTo("QUEUED");
        doCallRealMethod().when(repository).lock(any());
        assertThat(http.postForEntity(source + "/cancel", null, JsonNode.class).getStatusCode()).isEqualTo(HttpStatus.OK);
        HttpHeaders headers = new HttpHeaders();
        headers.set("Idempotency-Key", "recover-dead-letter");
        var retry = http.postForEntity(source + "/retry", new HttpEntity<>(headers), JsonNode.class);
        assertThat(retry.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(completed(retry.getHeaders().getLocation().toString()).get("result").get("text").asText()).isEqualTo("RECOVER ME");
        assertThat(http.getForObject(source, JsonNode.class).get("status").asText()).isEqualTo("CANCELLED");
        // An operator might later republish the original dead-letter message.
        Object oldMessage = rabbit.receiveAndConvert(RabbitConfiguration.DEAD_QUEUE);
        assertThat(oldMessage).isEqualTo(response.getBody().get("id").asText());
        rabbit.convertAndSend(RabbitConfiguration.EXCHANGE, RabbitConfiguration.ROUTING_KEY, oldMessage);
        var events = http.getForObject(source + "/events", JsonNode.class).get("items");
        assertThat(events).hasSize(3);
        assertThat(events.get(1).get("type").asText()).isEqualTo("CANCELLED");
        assertThat(events.get(2).get("type").asText()).isEqualTo("RETRY_REQUESTED");
    }

    @Test
    void abandonedCommittedClaimIsRecoveredThroughSchedulerOutboxBrokerAndWorker() {
        UUID id = UUID.randomUUID();
        // This fixture represents a worker that died after its claim committed.
        jdbc.update("INSERT INTO jobs(id,type,input_text) VALUES (?,'UPPERCASE','recovered worker')", id);
        var lost = execution.claim(id, 0).orElseThrow();
        jdbc.update("UPDATE job_attempts SET lease_until=clock_timestamp()-interval '1 second' WHERE id=?", lost.attempt().id());
        var result = completed("/api/v1/jobs/" + id);
        assertThat(result.get("attemptCount").asInt()).isEqualTo(2);
        assertThat(result.get("result").get("text").asText()).isEqualTo("RECOVERED WORKER");
        var history = http.getForObject("/api/v1/jobs/" + id + "/attempts", JsonNode.class);
        assertThat(history.get("items")).hasSize(2);
        assertThat(history.get("items").get(0).get("status").asText()).isEqualTo("ABANDONED");
        assertThat(history.get("items").get(1).get("status").asText()).isEqualTo("SUCCEEDED");
        assertThat(history.get("items").get(1).get("generation").asInt()).isEqualTo(1);
        assertThat(history.toString()).doesNotContain("claimToken", "claim_token");
    }

    @Test
    void runningJobCanBeObservedAndCooperativelyCancelledThroughHttp() throws Exception {
        var entered = new CountDownLatch(1);
        var stopped = new CountDownLatch(1);
        doAnswer(call -> {
            entered.countDown();
            try {
                ((com.maneesh.jobplatform.worker.ExecutionControl) call.getArgument(2)).pause(Duration.ofSeconds(20));
            } finally { stopped.countDown(); }
            throw new AssertionError("Cancelled handler must stop before returning a result");
        }).when(processor).process(eq(com.maneesh.jobplatform.jobs.JobType.UPPERCASE), eq("cancel in flight"),
                any(com.maneesh.jobplatform.worker.ExecutionControl.class));
        var created = http.postForEntity("/api/v1/jobs", Map.of("type", "UPPERCASE", "text", "cancel in flight", "timeoutSeconds", 10), JsonNode.class);
        String location = created.getHeaders().getLocation().toString();
        try {
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            var running = http.getForObject(location, JsonNode.class);
            assertThat(running.get("status").asText()).isEqualTo("RUNNING");
            assertThat(running.get("activeAttemptId").isNull()).isFalse();
            assertThat(running.get("attemptCount").asInt()).isEqualTo(1);
            var cancelled = http.postForEntity(location + "/cancel", null, JsonNode.class);
            assertThat(cancelled.getStatusCode()).isEqualTo(HttpStatus.OK);
            assertThat(cancelled.getBody().get("status").asText()).isEqualTo("CANCELLED");
            assertThat(stopped.await(3, TimeUnit.SECONDS)).isTrue();
            var attempts = http.getForObject(location + "/attempts", JsonNode.class).get("items");
            assertThat(attempts.get(0).get("status").asText()).isEqualTo("CANCELLED");
            assertThat(http.getForObject(location, JsonNode.class).get("result").isNull()).isTrue();
        } finally {
            http.postForEntity(location + "/cancel", null, JsonNode.class);
            stopped.await(3, TimeUnit.SECONDS);
        }
    }
}
