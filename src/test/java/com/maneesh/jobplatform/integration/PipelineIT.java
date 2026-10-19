package com.maneesh.jobplatform.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.maneesh.jobplatform.config.RabbitConfiguration;
import com.maneesh.jobplatform.jobs.JobRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
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
        "jobs.dispatcher.delay-ms=100", "jobs.worker.enabled=true", "jobs.dispatcher.enabled=true"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@AutoConfigureObservability
class PipelineIT {
    @Autowired TestRestTemplate http;
    @Autowired JdbcTemplate jdbc;
    @Autowired RabbitTemplate rabbit;
    @MockitoSpyBean JobRepository repository;

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
        assertThat(childHistory).hasSize(2);
        assertThat(childHistory.get(1).get("type").asText()).isEqualTo("SUCCEEDED");
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
                .when(repository).succeed(any(), any());
        var response = http.postForEntity("/api/v1/jobs", Map.of("type", "UPPERCASE", "text", "recover me"), JsonNode.class);
        String source = response.getHeaders().getLocation().toString();
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(admin.getQueueInfo(RabbitConfiguration.DEAD_QUEUE).getMessageCount()).isEqualTo(1));
        assertThat(http.getForObject(source, JsonNode.class).get("status").asText()).isEqualTo("QUEUED");
        doCallRealMethod().when(repository).succeed(any(), any());
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
}
