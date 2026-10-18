package com.maneesh.jobplatform.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.maneesh.jobplatform.config.RabbitConfiguration;
import java.time.Duration;
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
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "jobs.dispatcher.delay-ms=100", "jobs.worker.enabled=true", "jobs.dispatcher.enabled=true"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@AutoConfigureObservability
class PipelineIT {
    @Autowired TestRestTemplate http;
    @Autowired JdbcTemplate jdbc;
    @Autowired RabbitTemplate rabbit;

    @BeforeEach
    void resetDatabase() {
        jdbc.execute("TRUNCATE job_outbox, jobs");
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
}
