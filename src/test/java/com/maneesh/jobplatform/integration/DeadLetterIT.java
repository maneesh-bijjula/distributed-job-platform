package com.maneesh.jobplatform.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.maneesh.jobplatform.config.RabbitConfiguration;
import com.maneesh.jobplatform.deadletter.*;
import com.maneesh.jobplatform.jobs.*;
import com.maneesh.jobplatform.worker.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.http.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.web.server.ResponseStatusException;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "jobs.dispatcher.enabled=false", "jobs.worker.enabled=false"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class DeadLetterIT {
    @Autowired DeadLetterCollector collector;
    @MockitoSpyBean DeadLetterService inbox;
    @MockitoSpyBean DeadLetterRepository entries;
    @MockitoSpyBean JobRepository repository;
    @Autowired JobService jobs;
    @Autowired ExecutionService execution;
    @Autowired JobExecutor executor;
    @Autowired RabbitTemplate rabbit;
    @Autowired JdbcTemplate jdbc;
    @Autowired TestRestTemplate http;

    @BeforeEach
    void reset() {
        IntegrationDatabase.reset(jdbc);
        new RabbitAdmin(rabbit.getConnectionFactory()).purgeQueue(RabbitConfiguration.DEAD_QUEUE);
    }

    private Job submit() { return jobs.submit(JobType.UPPERCASE, "hello", null).job(); }
    private UUID capture(Job job) { return inbox.capture(job.id().toString().getBytes(StandardCharsets.US_ASCII), job.generation()); }
    private void sendDead(String body, Object generation) {
        var properties = new MessageProperties();
        if (generation != null) properties.setHeader("job-generation", generation);
        rabbit.send(RabbitConfiguration.DEAD_QUEUE, new Message(body.getBytes(StandardCharsets.UTF_8), properties));
        await().atMost(Duration.ofSeconds(3)).until(() -> new RabbitAdmin(rabbit.getConnectionFactory())
                .getQueueInfo(RabbitConfiguration.DEAD_QUEUE).getMessageCount() > 0);
    }
    private void conflict(Runnable action) {
        assertThatThrownBy(action::run).isInstanceOfSatisfying(ResponseStatusException.class,
                error -> assertThat(error.getStatusCode()).isEqualTo(HttpStatus.CONFLICT));
    }

    @Test
    void boundedCollectionCommitsSanitizedRecordsBeforeRemovingBrokerMessages() {
        sendDead("private malformed payload", null);
        sendDead(UUID.randomUUID().toString(), null);
        var first = collector.collect(1);
        assertThat(first.collected()).isEqualTo(1);
        assertThat(inbox.get(first.entryIds().getFirst()).reason()).isEqualTo(DeadLetter.Reason.MALFORMED_MESSAGE);
        var second = collector.collect(1);
        assertThat(second.collected()).isEqualTo(1);
        assertThat(inbox.get(second.entryIds().getFirst()).reason()).isEqualTo(DeadLetter.Reason.UNKNOWN_JOB);
        assertThat(collector.collect(1).collected()).isZero();
        String response = http.getForObject("/api/v1/dead-letters", String.class);
        assertThat(response).doesNotContain("private malformed payload", "resolutionKey");
        assertThat(inbox.get(first.entryIds().getFirst()).payloadSha256()).hasSize(64);
        assertThat(inbox.get(first.entryIds().getFirst()).payloadBytes()).isEqualTo(25);
    }

    @Test
    void storageFailureRequeuesBrokerMessageAndCanBeCollectedAfterRepair() {
        sendDead("malformed", null);
        doThrow(new DataAccessResourceFailureException("storage outage")).when(inbox).capture(any(), any());
        assertThatThrownBy(() -> collector.collect(1)).isInstanceOf(org.springframework.amqp.AmqpException.class)
                .hasRootCauseInstanceOf(DataAccessResourceFailureException.class);
        assertThat(inbox.list(null, 0, 10)).isEmpty();
        doCallRealMethod().when(inbox).capture(any(), any());
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(new RabbitAdmin(rabbit.getConnectionFactory()).getQueueInfo(RabbitConfiguration.DEAD_QUEUE).getMessageCount()).isEqualTo(1));
        assertThat(collector.collect(1).collected()).isEqualTo(1);
    }

    @Test
    void duplicateCaptureAfterResolutionRetainsOriginalIdAndAudit() {
        UUID id = inbox.capture("bad".getBytes(StandardCharsets.UTF_8), null);
        inbox.discard(id, "discard-first");
        assertThat(inbox.capture("bad".getBytes(StandardCharsets.UTF_8), null)).isEqualTo(id);
        assertThat(inbox.get(id).state()).isEqualTo(DeadLetter.State.DISCARDED);
        assertThat(inbox.list(null, 0, 10)).hasSize(1);
    }

    @Test
    void malformedUnknownAndInvalidGenerationMessagesCannotRecover() {
        UUID malformed = inbox.capture("private".getBytes(StandardCharsets.UTF_8), null);
        UUID unknown = inbox.capture(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8), 0);
        Job known = submit();
        UUID invalid = inbox.capture(known.id().toString().getBytes(StandardCharsets.UTF_8), -1);
        for (UUID id : new UUID[]{malformed, unknown, invalid}) {
            conflict(() -> inbox.recover(id, "recover-" + id));
            assertThat(inbox.get(id).state()).isEqualTo(DeadLetter.State.OPEN);
            assertThat(inbox.discard(id, "discard-" + id).entry().state()).isEqualTo(DeadLetter.State.DISCARDED);
        }
        assertThat(jdbc.queryForObject("SELECT count(*) FROM jobs", Integer.class)).isEqualTo(1);
    }

    @Test
    void queuedRecoveryAdvancesGenerationAndHonorsOriginalScheduleAndBackoff() {
        Job future = jobs.submit(JobType.UPPERCASE, "later", null, 8, Instant.now().plusSeconds(3600)).job();
        UUID id = capture(future);
        var result = inbox.recover(id, "recover-future");
        assertThat(result.created()).isTrue();
        assertThat(result.entry().recoveryJobId()).isEqualTo(future.id());
        assertThat(jobs.get(future.id()).generation()).isEqualTo(1);
        assertThat(execution.claim(future.id(), 0)).isEmpty();
        assertThat(execution.claim(future.id(), 1)).isEmpty();
        assertThat(jdbc.queryForObject("SELECT available_at=scheduled_at FROM job_outbox o JOIN jobs j ON j.id=o.job_id WHERE j.id=?", Boolean.class, future.id())).isTrue();
        assertThat(inbox.recover(id, "recover-future").created()).isFalse();
        assertThat(jobs.get(future.id()).generation()).isEqualTo(1);

        Job retry = submit();
        execution.fail(execution.claim(retry.id(), 0).orElseThrow(), HandlerFailure.RETRYABLE_INFRASTRUCTURE);
        retry = jobs.get(retry.id());
        Instant retryAt = retry.retryAt();
        inbox.recover(capture(retry), "recover-backoff");
        assertThat(jobs.get(retry.id()).retryAt()).isEqualTo(retryAt);
        assertThat(jdbc.queryForObject("SELECT available_at>=retry_at FROM job_outbox o JOIN jobs j ON j.id=o.job_id WHERE j.id=?", Boolean.class, retry.id())).isTrue();
    }

    @Test
    void queuedImmediateRecoveryUsesOutboxAndOldMessageCannotExecute() {
        Job job = submit();
        inbox.recover(capture(job), "redrive");
        executor.execute(job.id(), 0);
        assertThat(jobs.get(job.id()).attemptCount()).isZero();
        executor.execute(job.id(), 1);
        assertThat(jobs.get(job.id()).status()).isEqualTo(JobStatus.SUCCEEDED);
    }

    @Test
    void staleBrokerEntryCannotAdvanceANewerQueuedGeneration() {
        Job job = submit();
        UUID old = capture(job);
        jdbc.update("UPDATE jobs SET generation=1 WHERE id=?", job.id());
        conflict(() -> inbox.recover(old, "stale"));
        assertThat(jobs.get(job.id()).generation()).isEqualTo(1);
    }

    @Test
    void concurrentTerminalRecoveryCreatesOneLinkedChildWithReplayAfterCompletion() throws Exception {
        Job source = submit();
        execution.fail(execution.claim(source.id(), 0).orElseThrow());
        UUID id = inbox.list(null, 0, 10).getFirst().id();
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var tasks = new ArrayList<Callable<DeadLetterService.Resolution>>();
            for (int i = 0; i < 10; i++) tasks.add(() -> inbox.recover(id, "one-recovery"));
            int created = 0;
            for (var future : pool.invokeAll(tasks)) if (future.get().created()) created++;
            assertThat(created).isEqualTo(1);
        }
        UUID child = inbox.get(id).recoveryJobId();
        assertThat(child).isNotEqualTo(source.id());
        assertThat(jobs.get(child).retryOf()).isEqualTo(source.id());
        assertThat(jobs.get(child).retryPolicy()).isEqualTo(source.retryPolicy());
        executor.execute(child);
        assertThat(inbox.recover(id, "one-recovery").entry().recoveryJobId()).isEqualTo(child);
        assertThat(jobs.get(source.id()).status()).isEqualTo(JobStatus.FAILED);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM jobs", Integer.class)).isEqualTo(2);
        assertThat(jobs.events(source.id(), 0, 20)).filteredOn(event -> event.type() == JobEvent.Type.DEAD_LETTER_RECOVERED).hasSize(1);
        conflict(() -> inbox.recover(id, "another-key"));
        conflict(() -> inbox.discard(id, "one-recovery"));
    }

    @Test
    void concurrentDistinctRecoveryKeysAllowOneResolutionOnly() throws Exception {
        Job job = submit();
        UUID id = capture(job);
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var tasks = new ArrayList<Callable<Boolean>>();
            for (int i = 0; i < 8; i++) {
                String key = "recover-" + i;
                tasks.add(() -> {
                    try { inbox.recover(id, key); return true; }
                    catch (ResponseStatusException error) { assertThat(error.getStatusCode()).isEqualTo(HttpStatus.CONFLICT); return false; }
                });
            }
            int won = 0;
            for (var result : pool.invokeAll(tasks)) if (result.get()) won++;
            assertThat(won).isEqualTo(1);
        }
        assertThat(jobs.get(job.id()).generation()).isEqualTo(1);
    }

    @Test
    void resolutionKeysCannotCrossEntriesOrActionsAndDiscardDoesNotCancelWork() {
        Job job = submit();
        UUID first = capture(job);
        UUID other = inbox.capture("malformed".getBytes(StandardCharsets.UTF_8), null);
        var resolved = inbox.discard(first, "global-action").entry();
        assertThat(inbox.discard(first, "global-action").entry()).isEqualTo(resolved);
        conflict(() -> inbox.discard(other, "global-action"));
        conflict(() -> inbox.recover(first, "global-action"));
        assertThat(jobs.get(job.id()).status()).isEqualTo(JobStatus.QUEUED);
        assertThat(inbox.get(other).state()).isEqualTo(DeadLetter.State.OPEN);
    }

    @Test
    void recoveryAndAuditRollBackTogetherAndTheKeyCanBeRetried() {
        Job job = submit();
        UUID id = capture(job);
        doThrow(new DataAccessResourceFailureException("audit outage")).when(repository)
                .recordEvent(eq(job.id()), eq(JobEvent.Type.DEAD_LETTER_RECOVERED), eq(JobStatus.QUEUED), eq(job.id()));
        assertThatThrownBy(() -> inbox.recover(id, "retry-after-outage")).isInstanceOf(DataAccessResourceFailureException.class);
        assertThat(inbox.get(id).state()).isEqualTo(DeadLetter.State.OPEN);
        assertThat(jobs.get(job.id()).generation()).isZero();
        doCallRealMethod().when(repository).recordEvent(eq(job.id()), eq(JobEvent.Type.DEAD_LETTER_RECOVERED), eq(JobStatus.QUEUED), eq(job.id()));
        assertThat(inbox.recover(id, "retry-after-outage").created()).isTrue();
    }

    @Test
    void successfulAndRunningJobsCannotBeRecovered() {
        Job running = submit();
        UUID runningEntry = capture(running);
        execution.claim(running.id(), 0);
        conflict(() -> inbox.recover(runningEntry, "running"));
        Job succeeded = submit();
        UUID completedEntry = capture(succeeded);
        executor.execute(succeeded.id());
        conflict(() -> inbox.recover(completedEntry, "succeeded"));
    }

    @Test
    void inspectionAndRecoveryAreAvailableThroughHttpWithAuditAndPagination() {
        Job job = submit();
        sendDead(job.id().toString(), 0);
        var collected = http.postForEntity("/api/v1/dead-letters/collect?limit=1", null, JsonNode.class);
        assertThat(collected.getStatusCode()).isEqualTo(HttpStatus.OK);
        String id = collected.getBody().get("entryIds").get(0).asText();
        inbox.capture("bad".getBytes(StandardCharsets.UTF_8), null);
        var page = http.getForObject("/api/v1/dead-letters?state=OPEN&size=1", JsonNode.class);
        assertThat(page.get("hasNext").asBoolean()).isTrue();
        assertThat(page.get("items").size()).isEqualTo(1);
        var headers = new HttpHeaders(); headers.set("Idempotency-Key", "http-recover");
        String action = "/api/v1/dead-letters/" + id + "/recover";
        var recovered = http.postForEntity(action, new HttpEntity<>(headers), JsonNode.class);
        assertThat(recovered.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(recovered.getHeaders().getLocation().toString()).endsWith("/api/v1/jobs/" + job.id());
        assertThat(http.postForEntity(action, new HttpEntity<>(headers), JsonNode.class).getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(http.getForObject("/api/v1/dead-letters/" + id, JsonNode.class).get("state").asText()).isEqualTo("RECOVERED");
        assertThat(http.getForEntity("/api/v1/dead-letters/" + UUID.randomUUID(), JsonNode.class).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @ParameterizedTest
    @ValueSource(strings = {"?size=101", "?size=0", "?page=-1", "?page=1000001", "?state=BAD"})
    void httpRejectsUnboundedInspection(String query) {
        assertThat(http.getForEntity("/api/v1/dead-letters" + query, JsonNode.class).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void httpRequiresValidKeysEmptyBodiesAndBoundedCollection() {
        UUID id = capture(submit());
        assertThat(http.postForEntity("/api/v1/dead-letters/collect?limit=101", null, JsonNode.class).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(http.postForEntity("/api/v1/dead-letters/collect?limit=0", null, JsonNode.class).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(http.postForEntity("/api/v1/dead-letters/" + id + "/recover", null, JsonNode.class).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        var headers = new HttpHeaders(); headers.set("Idempotency-Key", "invalid key!");
        assertThat(http.postForEntity("/api/v1/dead-letters/" + id + "/discard", new HttpEntity<>(headers), JsonNode.class).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        headers.set("Idempotency-Key", "valid");
        assertThat(http.postForEntity("/api/v1/dead-letters/" + id + "/recover", new HttpEntity<>(java.util.Map.of("force", true), headers), JsonNode.class).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(http.postForEntity("/api/v1/dead-letters/collect", java.util.Map.of("force", true), JsonNode.class).getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }
}
