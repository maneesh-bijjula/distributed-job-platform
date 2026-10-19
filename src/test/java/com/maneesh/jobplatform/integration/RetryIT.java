package com.maneesh.jobplatform.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.maneesh.jobplatform.JobPlatformApplication;
import com.maneesh.jobplatform.deadletter.*;
import com.maneesh.jobplatform.jobs.*;
import com.maneesh.jobplatform.worker.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
        "jobs.dispatcher.enabled=false", "jobs.worker.enabled=false"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class RetryIT {
    @Autowired JobService jobs;
    @Autowired ExecutionService execution;
    @Autowired JobExecutor executor;
    @Autowired JdbcTemplate jdbc;
    @Autowired DeadLetterService inbox;
    @MockitoSpyBean JobRepository repository;
    @MockitoSpyBean JobProcessor processor;
    @MockitoSpyBean DeadLetterRepository deadLetters;

    @BeforeEach
    void reset() { IntegrationDatabase.reset(jdbc); }

    private Job submit(int max, int initial, int cap) {
        return jobs.submit(JobType.UPPERCASE, "hello", null, 0, null, 60, new RetryPolicy(max, initial, cap)).job();
    }
    private ExecutionService.Claim claim(Job job) { return execution.claim(job.id(), jobs.get(job.id()).generation()).orElseThrow(); }
    private void makeRetryDue(UUID id) {
        jdbc.update("UPDATE jobs SET retry_at=clock_timestamp()-interval '1 second' WHERE id=?", id);
        jdbc.update("UPDATE job_outbox SET available_at=clock_timestamp()-interval '1 second' WHERE job_id=?", id);
    }

    @Test
    void retryIsDurableFencedAndDoesNotOccupyWorkerWhileWaiting() {
        Job job = submit(3, 2, 10);
        doThrow(new RetryableJobException("secret endpoint")).when(processor).process(any(), any(), any());
        executor.execute(job.id()); // Returns immediately after persisting backoff.
        Job waiting = jobs.get(job.id());
        assertThat(waiting.status()).isEqualTo(JobStatus.QUEUED);
        assertThat(waiting.generation()).isEqualTo(1);
        assertThat(waiting.activeAttemptId()).isNull();
        assertThat(waiting.error()).isNull();
        assertThat(waiting.completedAt()).isNull();
        assertThat(waiting.retryAt()).isNotNull();
        assertThat(jdbc.queryForObject("SELECT available_at=retry_at FROM job_outbox o JOIN jobs j ON j.id=o.job_id WHERE j.id=?", Boolean.class, job.id())).isTrue();
        assertThat(execution.claim(job.id(), 0)).isEmpty();
        assertThat(execution.claim(job.id(), 1)).isEmpty(); // Even a forged early current-generation delivery waits.
        assertThat(jobs.attempts(job.id(), 0, 10)).hasSize(1);
        var attempt = jobs.attempts(job.id(), 0, 10).getFirst();
        assertThat(attempt.status()).isEqualTo(JobAttempt.Status.FAILED);
        assertThat(attempt.failureKind()).isEqualTo(HandlerFailure.RETRYABLE_INFRASTRUCTURE);
        assertThat(attempt.error()).isEqualTo("Temporary handler dependency failure").doesNotContain("secret");
        assertThat(jobs.events(job.id(), 0, 20)).extracting(JobEvent::type)
                .containsExactly(JobEvent.Type.SUBMITTED, JobEvent.Type.STARTED, JobEvent.Type.RETRY_SCHEDULED);
        assertThat(inbox.list(null, 0, 10)).isEmpty();
    }

    @Test
    void backoffIncreasesAndStopsAtCapThenExhaustsExactlyAtAttemptLimit() {
        Job job = submit(4, 2, 3);
        for (int number = 1; number <= 4; number++) {
            var claim = claim(job);
            assertThat(execution.fail(claim, HandlerFailure.RETRYABLE_INFRASTRUCTURE)).isTrue();
            Job current = jobs.get(job.id());
            assertThat(current.attemptCount()).isEqualTo(number);
            if (number < 4) {
                Instant finished = jobs.attempts(job.id(), 0, 10).getLast().finishedAt();
                long delay = Duration.between(finished, current.retryAt()).toMillis();
                assertThat(delay).isBetween((number == 1 ? 2000L : 3000L) - 10, (number == 1 ? 2000L : 3000L) + 1000);
                makeRetryDue(job.id());
            }
        }
        assertThat(jobs.get(job.id()).status()).isEqualTo(JobStatus.FAILED);
        assertThat(jobs.attempts(job.id(), 0, 10)).hasSize(4);
        assertThat(inbox.list(null, 0, 10)).singleElement().extracting(DeadLetter::reason).isEqualTo(DeadLetter.Reason.RETRIES_EXHAUSTED);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM job_outbox WHERE job_id=?", Integer.class, job.id())).isZero();
        assertThat(execution.claim(job.id(), 3)).isEmpty();
    }

    @Test
    void permanentFailureDoesNotRetryEvenWithRemainingBudget() {
        Job job = submit(5, 1, 10);
        doThrow(new IllegalArgumentException("private input")).when(processor).process(any(), any(), any());
        executor.execute(job.id());
        assertThat(jobs.get(job.id()).status()).isEqualTo(JobStatus.FAILED);
        assertThat(jobs.get(job.id()).attemptCount()).isEqualTo(1);
        assertThat(jobs.attempts(job.id(), 0, 10).getFirst().failureKind()).isEqualTo(HandlerFailure.PERMANENT_HANDLER);
        assertThat(inbox.list(null, 0, 10).getFirst().reason()).isEqualTo(DeadLetter.Reason.PERMANENT_HANDLER);
    }

    @Test
    void oneAttemptDisablesAutomaticRetriesAndWorkerLossRecovery() {
        Job failed = submit(1, 1, 10);
        execution.fail(claim(failed), HandlerFailure.RETRYABLE_INFRASTRUCTURE);
        assertThat(jobs.get(failed.id()).status()).isEqualTo(JobStatus.FAILED);
        Job abandoned = submit(1, 1, 10);
        claim(abandoned);
        jdbc.update("UPDATE job_attempts SET lease_until=clock_timestamp()-interval '1 second' WHERE job_id=?", abandoned.id());
        execution.recoverBatch();
        assertThat(jobs.get(abandoned.id()).status()).isEqualTo(JobStatus.FAILED);
        assertThat(jobs.get(abandoned.id()).error()).isEqualTo("Execution attempt limit exceeded");
    }

    @Test
    void duplicateFailureAndConcurrentReplacementClaimsDoNotCreateExtraAttempts() throws Exception {
        Job job = submit(3, 1, 10);
        var original = claim(job);
        assertThat(execution.fail(original, HandlerFailure.RETRYABLE_INFRASTRUCTURE)).isTrue();
        assertThat(execution.fail(original, HandlerFailure.RETRYABLE_INFRASTRUCTURE)).isFalse();
        makeRetryDue(job.id());
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var calls = new ArrayList<Callable<Boolean>>();
            for (int i = 0; i < 10; i++) calls.add(() -> execution.claim(job.id(), 1).isPresent());
            int claimed = 0;
            for (var result : pool.invokeAll(calls)) if (result.get()) claimed++;
            assertThat(claimed).isEqualTo(1);
        }
        assertThat(jobs.get(job.id()).attemptCount()).isEqualTo(2);
        assertThat(execution.succeed(original, new ObjectMapper().createObjectNode())).isFalse();
        assertThat(execution.heartbeat(original)).isEmpty();
    }

    @Test
    void cancellationDuringBackoffFencesDueDelivery() {
        Job job = submit(3, 1, 10);
        execution.fail(claim(job), HandlerFailure.RETRYABLE_INFRASTRUCTURE);
        jobs.cancel(job.id());
        executor.execute(job.id(), 1);
        assertThat(jobs.get(job.id()).status()).isEqualTo(JobStatus.CANCELLED);
        assertThat(jobs.get(job.id()).retryAt()).isNull();
        assertThat(jobs.attempts(job.id(), 0, 10)).hasSize(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM job_outbox", Integer.class)).isZero();
    }

    @Test
    void deadlineKeepsRunningDuringBackoffAndTimeoutIsInspectable() {
        Job job = submit(3, 10, 10);
        var original = claim(job);
        execution.fail(original, HandlerFailure.RETRYABLE_INFRASTRUCTURE);
        assertThat(jobs.get(job.id()).deadlineAt()).isEqualTo(original.job().deadlineAt());
        jdbc.update("UPDATE jobs SET deadline_at=clock_timestamp()-interval '1 second' WHERE id=?", job.id());
        execution.recoverBatch();
        assertThat(jobs.get(job.id()).status()).isEqualTo(JobStatus.FAILED);
        assertThat(jobs.get(job.id()).attemptCount()).isEqualTo(1);
        assertThat(jobs.get(job.id()).retryAt()).isNull();
        assertThat(inbox.list(null, 0, 10).getFirst().reason()).isEqualTo(DeadLetter.Reason.DEADLINE_EXCEEDED);
    }

    @Test
    void manualRetryInheritsPolicyAndGetsFreshHistoryAndDeadline() {
        Job source = submit(1, 2, 20);
        execution.fail(claim(source), HandlerFailure.RETRYABLE_INFRASTRUCTURE);
        Job child = jobs.retry(source.id(), "manual-retry").job();
        assertThat(child.retryPolicy()).isEqualTo(source.retryPolicy());
        assertThat(child.attemptCount()).isZero();
        assertThat(child.deadlineAt()).isNull();
        assertThat(child.retryAt()).isNull();
        assertThat(jobs.retry(source.id(), "manual-retry").job()).isEqualTo(child);
        executor.execute(child.id());
        assertThat(jobs.retry(source.id(), "manual-retry").job().status()).isEqualTo(JobStatus.SUCCEEDED);
    }

    @Test
    void policyParticipatesInSubmissionIdempotencyAndLegacyReplayIsPreserved() {
        Job first = jobs.submit(JobType.SHA256, "abc", "policy", 0, null, 30, new RetryPolicy(2, 1, 5)).job();
        assertThatThrownBy(() -> jobs.submit(JobType.SHA256, "abc", "policy", 0, null, 30, new RetryPolicy(3, 1, 5)))
                .isInstanceOf(org.springframework.web.server.ResponseStatusException.class);
        assertThat(jobs.submit(JobType.SHA256, "abc", "policy", 0, null, 30, new RetryPolicy(2, 1, 5)).job()).isEqualTo(first);
        jdbc.update("UPDATE jobs SET max_attempts=25,legacy_retry_policy=true WHERE id=?", first.id());
        assertThat(jobs.submit(JobType.SHA256, "abc", "policy").job().retryPolicy().maxAttempts()).isEqualTo(25);
        assertThatThrownBy(() -> jobs.submit(JobType.SHA256, "changed", "policy"))
                .isInstanceOf(org.springframework.web.server.ResponseStatusException.class);
    }

    @Test
    void retryTransitionRollsBackWhenHistoryCannotBeSaved() {
        Job job = submit(3, 1, 10);
        var current = claim(job);
        doThrow(new DataAccessResourceFailureException("audit unavailable")).when(repository)
                .recordEvent(eq(job.id()), eq(JobEvent.Type.RETRY_SCHEDULED), eq(JobStatus.QUEUED), isNull(), any(UUID.class));
        assertThatThrownBy(() -> execution.fail(current, HandlerFailure.RETRYABLE_INFRASTRUCTURE))
                .isInstanceOf(DataAccessResourceFailureException.class);
        assertThat(jobs.get(job.id()).status()).isEqualTo(JobStatus.RUNNING);
        assertThat(jobs.get(job.id()).generation()).isZero();
        assertThat(jobs.attempts(job.id(), 0, 10).getFirst().status()).isEqualTo(JobAttempt.Status.RUNNING);
        assertThat(jobs.get(job.id()).retryAt()).isNull();
    }

    @Test
    void terminalFailureAndInboxInsertAreAtomic() {
        Job job = submit(1, 1, 10);
        var current = claim(job);
        doThrow(new DataAccessResourceFailureException("inbox unavailable")).when(deadLetters).execution(any(), any());
        assertThatThrownBy(() -> execution.fail(current, HandlerFailure.RETRYABLE_INFRASTRUCTURE))
                .isInstanceOf(DataAccessResourceFailureException.class);
        assertThat(jobs.get(job.id()).status()).isEqualTo(JobStatus.RUNNING);
        assertThat(jobs.attempts(job.id(), 0, 10).getFirst().failureKind()).isNull();
    }

    @Test
    void applicationRestartRetainsBackoffAndReplacementCanFinish() {
        UUID id;
        Instant retryAt;
        Instant deadline;
        try (var first = new SpringApplicationBuilder(JobPlatformApplication.class).web(WebApplicationType.NONE)
                .run("--jobs.worker.enabled=false", "--jobs.dispatcher.enabled=false")) {
            var firstJobs = first.getBean(JobService.class);
            var firstExecution = first.getBean(ExecutionService.class);
            Job job = firstJobs.submit(JobType.UPPERCASE, "restart", null, 0, null, 60, new RetryPolicy(3, 3, 10)).job();
            id = job.id();
            firstExecution.fail(firstExecution.claim(id, 0).orElseThrow(), HandlerFailure.RETRYABLE_INFRASTRUCTURE);
            retryAt = firstJobs.get(id).retryAt();
            deadline = firstJobs.get(id).deadlineAt();
        }
        try (var second = new SpringApplicationBuilder(JobPlatformApplication.class).web(WebApplicationType.NONE)
                .run("--jobs.worker.enabled=false", "--jobs.dispatcher.enabled=false")) {
            assertThat(second.getBean(JobService.class).get(id).retryAt()).isEqualTo(retryAt);
            assertThat(second.getBean(JobService.class).get(id).deadlineAt()).isEqualTo(deadline);
            await().atMost(Duration.ofSeconds(8)).until(() -> !Instant.now().isBefore(retryAt));
            second.getBean(JobExecutor.class).execute(id, 1);
            assertThat(second.getBean(JobService.class).get(id).status()).isEqualTo(JobStatus.SUCCEEDED);
            assertThat(second.getBean(JobService.class).get(id).attemptCount()).isEqualTo(2);
        }
    }
}
