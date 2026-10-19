package com.maneesh.jobplatform.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.maneesh.jobplatform.config.ExecutionProperties;
import com.maneesh.jobplatform.jobs.*;
import com.maneesh.jobplatform.worker.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
        "jobs.dispatcher.enabled=false", "jobs.worker.enabled=false", "jobs.execution.lease-seconds=3",
        "jobs.execution.heartbeat-millis=100", "jobs.execution.max-recoveries=2"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class ExecutionIT {
    @Autowired JobService jobs;
    @Autowired ExecutionService execution;
    @Autowired JobExecutor executor;
    @Autowired ExecutionProperties properties;
    @Autowired ScheduledThreadPoolExecutor monitor;
    @Autowired JdbcTemplate jdbc;
    @MockitoSpyBean JobRepository repository;
    @MockitoSpyBean ExecutionRepository attempts;
    @MockitoSpyBean JobProcessor processor;

    @BeforeEach
    void reset() { IntegrationDatabase.reset(jdbc); }

    private Job submit() { return jobs.submit(JobType.UPPERCASE, "hello", null).job(); }
    private ExecutionService.Claim claim(Job job) { return execution.claim(job.id(), job.generation()).orElseThrow(); }
    private void loseLease(UUID id) {
        jdbc.update("UPDATE job_attempts SET lease_until=clock_timestamp()-interval '1 second' WHERE job_id=? AND status='RUNNING'", id);
    }
    private void expireDeadline(UUID id) {
        jdbc.update("UPDATE job_attempts SET deadline_at=clock_timestamp()-interval '1 second',lease_until=clock_timestamp()-interval '2 seconds' WHERE job_id=? AND status='RUNNING'", id);
        jdbc.update("UPDATE jobs SET deadline_at=clock_timestamp()-interval '1 second' WHERE id=?", id);
    }

    @Test
    void claimCommitsVisibleRunningStateAndAttemptBeforeComputation() {
        Job job = submit();
        var claim = claim(job);
        Job running = jobs.get(job.id());
        assertThat(running.status()).isEqualTo(JobStatus.RUNNING);
        assertThat(running.activeAttemptId()).isEqualTo(claim.attempt().id());
        assertThat(running.attemptCount()).isEqualTo(1);
        assertThat(running.deadlineAt()).isEqualTo(claim.attempt().deadlineAt());
        assertThat(jobs.list(JobStatus.RUNNING, 0, 10)).containsExactly(running);
        assertThat(jobs.attempts(job.id(), 0, 10)).containsExactly(claim.attempt());
        assertThat(jobs.events(job.id(), 0, 10).getLast().attemptId()).isEqualTo(claim.attempt().id());
        assertThat(execution.claim(job.id(), 0)).isEmpty();
    }

    @Test
    void concurrentClaimsCreateOneAttempt() throws Exception {
        UUID id = submit().id();
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var tasks = new ArrayList<Callable<Boolean>>();
            for (int i=0; i<12; i++) tasks.add(() -> execution.claim(id, 0).isPresent());
            int claimed = 0;
            for (var future : pool.invokeAll(tasks)) if (future.get()) claimed++;
            assertThat(claimed).isEqualTo(1);
        }
        assertThat(jobs.attempts(id, 0, 20)).hasSize(1);
        assertThat(jobs.events(id, 0, 20)).hasSize(2);
    }

    @Test
    void heartbeatRenewsLeaseButNeverMovesTheFixedDeadline() {
        var claim = claim(submit());
        assertThat(execution.heartbeat(claim)).isPresent();
        var renewed = jobs.attempts(claim.job().id(), 0, 10).getFirst();
        assertThat(renewed.heartbeatAt()).isAfter(claim.attempt().heartbeatAt());
        assertThat(renewed.leaseUntil()).isAfter(claim.attempt().leaseUntil());
        assertThat(renewed.deadlineAt()).isEqualTo(claim.attempt().deadlineAt());
    }

    @Test
    void expiredLeaseCannotBeRenewedAndOldGenerationCannotClaimRecoveredJob() {
        var original = claim(submit());
        UUID id = original.job().id();
        loseLease(id);
        assertThat(execution.heartbeat(original)).isEmpty();
        assertThat(jobs.get(id).status()).isEqualTo(JobStatus.QUEUED);
        assertThat(jobs.attempts(id, 0, 10).getFirst().status()).isEqualTo(JobAttempt.Status.ABANDONED);
        assertThat(jdbc.queryForObject("SELECT generation FROM job_outbox WHERE job_id=?", Integer.class, id)).isEqualTo(1);
        executor.execute(id); // Legacy UUID-only messages belong to generation zero.
        assertThat(jobs.get(id).attemptCount()).isEqualTo(1);
        assertThat(execution.succeed(original, new ObjectMapper().createObjectNode().put("text", "STALE"))).isFalse();
        executor.execute(id, 1);
        assertThat(jobs.get(id).result().get("text").asText()).isEqualTo("HELLO");
        assertThat(jobs.attempts(id, 0, 10)).extracting(JobAttempt::status)
                .containsExactly(JobAttempt.Status.ABANDONED, JobAttempt.Status.SUCCEEDED);
        assertThat(jobs.attempts(id, 0, 10)).extracting(JobAttempt::deadlineAt)
                .containsOnly(original.attempt().deadlineAt());
    }

    @Test
    void staleWorkerCannotAffectReplacementBeforeOrAfterItsResultCommits() {
        var original = claim(submit());
        loseLease(original.job().id());
        assertThat(execution.recoverBatch()).isEqualTo(1);
        var replacement = execution.claim(original.job().id(), 1).orElseThrow();
        var stale = new ObjectMapper().createObjectNode().put("text", "STALE");
        var correct = new ObjectMapper().createObjectNode().put("text", "NEW");
        assertThat(execution.succeed(original, stale)).isFalse();
        assertThat(execution.fail(original)).isFalse();
        assertThat(execution.heartbeat(original)).isEmpty();
        assertThat(jobs.get(original.job().id()).activeAttemptId()).isEqualTo(replacement.attempt().id());
        assertThat(execution.succeed(replacement, correct)).isTrue();
        assertThat(execution.succeed(original, stale)).isFalse();
        assertThat(jobs.get(original.job().id()).result()).isEqualTo(correct);
    }

    @Test
    void forgedClaimTokenCannotRenewOrCommitAnActiveAttempt() {
        var current = claim(submit());
        var forged = new ExecutionService.Claim(current.job(), current.attempt(), UUID.randomUUID(), current.window());
        assertThat(execution.heartbeat(forged)).isEmpty();
        assertThat(execution.succeed(forged, new ObjectMapper().createObjectNode())).isFalse();
        assertThat(jobs.get(current.job().id()).status()).isEqualTo(JobStatus.RUNNING);
        assertThat(execution.heartbeat(current)).isPresent();
    }

    @Test
    void concurrentRecoveryPassesRequeueExpiredAttemptExactlyOnce() throws Exception {
        var current = claim(submit());
        loseLease(current.job().id());
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var tasks = new ArrayList<Callable<Integer>>();
            for (int i=0; i<8; i++) tasks.add(execution::recoverBatch);
            int recovered = 0;
            for (var future : pool.invokeAll(tasks)) recovered += future.get();
            assertThat(recovered).isEqualTo(1);
        }
        assertThat(jobs.get(current.job().id()).generation()).isEqualTo(1);
        assertThat(jobs.events(current.job().id(), 0, 10)).extracting(JobEvent::type)
                .containsExactly(JobEvent.Type.SUBMITTED, JobEvent.Type.STARTED, JobEvent.Type.LEASE_EXPIRED);
    }

    @Test
    void recoveryBudgetPreventsAnEndlessWorkerLossLoop() {
        Job job = jobs.submit(JobType.UPPERCASE, "hello", null, 0, null, 3600).job();
        for (int generation=0; generation<=properties.maxRecoveries(); generation++) {
            assertThat(execution.claim(job.id(), generation)).isPresent();
            loseLease(job.id());
            assertThat(execution.recoverBatch()).isEqualTo(1);
        }
        assertThat(jobs.get(job.id()).status()).isEqualTo(JobStatus.FAILED);
        assertThat(jobs.get(job.id()).error()).isEqualTo("Worker recovery limit exceeded");
        assertThat(jobs.get(job.id()).attemptCount()).isEqualTo(properties.maxRecoveries()+1);
        assertThat(execution.recoverBatch()).isZero();
        assertThat(jobs.events(job.id(), 0, 20).getLast().type()).isEqualTo(JobEvent.Type.RECOVERY_EXHAUSTED);
    }

    @Test
    void expiredExecutionDeadlineFailsInsteadOfCreatingAnotherAttempt() {
        var current = claim(submit());
        expireDeadline(current.job().id());
        assertThat(execution.recoverBatch()).isEqualTo(1);
        assertThat(jobs.get(current.job().id()).status()).isEqualTo(JobStatus.FAILED);
        assertThat(jobs.get(current.job().id()).error()).isEqualTo("Execution deadline exceeded");
        assertThat(jobs.attempts(current.job().id(), 0, 10).getFirst().status()).isEqualTo(JobAttempt.Status.TIMED_OUT);
        assertThat(execution.succeed(current, new ObjectMapper().createObjectNode())).isFalse();
        assertThat(execution.recoverBatch()).isZero();
    }

    @Test
    void lateResultCannotBeatTheDeadlineEvenBeforeTheReaperRuns() {
        var current = claim(submit());
        expireDeadline(current.job().id());
        assertThat(execution.succeed(current, new ObjectMapper().createObjectNode())).isFalse();
        assertThat(jobs.get(current.job().id()).status()).isEqualTo(JobStatus.FAILED);
        assertThat(jobs.events(current.job().id(), 0, 10).getLast().type()).isEqualTo(JobEvent.Type.TIMED_OUT);
    }

    @Test
    void recoveredQueueWaitConsumesRemainingDeadlineAndManualRetryGetsFreshBudget() {
        var current = claim(jobs.submit(JobType.UPPERCASE, "hello", null, 0, null, 9).job());
        UUID id = current.job().id();
        loseLease(id);
        execution.recoverBatch();
        jdbc.update("UPDATE jobs SET deadline_at=clock_timestamp()-interval '1 second' WHERE id=?", id);
        assertThat(execution.recoverBatch()).isEqualTo(1);
        assertThat(jobs.get(id).status()).isEqualTo(JobStatus.FAILED);
        assertThat(jobs.get(id).attemptCount()).isEqualTo(1);
        var retry = jobs.retry(id, "fresh-budget").job();
        assertThat(retry.timeoutSeconds()).isEqualTo(9);
        assertThat(retry.deadlineAt()).isNull();
        var next = claim(retry);
        assertThat(next.window().deadlineMillis()).isGreaterThan(8000);
    }

    @Test
    void futureScheduledJobsCannotBeStartedByEarlyLegacyMessages() {
        Job job = jobs.submit(JobType.UPPERCASE, "future", null, 0, Instant.now().plusSeconds(3600)).job();
        executor.execute(job.id());
        assertThat(jobs.get(job.id()).status()).isEqualTo(JobStatus.QUEUED);
        assertThat(jobs.attempts(job.id(), 0, 10)).isEmpty();
        verifyNoInteractions(processor);
    }

    @Test
    void recoveryStateAndReplacementOutboxRollBackWhenAuditWriteFails() {
        var current = claim(submit());
        UUID id = current.job().id();
        loseLease(id);
        doThrow(new DataAccessResourceFailureException("audit unavailable")).when(repository)
                .recordEvent(eq(id), eq(JobEvent.Type.LEASE_EXPIRED), eq(JobStatus.QUEUED), isNull(), any(UUID.class));
        assertThatThrownBy(execution::recoverBatch).isInstanceOf(DataAccessResourceFailureException.class);
        assertThat(jobs.get(id).status()).isEqualTo(JobStatus.RUNNING);
        assertThat(jobs.get(id).generation()).isZero();
        assertThat(jobs.attempts(id, 0, 10).getFirst().status()).isEqualTo(JobAttempt.Status.RUNNING);
        assertThat(jdbc.queryForObject("SELECT generation FROM job_outbox WHERE job_id=?", Integer.class, id)).isZero();
        doCallRealMethod().when(repository).recordEvent(eq(id), eq(JobEvent.Type.LEASE_EXPIRED), eq(JobStatus.QUEUED), isNull(), any(UUID.class));
        assertThat(execution.recoverBatch()).isEqualTo(1);
    }

    @Test
    void handlerRunsOutsideDatabaseTransactionAndHeartbeatsKeepItsLeaseAlive() throws Exception {
        Job job = submit();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        doAnswer(call -> {
            assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
            assertThat(jobs.get(job.id()).status()).isEqualTo(JobStatus.RUNNING);
            entered.countDown();
            ExecutionControl control = call.getArgument(2);
            while (release.getCount() != 0) control.pause(Duration.ofMillis(100));
            return new ObjectMapper().createObjectNode().put("text", "HEARTBEATING");
        }).when(processor).process(eq(JobType.UPPERCASE), eq("hello"), any(ExecutionControl.class));
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var future = pool.submit(() -> executor.execute(job.id()));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            var initial = jobs.attempts(job.id(), 0, 10).getFirst();
            try {
                await().pollDelay(Duration.ofMillis(3200)).atMost(Duration.ofSeconds(6)).untilAsserted(() -> {
                    assertThat(jobs.get(job.id()).status()).isEqualTo(JobStatus.RUNNING);
                    assertThat(jobs.attempts(job.id(), 0, 10).getFirst().leaseUntil()).isAfter(initial.leaseUntil());
                });
            } finally { release.countDown(); }
            future.get(5, TimeUnit.SECONDS);
        }
        assertThat(jobs.get(job.id()).status()).isEqualTo(JobStatus.SUCCEEDED);
        assertThat(jobs.get(job.id()).attemptCount()).isEqualTo(1);
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertThat(monitor.getQueue()).isEmpty());
    }

    @Test
    void runningHandlerObservesCancellationAtItsCooperativeCheckpoint() throws Exception {
        Job job = submit();
        var entered = new CountDownLatch(1);
        doAnswer(call -> {
            entered.countDown();
            ((ExecutionControl) call.getArgument(2)).pause(Duration.ofSeconds(20));
            throw new AssertionError("Cancelled handler must not finish this work");
        }).when(processor).process(eq(JobType.UPPERCASE), eq("hello"), any(ExecutionControl.class));
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var future = pool.submit(() -> executor.execute(job.id()));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(jobs.cancel(job.id()).status()).isEqualTo(JobStatus.CANCELLED);
            future.get(2, TimeUnit.SECONDS);
        }
        assertThat(jobs.get(job.id()).result()).isNull();
        assertThat(jobs.attempts(job.id(), 0, 10).getFirst().status()).isEqualTo(JobAttempt.Status.CANCELLED);
    }

    @Test
    void realExecutionTimeoutStopsCooperativeHandlerAndPersistsTimeoutOutcome() throws Exception {
        Job job = jobs.submit(JobType.UPPERCASE, "hello", null, 0, null, 1).job();
        doAnswer(call -> {
            ((ExecutionControl) call.getArgument(2)).pause(Duration.ofSeconds(10));
            throw new AssertionError("Timed-out handler must stop at a checkpoint");
        }).when(processor).process(eq(JobType.UPPERCASE), eq("hello"), any(ExecutionControl.class));
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            pool.submit(() -> executor.execute(job.id())).get(3, TimeUnit.SECONDS);
        }
        assertThat(jobs.get(job.id()).status()).isEqualTo(JobStatus.FAILED);
        assertThat(jobs.attempts(job.id(), 0, 10).getFirst().status()).isEqualTo(JobAttempt.Status.TIMED_OUT);
    }

    @Test
    void timeoutParticipatesInIdempotencyAndAttemptPagesRemainBounded() {
        var source = jobs.submit(JobType.UPPERCASE, "hello", "timeout-key", 0, null, 4);
        assertThat(jobs.submit(JobType.UPPERCASE, "hello", "timeout-key", 0, null, 4).created()).isFalse();
        assertThatThrownBy(() -> jobs.submit(JobType.UPPERCASE, "hello", "timeout-key", 0, null, 5))
                .isInstanceOf(org.springframework.web.server.ResponseStatusException.class);
        var first = claim(source.job());
        loseLease(source.job().id());
        execution.recoverBatch();
        var second = execution.claim(source.job().id(), 1).orElseThrow();
        assertThat(jobs.attempts(source.job().id(), 0, 1)).hasSize(2);
        assertThat(jobs.attempts(source.job().id(), 1, 1)).containsExactly(second.attempt());
        assertThat(first.attempt().number()).isEqualTo(1);
        assertThat(second.attempt().number()).isEqualTo(2);
    }

    @Test
    void failedHeartbeatCannotKeepAnExecutionAliveLocally() throws Exception {
        Job job = submit();
        doThrow(new DataAccessResourceFailureException("simulated heartbeat outage")).when(attempts)
                .renew(any(), any(), anyInt());
        doAnswer(call -> {
            ((ExecutionControl) call.getArgument(2)).pause(Duration.ofSeconds(10));
            throw new AssertionError("Unconfirmed heartbeats must not keep work alive");
        }).when(processor).process(eq(JobType.UPPERCASE), eq("hello"), any(ExecutionControl.class));
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            pool.submit(() -> executor.execute(job.id())).get(5, TimeUnit.SECONDS);
        }
        execution.recoverBatch();
        assertThat(jobs.get(job.id()).status()).isEqualTo(JobStatus.QUEUED);
        assertThat(jobs.get(job.id()).error()).isNull();
        assertThat(jobs.attempts(job.id(), 0, 10).getFirst().status()).isEqualTo(JobAttempt.Status.ABANDONED);
        assertThat(jobs.events(job.id(), 0, 10).getLast().type()).isEqualTo(JobEvent.Type.LEASE_EXPIRED);
    }
}
