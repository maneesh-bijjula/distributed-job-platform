package com.maneesh.jobplatform.integration;

import com.maneesh.jobplatform.jobs.*;
import com.maneesh.jobplatform.messaging.JobPublisher;
import com.maneesh.jobplatform.messaging.OutboxRelay;
import com.maneesh.jobplatform.worker.JobExecutor;
import com.maneesh.jobplatform.worker.JobProcessor;
import java.time.Instant;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.web.server.ResponseStatusException;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = {
        "jobs.dispatcher.enabled=false", "jobs.worker.enabled=false"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class LifecycleIT {
    @Autowired JobService service;
    @Autowired JobExecutor executor;
    @Autowired OutboxRelay relay;
    @Autowired JdbcTemplate jdbc;
    @MockitoBean JobPublisher publisher;
    @MockitoSpyBean JobProcessor processor;
    @MockitoSpyBean JobRepository repository;

    @BeforeEach
    void reset() {
        IntegrationDatabase.reset(jdbc);
    }

    private int outboxSize() {
        return jdbc.queryForObject("SELECT count(*) FROM job_outbox", Integer.class);
    }

    private void conflict(Runnable action) {
        assertThatThrownBy(action::run).isInstanceOfSatisfying(ResponseStatusException.class,
                exception -> assertThat(exception.getStatusCode().value()).isEqualTo(409));
    }

    @Test
    void cancellingScheduledJobRemovesOutboxAndDuplicateCancellationAddsNoEvent() {
        Job original = service.submit(JobType.SHA256, "abc", "original", 9, Instant.now().plusSeconds(3600)).job();
        Job cancelled = service.cancel(original.id());
        assertThat(cancelled.status()).isEqualTo(JobStatus.CANCELLED);
        assertThat(cancelled.completedAt()).isNotNull();
        assertThat(cancelled.error()).isNull();
        assertThat(cancelled.result()).isNull();
        assertThat(service.cancel(original.id())).isEqualTo(cancelled);
        assertThat(service.list(JobStatus.CANCELLED, 0, 10)).containsExactly(cancelled);
        assertThat(outboxSize()).isZero();
        assertThat(relay.dispatchBatch()).isZero();
        executor.execute(original.id());
        verifyNoInteractions(processor);
        assertThat(service.events(original.id(), 0, 10)).extracting(JobEvent::type)
                .containsExactly(JobEvent.Type.SUBMITTED, JobEvent.Type.CANCELLED);
        assertThat(service.submit(JobType.SHA256, "abc", "original", 9, original.scheduledAt()).job())
                .isEqualTo(cancelled);
    }

    @Test
    void alreadyPublishedCancelledJobCannotExecuteOnDelayedDelivery() {
        Job job = service.submit(JobType.UPPERCASE, "hello", null).job();
        assertThat(relay.dispatchBatch()).isEqualTo(1);
        service.cancel(job.id());
        executor.execute(job.id());
        executor.execute(job.id());
        verifyNoInteractions(processor);
        assertThat(service.get(job.id()).status()).isEqualTo(JobStatus.CANCELLED);
    }

    @Test
    void workerThatAlreadyOwnsJobCompletesBeforeCancellationAndKeepsItsResult() throws Exception {
        Job job = service.submit(JobType.SHA256, "abc", null).job();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        doAnswer(call -> {
            entered.countDown();
            assertThat(release.await(10, TimeUnit.SECONDS)).isTrue();
            return call.callRealMethod();
        }).when(processor).process(JobType.SHA256, "abc");
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var execution = pool.submit(() -> executor.execute(job.id()));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            var cancelStarted = new CountDownLatch(1);
            var cancellation = pool.submit(() -> {
                cancelStarted.countDown();
                conflict(() -> service.cancel(job.id()));
            });
            try {
                assertThat(cancelStarted.await(5, TimeUnit.SECONDS)).isTrue();
            } finally {
                release.countDown();
            }
            execution.get(10, TimeUnit.SECONDS);
            cancellation.get(10, TimeUnit.SECONDS);
        }
        assertThat(service.get(job.id()).status()).isEqualTo(JobStatus.SUCCEEDED);
        assertThat(service.get(job.id()).result().get("sha256").asText()).startsWith("ba7816");
        assertThat(service.events(job.id(), 0, 10)).extracting(JobEvent::type)
                .containsExactly(JobEvent.Type.SUBMITTED, JobEvent.Type.SUCCEEDED);
    }

    @Test
    void concurrentCancellationsCommitOnlyOneTransition() throws Exception {
        UUID id = service.submit(JobType.SHA256, "abc", null).job().id();
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var actions = new ArrayList<Callable<Job>>();
            for (int i = 0; i < 8; i++) actions.add(() -> service.cancel(id));
            for (var future : pool.invokeAll(actions)) assertThat(future.get().status()).isEqualTo(JobStatus.CANCELLED);
        }
        assertThat(service.events(id, 0, 10)).hasSize(2);
        assertThat(outboxSize()).isZero();
    }

    @Test
    void retryCreatesLinkedImmediateJobAndLeavesScheduledOriginalCancelled() {
        Job source = service.submit(JobType.UPPERCASE, "later", "source", 8, Instant.now().plusSeconds(3600)).job();
        service.cancel(source.id());
        var retry = service.retry(source.id(), "retry");
        assertThat(retry.created()).isTrue();
        assertThat(retry.job().id()).isNotEqualTo(source.id());
        assertThat(retry.job().retryOf()).isEqualTo(source.id());
        assertThat(retry.job().priority()).isEqualTo(8);
        assertThat(retry.job().scheduledAt()).isNull();
        assertThat(relay.dispatchBatch()).isEqualTo(1);
        executor.execute(source.id());
        verifyNoInteractions(processor);
        executor.execute(retry.job().id());
        assertThat(service.get(retry.job().id()).result().get("text").asText()).isEqualTo("LATER");
        assertThat(service.get(source.id()).status()).isEqualTo(JobStatus.CANCELLED);
        assertThat(service.get(source.id()).scheduledAt()).isEqualTo(source.scheduledAt());
        assertThat(service.events(source.id(), 0, 10)).extracting(JobEvent::type)
                .containsExactly(JobEvent.Type.SUBMITTED, JobEvent.Type.CANCELLED, JobEvent.Type.RETRY_REQUESTED);
        assertThat(service.events(source.id(), 0, 10).getLast().relatedJobId()).isEqualTo(retry.job().id());
        assertThat(service.events(retry.job().id(), 0, 10).getFirst().relatedJobId()).isEqualTo(source.id());
    }

    @Test
    void failedJobCanRetryWithoutLosingItsOriginalFailure() {
        doThrow(new IllegalStateException("private error")).when(processor).process(JobType.SHA256, "abc");
        Job source = service.submit(JobType.SHA256, "abc", null).job();
        executor.execute(source.id());
        Job failed = service.get(source.id());
        doCallRealMethod().when(processor).process(JobType.SHA256, "abc");
        var retry = service.retry(source.id(), "retry-failed");
        executor.execute(source.id());
        executor.execute(retry.job().id());
        assertThat(service.get(source.id())).isEqualTo(failed);
        assertThat(failed.error()).isEqualTo("Job handler failed");
        assertThat(service.get(retry.job().id()).status()).isEqualTo(JobStatus.SUCCEEDED);
        assertThat(service.events(source.id(), 0, 10)).extracting(JobEvent::type)
                .containsExactly(JobEvent.Type.SUBMITTED, JobEvent.Type.FAILED, JobEvent.Type.RETRY_REQUESTED);
    }

    @Test
    void concurrentRetryReplaysCreateOneChildAndOneOutboxEntry() throws Exception {
        UUID id = service.submit(JobType.SHA256, "abc", null).job().id();
        service.cancel(id);
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var actions = new ArrayList<Callable<JobService.Submission>>();
            for (int i = 0; i < 12; i++) actions.add(() -> service.retry(id, "same-retry"));
            var results = new ArrayList<JobService.Submission>();
            for (var future : pool.invokeAll(actions)) results.add(future.get());
            assertThat(results.stream().filter(JobService.Submission::created).count()).isEqualTo(1);
            assertThat(results.stream().map(result -> result.job().id()).distinct().count()).isEqualTo(1);
        }
        assertThat(outboxSize()).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM jobs", Integer.class)).isEqualTo(2);
        assertThat(service.events(id, 0, 10)).hasSize(3);
    }

    @Test
    void retryKeysCannotCrossSourcesOrBeReusedAsOrdinarySubmissionKeys() {
        Job first = service.submit(JobType.SHA256, "abc", "source").job();
        Job second = service.submit(JobType.SHA256, "abc", null).job();
        service.cancel(first.id());
        service.cancel(second.id());
        var child = service.retry(first.id(), "retry").job();
        conflict(() -> service.retry(second.id(), "retry"));
        conflict(() -> service.retry(first.id(), "source"));
        conflict(() -> service.submit(JobType.SHA256, "abc", "retry"));
        executor.execute(child.id());
        assertThat(service.retry(first.id(), "retry").job().status()).isEqualTo(JobStatus.SUCCEEDED);
        assertThat(service.events(second.id(), 0, 10)).hasSize(2);
    }

    @Test
    void activeAndSuccessfulJobsRejectRetryAndSuccessfulJobsRejectCancellation() {
        UUID id = service.submit(JobType.SHA256, "abc", null).job().id();
        conflict(() -> service.retry(id, "queued"));
        executor.execute(id);
        conflict(() -> service.retry(id, "successful"));
        conflict(() -> service.cancel(id));
    }

    @Test
    void cancellationRollsBackStateAndOutboxWhenEventPersistenceFails() {
        UUID id = service.submit(JobType.SHA256, "abc", null).job().id();
        doThrow(new DataAccessResourceFailureException("audit unavailable")).when(repository)
                .recordEvent(id, JobEvent.Type.CANCELLED, JobStatus.CANCELLED, null);
        assertThatThrownBy(() -> service.cancel(id)).isInstanceOf(DataAccessResourceFailureException.class);
        assertThat(service.get(id).status()).isEqualTo(JobStatus.QUEUED);
        assertThat(outboxSize()).isEqualTo(1);
        assertThat(service.events(id, 0, 10)).hasSize(1);
    }

    @Test
    void retryRollsBackChildAndOutboxWhenParentHistoryCannotCommit() {
        UUID id = service.submit(JobType.SHA256, "abc", null).job().id();
        service.cancel(id);
        doThrow(new DataAccessResourceFailureException("audit unavailable")).when(repository)
                .recordEvent(eq(id), eq(JobEvent.Type.RETRY_REQUESTED), eq(JobStatus.CANCELLED), any());
        assertThatThrownBy(() -> service.retry(id, "retry-rollback"))
                .isInstanceOf(DataAccessResourceFailureException.class);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM jobs", Integer.class)).isEqualTo(1);
        assertThat(outboxSize()).isZero();
        assertThat(service.events(id, 0, 10)).hasSize(2);
        doCallRealMethod().when(repository).recordEvent(eq(id), eq(JobEvent.Type.RETRY_REQUESTED), eq(JobStatus.CANCELLED), any());
        assertThat(service.retry(id, "retry-rollback").created()).isTrue();
    }

    @Test
    void resultAndHistoryRollBackTogetherAndDuplicateDeliveryAddsNoEvent() {
        UUID id = service.submit(JobType.SHA256, "abc", null).job().id();
        doThrow(new DataAccessResourceFailureException("audit unavailable")).when(repository)
                .recordEvent(id, JobEvent.Type.SUCCEEDED, JobStatus.SUCCEEDED, null);
        assertThatThrownBy(() -> executor.execute(id)).isInstanceOf(DataAccessResourceFailureException.class);
        assertThat(service.get(id).status()).isEqualTo(JobStatus.QUEUED);
        assertThat(service.get(id).result()).isNull();
        assertThat(service.events(id, 0, 10)).hasSize(1);
        doCallRealMethod().when(repository).recordEvent(id, JobEvent.Type.SUCCEEDED, JobStatus.SUCCEEDED, null);
        executor.execute(id);
        executor.execute(id);
        assertThat(service.events(id, 0, 10)).extracting(JobEvent::type)
                .containsExactly(JobEvent.Type.SUBMITTED, JobEvent.Type.SUCCEEDED);
    }

    @Test
    void historyPagesRemainOrderedAndMissingResourcesReturn404() {
        UUID id = service.submit(JobType.SHA256, "abc", null).job().id();
        service.cancel(id);
        service.retry(id, "retry-page");
        var first = service.events(id, 0, 2).subList(0, 2);
        var second = service.events(id, 1, 2);
        assertThat(first).doesNotContainAnyElementsOf(second);
        assertThat(first.getFirst().id()).isLessThan(first.getLast().id());
        assertThat(second).hasSize(1);
        assertThatThrownBy(() -> service.events(UUID.randomUUID(), 0, 20))
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        exception -> assertThat(exception.getStatusCode().value()).isEqualTo(404));
        assertThatThrownBy(() -> service.cancel(UUID.randomUUID())).isInstanceOf(ResponseStatusException.class);
        assertThatThrownBy(() -> service.retry(UUID.randomUUID(), "missing")).isInstanceOf(ResponseStatusException.class);
    }
}
