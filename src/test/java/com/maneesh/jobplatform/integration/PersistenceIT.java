package com.maneesh.jobplatform.integration;

import com.maneesh.jobplatform.jobs.*;
import com.maneesh.jobplatform.messaging.JobPublisher;
import com.maneesh.jobplatform.messaging.OutboxRelay;
import com.maneesh.jobplatform.worker.JobExecutor;
import com.maneesh.jobplatform.worker.JobProcessor;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
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
class PersistenceIT {
    @Autowired JobService service;
    @Autowired JobExecutor executor;
    @Autowired OutboxRelay relay;
    @Autowired JdbcTemplate jdbc;
    @MockitoBean JobPublisher publisher;
    @MockitoSpyBean JobProcessor processor;

    @BeforeEach
    void resetDatabase() {
        jdbc.execute("TRUNCATE job_outbox, jobs");
    }

    private int outboxSize() {
        return jdbc.queryForObject("SELECT count(*) FROM job_outbox", Integer.class);
    }

    @Test
    void jobAndOutboxAreCommittedTogether() {
        var submission = service.submit(JobType.SHA256, "abc", null);
        assertThat(submission.created()).isTrue();
        assertThat(service.get(submission.job().id()).status()).isEqualTo(JobStatus.QUEUED);
        assertThat(outboxSize()).isEqualTo(1);
    }

    @Test
    void concurrentSubmissionsWithSameKeyCreateOneJobAndOneMessage() throws Exception {
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var tasks = new ArrayList<Callable<JobService.Submission>>();
            for (int i = 0; i < 12; i++) {
                tasks.add(() -> service.submit(JobType.SHA256, "abc", "same-key"));
            }
            var results = new ArrayList<JobService.Submission>();
            for (var future : pool.invokeAll(tasks)) results.add(future.get());
            assertThat(results.stream().map(result -> result.job().id()).distinct().count()).isEqualTo(1);
            assertThat(results.stream().filter(JobService.Submission::created).count()).isEqualTo(1);
            assertThat(outboxSize()).isEqualTo(1);
        }
    }

    @Test
    void changingRequestForExistingKeyIsAConflict() {
        service.submit(JobType.SHA256, "abc", "same-key");
        assertThatThrownBy(() -> service.submit(JobType.SHA256, "different", "same-key"))
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        exception -> assertThat(exception.getStatusCode().value()).isEqualTo(409));
        assertThat(outboxSize()).isEqualTo(1);
    }

    @Test
    void publishFailureRollsBackEntireOutboxBatch() {
        service.submit(JobType.SHA256, "one", null);
        service.submit(JobType.SHA256, "two", null);
        doNothing().doThrow(new IllegalStateException("broker unavailable")).when(publisher).publish(any(), anyInt());
        assertThatThrownBy(relay::dispatchBatch).isInstanceOf(IllegalStateException.class);
        assertThat(outboxSize()).isEqualTo(2);
        doNothing().when(publisher).publish(any(), anyInt());
        assertThat(relay.dispatchBatch()).isEqualTo(2);
        assertThat(outboxSize()).isZero();
    }

    @Test
    void concurrentDuplicateDeliveriesExecuteHandlerOnce() throws Exception {
        UUID id = service.submit(JobType.SHA256, "abc", null).job().id();
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var tasks = new ArrayList<Callable<Void>>();
            for (int i = 0; i < 8; i++) {
                tasks.add(() -> { executor.execute(id); return null; });
            }
            for (var future : pool.invokeAll(tasks)) future.get();
        }
        assertThat(service.get(id).status()).isEqualTo(JobStatus.SUCCEEDED);
        verify(processor, times(1)).process(JobType.SHA256, "abc");
    }

    @Test
    void handlerFailureIsDurableAndIsNotReexecuted() {
        doThrow(new IllegalStateException("handler failed")).when(processor).process(JobType.SHA256, "abc");
        UUID id = service.submit(JobType.SHA256, "abc", null).job().id();
        executor.execute(id);
        executor.execute(id);
        Job failed = service.get(id);
        assertThat(failed.status()).isEqualTo(JobStatus.FAILED);
        assertThat(failed.error()).isEqualTo("Job handler failed");
        assertThat(failed.completedAt()).isNotNull();
        assertThat(failed.result()).isNull();
        verify(processor, times(1)).process(JobType.SHA256, "abc");
    }

    @Test
    void paginatedQueriesHaveNoOverlapAndFilterByStatus() {
        for (int i = 0; i < 5; i++) service.submit(JobType.WORD_COUNT, "job " + i, null);
        var first = service.list(JobStatus.QUEUED, 0, 2).subList(0, 2);
        var second = service.list(JobStatus.QUEUED, 1, 2).subList(0, 2);
        assertThat(first).doesNotContainAnyElementsOf(second);
        assertThat(service.list(JobStatus.SUCCEEDED, 0, 10)).isEmpty();
        assertThat(service.list(JobStatus.QUEUED, 2, 2)).hasSize(1);
    }

    @Test
    void sqlLikeInputIsStoredAsData() {
        String text = "'; DROP TABLE jobs; --";
        var job = service.submit(JobType.UPPERCASE, text, null).job();
        executor.execute(job.id());
        assertThat(service.get(job.id()).result().get("text").asText()).isEqualTo("'; DROP TABLE JOBS; --");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM jobs", Integer.class)).isEqualTo(1);
    }

    @Test
    void scheduledIdempotentReplayNormalizesSubMicrosecondPrecision() {
        Instant schedule = Instant.parse("2030-06-01T12:00:00.123456789Z");
        var first = service.submit(JobType.SHA256, "abc", "scheduled", 5, schedule);
        var replay = service.submit(JobType.SHA256, "abc", "scheduled", 5, schedule);
        assertThat(replay.created()).isFalse();
        assertThat(replay.job().id()).isEqualTo(first.job().id());
        assertThat(replay.job().scheduledAt()).isEqualTo(schedule.truncatedTo(ChronoUnit.MICROS));
        assertThat(replay.job().priority()).isEqualTo(5);
        assertThat(outboxSize()).isEqualTo(1);
    }

    @Test
    void changingPriorityOrScheduleForExistingKeyConflicts() {
        Instant schedule = Instant.parse("2030-06-01T12:00:00Z");
        service.submit(JobType.SHA256, "abc", "scheduled", 5, schedule);
        assertThatThrownBy(() -> service.submit(JobType.SHA256, "abc", "scheduled", 6, schedule))
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        exception -> assertThat(exception.getStatusCode().value()).isEqualTo(409));
        assertThatThrownBy(() -> service.submit(JobType.SHA256, "abc", "scheduled", 5, schedule.plusSeconds(1)))
                .isInstanceOfSatisfying(ResponseStatusException.class,
                        exception -> assertThat(exception.getStatusCode().value()).isEqualTo(409));
    }

    @Test
    void concurrentRelaysClaimDisjointBatchesWithoutBlockingEachOther() throws Exception {
        for (int i = 0; i < 40; i++) service.submit(JobType.SHA256, "job " + i, null);
        var published = new ConcurrentLinkedQueue<UUID>();
        var bothRelaysEntered = new CountDownLatch(2);
        doAnswer(call -> {
            published.add(call.getArgument(0));
            bothRelaysEntered.countDown();
            assertThat(bothRelaysEntered.await(5, TimeUnit.SECONDS)).isTrue();
            return null;
        }).when(publisher).publish(any(), anyInt());
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var first = pool.submit(relay::dispatchBatch);
            var second = pool.submit(relay::dispatchBatch);
            assertThat(first.get(10, TimeUnit.SECONDS)).isEqualTo(20);
            assertThat(second.get(10, TimeUnit.SECONDS)).isEqualTo(20);
        }
        assertThat(published).hasSize(40).doesNotHaveDuplicates();
        assertThat(outboxSize()).isZero();
    }
}
