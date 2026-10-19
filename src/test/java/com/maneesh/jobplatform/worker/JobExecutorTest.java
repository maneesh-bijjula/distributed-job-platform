package com.maneesh.jobplatform.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.maneesh.jobplatform.config.ExecutionProperties;
import com.maneesh.jobplatform.jobs.*;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class JobExecutorTest {
    private final ExecutionService execution = mock(ExecutionService.class);
    private final JobProcessor processor = mock(JobProcessor.class);
    private final ScheduledThreadPoolExecutor monitor = mock(ScheduledThreadPoolExecutor.class);
    private final ScheduledFuture<?> heartbeat = mock(ScheduledFuture.class);
    private final JobExecutor executor = new JobExecutor(execution, processor, monitor,
            new ExecutionProperties(15, 2000, 30, 3, 4));
    private final UUID id = UUID.randomUUID();
    private ExecutionService.Claim claim;

    @BeforeEach
    void setUp() {
        var now = Instant.now();
        var job = new Job(id, JobType.SHA256, "abc", JobStatus.RUNNING, null, null, now, null, 0, null, null);
        var attempt = new JobAttempt(UUID.randomUUID(), id, 1, 0, "worker-test", JobAttempt.Status.RUNNING,
                now, now, now.plusSeconds(15), now.plusSeconds(30), null, null);
        claim = new ExecutionService.Claim(job, attempt, UUID.randomUUID(), new ExecutionRepository.Window(true, true, 15000, 30000));
        when(execution.claim(id, 0)).thenReturn(Optional.of(claim));
        doReturn(heartbeat).when(monitor).scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
    }

    @Test
    void unclaimedDeliveryDoesNotStartHandlerOrHeartbeat() {
        when(execution.claim(id, 0)).thenReturn(Optional.empty());
        executor.execute(id);
        verifyNoInteractions(processor, monitor);
    }

    @Test
    void resultIsCommittedThroughFencedLifecycleAndMonitorIsReleased() {
        var result = new ObjectMapper().createObjectNode().put("sha256", "value");
        when(processor.process(eq(JobType.SHA256), eq("abc"), any())).thenReturn(result);
        executor.execute(id);
        verify(execution).succeed(claim, result);
        verify(execution, never()).fail(any());
        verify(heartbeat).cancel(false);
    }

    @Test
    void handlerFailureUsesSanitizedLifecycleFailureAndReleasesMonitor() {
        when(processor.process(any(), any(), any())).thenThrow(new IllegalStateException("private details"));
        executor.execute(id);
        verify(execution).fail(claim);
        verify(execution, never()).succeed(any(), any());
        verify(heartbeat).cancel(false);
    }

    @Test
    void retryableHandlerFailureUsesPersistedRetryLifecycleAndReleasesMonitor() {
        when(processor.process(any(), any(), any())).thenThrow(new RetryableJobException("private dependency URL"));
        executor.execute(id);
        verify(execution).fail(claim, HandlerFailure.RETRYABLE_INFRASTRUCTURE);
        verify(execution, never()).succeed(any(), any());
        verify(heartbeat).cancel(false);
    }

    @Test
    void completionStorageFailureEscapesWithoutBeingClassifiedAsHandlerFailure() {
        var result = new ObjectMapper().createObjectNode();
        when(processor.process(any(), any(), any())).thenReturn(result);
        doThrow(new DataAccessResourceFailureException("storage unavailable")).when(execution).succeed(claim, result);
        assertThatThrownBy(() -> executor.execute(id)).isInstanceOf(DataAccessResourceFailureException.class);
        verify(execution, never()).fail(any());
        verify(heartbeat).cancel(false);
    }

    @Test
    void cooperativeStopReconcilesClaimWithoutReportingHandlerFailure() {
        when(processor.process(any(), any(), any())).thenThrow(new ExecutionControl.Stopped());
        executor.execute(id);
        verify(execution).reconcile(claim);
        verify(execution, never()).fail(any());
        verify(execution, never()).succeed(any(), any());
        verify(heartbeat).cancel(false);
    }

    @Test
    void malformedAndNegativeGenerationMessagesAreRejected() {
        var listener = new JobListener(executor);
        assertThatThrownBy(() -> listener.receive("not-a-uuid"))
                .isInstanceOf(org.springframework.amqp.AmqpRejectAndDontRequeueException.class);
        assertThatThrownBy(() -> listener.receive(id.toString(), -1))
                .isInstanceOf(org.springframework.amqp.AmqpRejectAndDontRequeueException.class);
        verifyNoInteractions(execution);
    }

    @Test
    void deliveryGenerationIsPassedToClaim() {
        when(execution.claim(id, 2)).thenReturn(Optional.empty());
        new JobListener(executor).receive(id.toString(), 2);
        verify(execution).claim(id, 2);
        verifyNoInteractions(processor);
    }
}
