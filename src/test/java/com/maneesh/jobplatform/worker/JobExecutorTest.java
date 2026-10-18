package com.maneesh.jobplatform.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.maneesh.jobplatform.jobs.*;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class JobExecutorTest {
    private final JobRepository repository = mock(JobRepository.class);
    private final JobProcessor processor = mock(JobProcessor.class);
    private final JobExecutor executor = new JobExecutor(repository, processor);
    private final UUID id = UUID.randomUUID();

    private Job job(JobStatus status) {
        return new Job(id, JobType.SHA256, "abc", status, null, null, Instant.now(), null, 0, null);
    }

    @BeforeEach
    void setUp() {
        when(repository.lock(id)).thenReturn(Optional.of(job(JobStatus.QUEUED)));
    }

    @Test
    void redeliveredCompletedJobDoesNotExecuteAgain() {
        when(repository.lock(id)).thenReturn(Optional.of(job(JobStatus.SUCCEEDED)));
        executor.execute(id);
        verifyNoInteractions(processor);
        verify(repository, never()).succeed(any(), any());
    }

    @Test
    void missingJobIsIgnored() {
        when(repository.lock(id)).thenReturn(Optional.empty());
        executor.execute(id);
        verifyNoInteractions(processor);
    }

    @Test
    void handlerFailurePersistsSanitizedFailure() {
        when(processor.process(any(), any())).thenThrow(new IllegalStateException("private implementation details"));
        executor.execute(id);
        verify(repository).fail(id, "Job handler failed");
        verify(repository, never()).succeed(any(), any());
    }

    @Test
    void storageFailureEscapesForTransactionRollbackAndBrokerRetry() {
        var result = new ObjectMapper().createObjectNode().put("sha256", "value");
        when(processor.process(any(), any())).thenReturn(result);
        doThrow(new DataAccessResourceFailureException("database unavailable"))
                .when(repository).succeed(id, result);
        assertThatThrownBy(() -> executor.execute(id)).isInstanceOf(DataAccessResourceFailureException.class);
        verify(repository, never()).fail(any(), any());
    }

    @Test
    void malformedBrokerMessageIsRejected() {
        var listener = new JobListener(executor);
        assertThatThrownBy(() -> listener.receive("not-a-uuid"))
                .isInstanceOf(org.springframework.amqp.AmqpRejectAndDontRequeueException.class);
        verifyNoInteractions(repository);
    }
}
