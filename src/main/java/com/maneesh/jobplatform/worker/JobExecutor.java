package com.maneesh.jobplatform.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.maneesh.jobplatform.jobs.JobRepository;
import com.maneesh.jobplatform.jobs.JobEvent;
import com.maneesh.jobplatform.jobs.JobStatus;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JobExecutor {
    private static final Logger log = LoggerFactory.getLogger(JobExecutor.class);
    private final JobRepository repository;
    private final JobProcessor processor;

    public JobExecutor(JobRepository repository, JobProcessor processor) {
        this.repository = repository;
        this.processor = processor;
    }

    @Transactional(timeout = 30)
    public void execute(UUID id) {
        var candidate = repository.lock(id);
        if (candidate.isEmpty()) {
            log.warn("Ignoring message for missing job {}", id);
            return;
        }
        var job = candidate.get();
        if (job.status() != JobStatus.QUEUED) {
            return;
        }
        JsonNode result;
        try {
            // Handlers are bounded, local computations with no external side effects.
            result = processor.process(job.type(), job.text());
        } catch (RuntimeException exception) {
            log.error("Handler failed for job {}", id, exception);
            repository.fail(id, "Job handler failed");
            repository.recordEvent(id, JobEvent.Type.FAILED, JobStatus.FAILED, null);
            return;
        }
        // Storage failures escape the handler catch so the transaction rolls back and AMQP retries.
        repository.succeed(id, result);
        repository.recordEvent(id, JobEvent.Type.SUCCEEDED, JobStatus.SUCCEEDED, null);
    }
}
