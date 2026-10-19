package com.maneesh.jobplatform.jobs;

import java.util.List;
import java.util.UUID;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;
import com.maneesh.jobplatform.config.ExecutionProperties;
import com.maneesh.jobplatform.worker.ExecutionService;

@Service
public class JobService {
    private final JobRepository repository;
    private final ExecutionService execution;
    private final ExecutionProperties properties;

    public JobService(JobRepository repository, ExecutionService execution, ExecutionProperties properties) {
        this.repository = repository;
        this.execution = execution;
        this.properties = properties;
    }

    public record Submission(Job job, boolean created) {}

    @Transactional
    public Submission submit(JobType type, String text, String key) {
        return submit(type, text, key, 0, null);
    }

    @Transactional
    public Submission submit(JobType type, String text, String key, int priority, Instant scheduledAt) {
        return submit(type, text, key, priority, scheduledAt, null);
    }

    @Transactional
    public Submission submit(JobType type, String text, String key, int priority, Instant scheduledAt, Integer timeoutSeconds) {
        return submit(type, text, key, priority, scheduledAt, timeoutSeconds, null);
    }

    @Transactional
    public Submission submit(JobType type, String text, String key, int priority, Instant scheduledAt,
                             Integer timeoutSeconds, RetryPolicy retryPolicy) {
        return create(type, text, key, priority, scheduledAt, null,
                timeoutSeconds == null ? properties.defaultTimeoutSeconds() : timeoutSeconds,
                retryPolicy == null ? RetryPolicy.DEFAULT : retryPolicy, retryPolicy == null);
    }

    private Submission create(JobType type, String text, String key, int priority, Instant scheduledAt,
                              UUID retryOf, int timeoutSeconds, RetryPolicy policy, boolean policyOmitted) {
        if (timeoutSeconds < 1 || timeoutSeconds > 3600) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "timeoutSeconds must be between 1 and 3600");
        }
        if (!policy.valid()) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid retry policy");
        // PostgreSQL stores timestamps at microsecond precision. Normalize before comparing replays.
        Instant schedule = scheduledAt == null ? null : scheduledAt.truncatedTo(ChronoUnit.MICROS);
        UUID id = UUID.randomUUID();
        if (repository.insert(id, type, text, key, priority, schedule, retryOf, timeoutSeconds, policy).isPresent()) {
            repository.enqueue(id);
            repository.recordEvent(id, JobEvent.Type.SUBMITTED, JobStatus.QUEUED, retryOf);
            return new Submission(get(id), true);
        }
        Job existing = repository.findByKey(key).orElseThrow();
        if (existing.type() != type || !existing.text().equals(text)
                || existing.priority() != priority || !Objects.equals(existing.scheduledAt(), schedule)
                || !Objects.equals(existing.retryOf(), retryOf) || existing.timeoutSeconds() != timeoutSeconds
                || (!existing.retryPolicy().equals(policy) && !(policyOmitted && repository.legacyRetryPolicy(existing.id())))) {
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "Idempotency-Key was already used with a different request");
        }
        return new Submission(existing, false);
    }

    @Transactional
    public Job cancel(UUID id) {
        return execution.cancel(id);
    }

    @Transactional
    public Submission retry(UUID id, String key) {
        if (key == null || !key.matches("[A-Za-z0-9._:-]{1,128}")) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "A valid Idempotency-Key is required for retry");
        }
        Job source = repository.lock(id).orElseThrow(() ->
                new ResponseStatusException(HttpStatus.NOT_FOUND, "Job not found"));
        if (source.status() != JobStatus.FAILED && source.status() != JobStatus.CANCELLED) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Only failed or cancelled jobs can be retried");
        }
        // Each retry has a new ID: a delayed message for the original job can
        // never execute this new run or revive the original terminal state.
        Submission submission = create(source.type(), source.text(), key, source.priority(), null, id, source.timeoutSeconds(), source.retryPolicy(), false);
        if (submission.created()) {
            repository.recordEvent(id, JobEvent.Type.RETRY_REQUESTED, source.status(), submission.job().id());
        }
        return submission;
    }

    public List<JobEvent> events(UUID id, int page, int size) {
        get(id);
        return repository.events(id, page, size);
    }

    public List<JobAttempt> attempts(UUID id, int page, int size) {
        return execution.list(id, page, size);
    }

    public Job get(UUID id) {
        return repository.find(id).orElseThrow(() ->
                new ResponseStatusException(HttpStatus.NOT_FOUND, "Job not found"));
    }

    public List<Job> list(JobStatus status, int page, int size) {
        return repository.list(status, page, size);
    }
}
