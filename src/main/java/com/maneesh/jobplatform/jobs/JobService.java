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

@Service
public class JobService {
    private final JobRepository repository;

    public JobService(JobRepository repository) {
        this.repository = repository;
    }

    public record Submission(Job job, boolean created) {}

    @Transactional
    public Submission submit(JobType type, String text, String key) {
        return submit(type, text, key, 0, null);
    }

    @Transactional
    public Submission submit(JobType type, String text, String key, int priority, Instant scheduledAt) {
        return create(type, text, key, priority, scheduledAt, null);
    }

    private Submission create(JobType type, String text, String key, int priority, Instant scheduledAt,
                              UUID retryOf) {
        // PostgreSQL stores timestamps at microsecond precision. Normalize before comparing replays.
        Instant schedule = scheduledAt == null ? null : scheduledAt.truncatedTo(ChronoUnit.MICROS);
        UUID id = UUID.randomUUID();
        if (repository.insert(id, type, text, key, priority, schedule, retryOf).isPresent()) {
            repository.enqueue(id);
            repository.recordEvent(id, JobEvent.Type.SUBMITTED, JobStatus.QUEUED, retryOf);
            return new Submission(get(id), true);
        }
        Job existing = repository.findByKey(key).orElseThrow();
        if (existing.type() != type || !existing.text().equals(text)
                || existing.priority() != priority || !Objects.equals(existing.scheduledAt(), schedule)
                || !Objects.equals(existing.retryOf(), retryOf)) {
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "Idempotency-Key was already used with a different request");
        }
        return new Submission(existing, false);
    }

    @Transactional
    public Job cancel(UUID id) {
        Job job = repository.lock(id).orElseThrow(() ->
                new ResponseStatusException(HttpStatus.NOT_FOUND, "Job not found"));
        if (job.status() == JobStatus.CANCELLED) return job;
        if (job.status() != JobStatus.QUEUED) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Only queued jobs can be cancelled");
        }
        repository.cancel(id);
        repository.recordEvent(id, JobEvent.Type.CANCELLED, JobStatus.CANCELLED, null);
        return get(id);
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
        Submission submission = create(source.type(), source.text(), key, source.priority(), null, id);
        if (submission.created()) {
            repository.recordEvent(id, JobEvent.Type.RETRY_REQUESTED, source.status(), submission.job().id());
        }
        return submission;
    }

    public List<JobEvent> events(UUID id, int page, int size) {
        get(id);
        return repository.events(id, page, size);
    }

    public Job get(UUID id) {
        return repository.find(id).orElseThrow(() ->
                new ResponseStatusException(HttpStatus.NOT_FOUND, "Job not found"));
    }

    public List<Job> list(JobStatus status, int page, int size) {
        return repository.list(status, page, size);
    }
}
