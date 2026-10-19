package com.maneesh.jobplatform.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.maneesh.jobplatform.config.ExecutionProperties;
import com.maneesh.jobplatform.jobs.*;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

@Service
public class ExecutionService {
    private final JobRepository jobs;
    private final ExecutionRepository attempts;
    private final ExecutionProperties properties;
    private final String workerId = "worker-" + UUID.randomUUID();

    public record Claim(Job job, JobAttempt attempt, UUID token, ExecutionRepository.Window window) {}

    public ExecutionService(JobRepository jobs, ExecutionRepository attempts, ExecutionProperties properties) {
        this.jobs = jobs;
        this.attempts = attempts;
        this.properties = properties;
    }

    @Transactional(timeout = 10)
    public Optional<Claim> claim(UUID id, int generation) {
        var found = jobs.lock(id);
        if (found.isEmpty()) return Optional.empty();
        Job job = found.get();
        if (job.status() != JobStatus.QUEUED || job.generation() != generation || !attempts.due(id)) return Optional.empty();
        if (attempts.deadlineExpired(id)) {
            timeoutQueued(job);
            return Optional.empty();
        }
        UUID token = UUID.randomUUID();
        JobAttempt attempt = attempts.start(job, UUID.randomUUID(), token, workerId, properties.leaseSeconds());
        jobs.recordEvent(id, JobEvent.Type.STARTED, JobStatus.RUNNING, null, attempt.id());
        return Optional.of(new Claim(jobs.find(id).orElseThrow(), attempt, token, attempts.window(attempt.id())));
    }

    @Transactional(timeout = 10)
    public Optional<ExecutionRepository.Window> heartbeat(Claim claim) {
        Job job = owned(claim);
        if (job == null) return Optional.empty();
        if (!attempts.renew(claim.attempt().id(), claim.token(), properties.leaseSeconds())) {
            expire(job, claim.attempt());
            return Optional.empty();
        }
        return Optional.of(attempts.window(claim.attempt().id()));
    }

    @Transactional(timeout = 10)
    public boolean succeed(Claim claim, JsonNode result) {
        Job job = owned(claim);
        if (job == null) return false;
        if (jobs.succeed(job.id(), result) != 1) {
            expire(job, claim.attempt());
            return false;
        }
        attempts.finish(claim.attempt().id(), JobAttempt.Status.SUCCEEDED, null);
        jobs.recordEvent(job.id(), JobEvent.Type.SUCCEEDED, JobStatus.SUCCEEDED, null, claim.attempt().id());
        return true;
    }

    @Transactional(timeout = 10)
    public boolean fail(Claim claim) {
        Job job = owned(claim);
        if (job == null) return false;
        if (jobs.fail(job.id(), "Job handler failed") != 1) {
            expire(job, claim.attempt());
            return false;
        }
        attempts.finish(claim.attempt().id(), JobAttempt.Status.FAILED, "Job handler failed");
        jobs.recordEvent(job.id(), JobEvent.Type.FAILED, JobStatus.FAILED, null, claim.attempt().id());
        return true;
    }

    @Transactional(timeout = 10)
    public void reconcile(Claim claim) {
        Job job = owned(claim);
        if (job != null) expire(job, claim.attempt());
    }

    @Transactional(timeout = 10)
    public Job cancel(UUID id) {
        Job job = jobs.lock(id).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Job not found"));
        if (job.status() == JobStatus.CANCELLED) return job;
        if (job.status() != JobStatus.QUEUED && job.status() != JobStatus.RUNNING) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Only queued or running jobs can be cancelled");
        }
        if (job.activeAttemptId() != null) attempts.finish(job.activeAttemptId(), JobAttempt.Status.CANCELLED, null);
        jobs.cancel(id);
        if (job.activeAttemptId() == null) jobs.recordEvent(id, JobEvent.Type.CANCELLED, JobStatus.CANCELLED, null);
        else jobs.recordEvent(id, JobEvent.Type.CANCELLED, JobStatus.CANCELLED, null, job.activeAttemptId());
        return jobs.find(id).orElseThrow();
    }

    @Transactional(timeout = 10)
    public int recoverBatch() {
        List<UUID> ids = attempts.expiredJobs();
        for (UUID id : ids) {
            Job job = jobs.find(id).orElseThrow(); // expiredJobs holds each job-row lock.
            if (job.status() == JobStatus.QUEUED) timeoutQueued(job);
            else expire(job, attempts.find(job.activeAttemptId()).orElseThrow());
        }
        return ids.size();
    }

    public List<JobAttempt> list(UUID id, int page, int size) {
        if (jobs.find(id).isEmpty()) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Job not found");
        return attempts.list(id, page, size);
    }

    private Job owned(Claim claim) {
        var found = jobs.lock(claim.job().id());
        if (found.isEmpty()) return null;
        Job job = found.get();
        if (job.status() != JobStatus.RUNNING || !claim.attempt().id().equals(job.activeAttemptId())
                || job.generation() != claim.attempt().generation()) return null;
        return attempts.owned(claim.attempt().id(), claim.token()).isPresent() ? job : null;
    }

    private void expire(Job job, JobAttempt attempt) {
        var window = attempts.window(attempt.id());
        if (!window.deadlineLive()) {
            attempts.finish(attempt.id(), JobAttempt.Status.TIMED_OUT, "Execution deadline exceeded");
            attempts.failJob(job.id(), "Execution deadline exceeded");
            jobs.recordEvent(job.id(), JobEvent.Type.TIMED_OUT, JobStatus.FAILED, null, attempt.id());
        } else if (!window.leaseLive()) {
            attempts.finish(attempt.id(), JobAttempt.Status.ABANDONED, "Worker lease expired");
            if (attempts.recoveryCount(job.id()) >= properties.maxRecoveries()) {
                attempts.failJob(job.id(), "Worker recovery limit exceeded");
                jobs.recordEvent(job.id(), JobEvent.Type.RECOVERY_EXHAUSTED, JobStatus.FAILED, null, attempt.id());
            } else {
                attempts.requeue(job.id());
                jobs.recordEvent(job.id(), JobEvent.Type.LEASE_EXPIRED, JobStatus.QUEUED, null, attempt.id());
            }
        }
    }

    private void timeoutQueued(Job job) {
        attempts.failJob(job.id(), "Execution deadline exceeded");
        jobs.recordEvent(job.id(), JobEvent.Type.TIMED_OUT, JobStatus.FAILED, null);
    }
}
