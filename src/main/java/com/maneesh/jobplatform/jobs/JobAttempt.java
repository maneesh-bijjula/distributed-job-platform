package com.maneesh.jobplatform.jobs;

import java.time.Instant;
import java.util.UUID;
import com.maneesh.jobplatform.worker.HandlerFailure;

public record JobAttempt(UUID id, UUID jobId, int number, int generation, String workerId,
                         Status status, Instant startedAt, Instant heartbeatAt, Instant leaseUntil,
                         Instant deadlineAt, Instant finishedAt, String error, HandlerFailure failureKind) {
    public JobAttempt(UUID id, UUID jobId, int number, int generation, String workerId, Status status,
                      Instant startedAt, Instant heartbeatAt, Instant leaseUntil, Instant deadlineAt,
                      Instant finishedAt, String error) {
        this(id, jobId, number, generation, workerId, status, startedAt, heartbeatAt, leaseUntil,
                deadlineAt, finishedAt, error, null);
    }
    public enum Status { RUNNING, SUCCEEDED, FAILED, CANCELLED, TIMED_OUT, ABANDONED }
}
