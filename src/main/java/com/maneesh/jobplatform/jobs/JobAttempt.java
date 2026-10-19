package com.maneesh.jobplatform.jobs;

import java.time.Instant;
import java.util.UUID;

public record JobAttempt(UUID id, UUID jobId, int number, int generation, String workerId,
                         Status status, Instant startedAt, Instant heartbeatAt, Instant leaseUntil,
                         Instant deadlineAt, Instant finishedAt, String error) {
    public enum Status { RUNNING, SUCCEEDED, FAILED, CANCELLED, TIMED_OUT, ABANDONED }
}
