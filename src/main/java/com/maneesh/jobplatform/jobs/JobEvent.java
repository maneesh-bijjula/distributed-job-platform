package com.maneesh.jobplatform.jobs;

import java.time.Instant;
import java.util.UUID;

public record JobEvent(long id, UUID jobId, Type type, JobStatus status,
                       UUID relatedJobId, Instant occurredAt, UUID attemptId) {
    public JobEvent(long id, UUID jobId, Type type, JobStatus status, UUID relatedJobId, Instant occurredAt) {
        this(id, jobId, type, status, relatedJobId, occurredAt, null);
    }
    public enum Type {
        BASELINE, SUBMITTED, STARTED, SUCCEEDED, FAILED, CANCELLED, RETRY_REQUESTED,
        LEASE_EXPIRED, TIMED_OUT, RECOVERY_EXHAUSTED, RETRY_SCHEDULED, RETRIES_EXHAUSTED,
        DEAD_LETTER_RECOVERED, DEAD_LETTER_DISCARDED
    }
}
