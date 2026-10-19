package com.maneesh.jobplatform.jobs;

import java.time.Instant;
import java.util.UUID;

public record JobEvent(long id, UUID jobId, Type type, JobStatus status,
                       UUID relatedJobId, Instant occurredAt) {
    public enum Type {
        BASELINE, SUBMITTED, SUCCEEDED, FAILED, CANCELLED, RETRY_REQUESTED
    }
}
