package com.maneesh.jobplatform.jobs;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;
import com.fasterxml.jackson.annotation.JsonIgnore;

public record Job(UUID id, JobType type, String text, JobStatus status, JsonNode result,
                  String error, Instant createdAt, Instant completedAt, int priority, Instant scheduledAt,
                  UUID retryOf, int timeoutSeconds, int attemptCount, UUID activeAttemptId,
                  Instant deadlineAt, @JsonIgnore int generation) {
    public Job(UUID id, JobType type, String text, JobStatus status, JsonNode result, String error,
               Instant createdAt, Instant completedAt, int priority, Instant scheduledAt, UUID retryOf) {
        this(id, type, text, status, result, error, createdAt, completedAt, priority, scheduledAt,
                retryOf, 30, 0, null, null, 0);
    }
}
