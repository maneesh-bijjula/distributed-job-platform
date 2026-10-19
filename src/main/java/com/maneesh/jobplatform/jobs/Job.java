package com.maneesh.jobplatform.jobs;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.maneesh.jobplatform.auth.ApiPrincipal;

public record Job(UUID id, JobType type, String text, JobStatus status, JsonNode result,
                  String error, Instant createdAt, Instant completedAt, int priority, Instant scheduledAt,
                  UUID retryOf, int timeoutSeconds, int attemptCount, UUID activeAttemptId,
                  Instant deadlineAt, @JsonIgnore int generation, RetryPolicy retryPolicy, Instant retryAt, UUID ownerId) {
    public Job(UUID id, JobType type, String text, JobStatus status, JsonNode result, String error,
               Instant createdAt, Instant completedAt, int priority, Instant scheduledAt, UUID retryOf,
               int timeoutSeconds, int attemptCount, UUID activeAttemptId, Instant deadlineAt, int generation,
               RetryPolicy retryPolicy, Instant retryAt) {
        this(id,type,text,status,result,error,createdAt,completedAt,priority,scheduledAt,retryOf,
                timeoutSeconds,attemptCount,activeAttemptId,deadlineAt,generation,retryPolicy,retryAt,ApiPrincipal.LEGACY_OWNER);
    }
    public Job(UUID id, JobType type, String text, JobStatus status, JsonNode result, String error,
               Instant createdAt, Instant completedAt, int priority, Instant scheduledAt, UUID retryOf,
               int timeoutSeconds, int attemptCount, UUID activeAttemptId, Instant deadlineAt, int generation) {
        this(id, type, text, status, result, error, createdAt, completedAt, priority, scheduledAt, retryOf,
                timeoutSeconds, attemptCount, activeAttemptId, deadlineAt, generation, RetryPolicy.DEFAULT, null);
    }
    public Job(UUID id, JobType type, String text, JobStatus status, JsonNode result, String error,
               Instant createdAt, Instant completedAt, int priority, Instant scheduledAt, UUID retryOf) {
        this(id, type, text, status, result, error, createdAt, completedAt, priority, scheduledAt,
                retryOf, 30, 0, null, null, 0);
    }
}
