package com.maneesh.jobplatform.jobs;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import com.fasterxml.jackson.annotation.JsonIgnore;

/** Immutable per-job policy. The attempt cap includes abandoned executions. */
public record RetryPolicy(@Min(1) @Max(25) int maxAttempts,
                          @Min(1) @Max(3600) int initialBackoffSeconds,
                          @Min(1) @Max(3600) int maxBackoffSeconds) {
    public static final RetryPolicy DEFAULT = new RetryPolicy(4, 1, 60);

    @JsonIgnore
    @AssertTrue(message = "initialBackoffSeconds must not exceed maxBackoffSeconds")
    public boolean isBackoffOrdered() { return initialBackoffSeconds <= maxBackoffSeconds; }

    public boolean valid() {
        return maxAttempts >= 1 && maxAttempts <= 25 && initialBackoffSeconds >= 1
                && initialBackoffSeconds <= maxBackoffSeconds && maxBackoffSeconds <= 3600;
    }

    public int delaySeconds(int completedAttempt) {
        long delay = initialBackoffSeconds;
        for (int i = 1; i < completedAttempt && delay < maxBackoffSeconds; i++) delay = Math.min(delay * 2, maxBackoffSeconds);
        return (int) Math.min(delay, maxBackoffSeconds);
    }
}
