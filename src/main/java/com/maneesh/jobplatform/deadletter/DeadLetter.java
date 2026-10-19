package com.maneesh.jobplatform.deadletter;

import java.time.Instant;
import java.util.UUID;

/** Sanitized inspection record; raw broker payloads and exception messages are never exposed. */
public record DeadLetter(UUID id, Source source, Reason reason, UUID jobId, Integer generation,
                         String payloadSha256, Integer payloadBytes, Instant capturedAt, State state,
                         Instant resolvedAt, UUID recoveryJobId) {
    public enum Source { BROKER, EXECUTION }
    public enum Reason { DELIVERY_FAILED, MALFORMED_MESSAGE, UNKNOWN_JOB, RETRIES_EXHAUSTED,
        PERMANENT_HANDLER, DEADLINE_EXCEEDED, RECOVERY_EXHAUSTED }
    public enum State { OPEN, RECOVERED, DISCARDED }
}
