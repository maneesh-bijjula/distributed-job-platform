package com.maneesh.jobplatform.deadletter;

import com.maneesh.jobplatform.jobs.*;
import com.maneesh.jobplatform.worker.ExecutionRepository;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

@Service
public class DeadLetterService {
    private final DeadLetterRepository inbox;
    private final JobRepository jobs;
    private final JobService jobService;
    private final ExecutionRepository execution;

    public DeadLetterService(DeadLetterRepository inbox, JobRepository jobs, JobService jobService,
                             ExecutionRepository execution) {
        this.inbox = inbox; this.jobs = jobs; this.jobService = jobService; this.execution = execution;
    }

    public record Resolution(DeadLetter entry, boolean created) {}

    public DeadLetter get(UUID id) {
        return inbox.find(id).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Dead letter not found"));
    }

    public List<DeadLetter> list(DeadLetter.State state, int page, int size) { return inbox.list(state, page, size); }

    /** Called through the Spring proxy: this transaction commits before the collector acknowledges RabbitMQ. */
    @Transactional(timeout = 10)
    public UUID capture(byte[] payload, Object generationHeader) {
        String hash;
        try { hash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(payload)); }
        catch (NoSuchAlgorithmException error) { throw new IllegalStateException("SHA-256 unavailable", error); }
        Integer generation = generation(generationHeader);
        UUID candidate = null;
        if (payload.length == 36 && generation != null) {
            String text = new String(payload, StandardCharsets.US_ASCII);
            try {
                UUID parsed = UUID.fromString(text);
                if (parsed.toString().equalsIgnoreCase(text)) candidate = parsed;
            } catch (IllegalArgumentException ignored) { /* Stored as malformed, never interpreted as executable work. */ }
        }
        var reason = candidate == null ? DeadLetter.Reason.MALFORMED_MESSAGE : DeadLetter.Reason.UNKNOWN_JOB;
        UUID jobId = null;
        if (candidate != null && jobs.find(candidate).isPresent()) {
            jobId = candidate;
            reason = DeadLetter.Reason.DELIVERY_FAILED;
        }
        return inbox.archive("broker:" + hash + ":" + (generation == null ? "invalid" : generation),
                DeadLetter.Source.BROKER, reason, jobId, generation, hash, payload.length);
    }

    private Integer generation(Object value) {
        if (value == null) return 0; // UUID-only legacy delivery.
        if (value instanceof Integer integer && integer >= 0) return integer;
        if (value instanceof Long number && number >= 0 && number <= Integer.MAX_VALUE) return number.intValue();
        return null;
    }

    @Transactional(timeout = 10)
    public Resolution recover(UUID id, String key) {
        DeadLetter replay = replay(id, key, DeadLetter.State.RECOVERED);
        if (replay != null) return new Resolution(replay, false);
        DeadLetter entry = lockOpen(id);
        if (entry.jobId() == null) throw conflict("Malformed and unknown messages cannot be recovered as jobs");
        Job source = jobs.lock(entry.jobId()).orElseThrow();
        UUID recovered;
        if (source.status() == JobStatus.QUEUED) {
            if (entry.source() != DeadLetter.Source.BROKER || entry.generation() == null
                    || entry.generation() != source.generation()) throw conflict("Delivery generation is stale");
            if (execution.deadlineExpired(source.id())) throw conflict("Execution deadline has expired");
            // Advance generation to fence delayed copies. enqueueCurrent preserves schedule/backoff.
            execution.redrive(source.id());
            recovered = source.id();
        } else if (source.status() == JobStatus.FAILED || source.status() == JobStatus.CANCELLED) {
            // The inbox owns action idempotency. Use an internal submission key so action keys
            // cannot collide with ordinary submission/manual-retry keys.
            recovered = jobService.retry(source.id(), "dead-letter:" + UUID.randomUUID()).job().id();
        } else {
            throw conflict("Only queued, failed, or cancelled jobs can be recovered");
        }
        jobs.recordEvent(source.id(), JobEvent.Type.DEAD_LETTER_RECOVERED, source.status(), recovered);
        inbox.resolve(id, DeadLetter.State.RECOVERED, key, recovered);
        return new Resolution(get(id), true);
    }

    @Transactional(timeout = 10)
    public Resolution discard(UUID id, String key) {
        DeadLetter replay = replay(id, key, DeadLetter.State.DISCARDED);
        if (replay != null) return new Resolution(replay, false);
        DeadLetter entry = lockOpen(id);
        if (entry.jobId() != null) {
            Job job = jobs.lock(entry.jobId()).orElseThrow();
            jobs.recordEvent(job.id(), JobEvent.Type.DEAD_LETTER_DISCARDED, job.status(), null);
        }
        inbox.resolve(id, DeadLetter.State.DISCARDED, key, null);
        return new Resolution(get(id), true);
    }

    private DeadLetter replay(UUID id, String key, DeadLetter.State target) {
        if (key == null || !key.matches("[A-Za-z0-9._:-]{1,128}")) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "A valid Idempotency-Key is required");
        }
        inbox.lockKey(key);
        var replay = inbox.resolution(key);
        if (replay.isEmpty()) return null;
        if (!replay.get().id().equals(id) || replay.get().state() != target) {
            throw conflict("Idempotency-Key was already used for another dead-letter action");
        }
        return replay.get();
    }

    private DeadLetter lockOpen(UUID id) {
        DeadLetter entry = inbox.lock(id).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Dead letter not found"));
        if (entry.state() != DeadLetter.State.OPEN) throw conflict("Dead letter is already resolved");
        return entry;
    }

    private ResponseStatusException conflict(String message) { return new ResponseStatusException(HttpStatus.CONFLICT, message); }
}
