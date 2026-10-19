package com.maneesh.jobplatform.api;

import com.maneesh.jobplatform.jobs.Job;
import com.maneesh.jobplatform.jobs.JobEvent;
import com.maneesh.jobplatform.jobs.JobAttempt;
import com.fasterxml.jackson.databind.JsonNode;
import com.maneesh.jobplatform.jobs.JobService;
import com.maneesh.jobplatform.jobs.JobStatus;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.security.Principal;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/jobs")
public class JobController {
    private final JobService service;

    public JobController(JobService service) {
        this.service = service;
    }

    @PostMapping
    public ResponseEntity<Job> submit(Principal principal, @Valid @RequestBody CreateJobRequest request,
            @RequestHeader(value = "Idempotency-Key", required = false)
            @Pattern(regexp = "[A-Za-z0-9._:-]{1,128}") String key) {
        var submission = service.submitFor(UUID.fromString(principal.getName()),request.type(), request.text(), key, request.priority(), request.scheduledAt(), request.timeoutSeconds(), request.retryPolicy());
        return ResponseEntity.status(submission.created() ? 202 : 200)
                .location(URI.create("/api/v1/jobs/" + submission.job().id())).body(submission.job());
    }

    @GetMapping("/{id}")
    public Job get(Principal principal, @PathVariable UUID id) {
        return service.getFor(UUID.fromString(principal.getName()),id);
    }

    @PostMapping("/{id}/cancel")
    public Job cancel(Principal principal, @PathVariable UUID id, @RequestBody(required = false) JsonNode body) {
        requireEmptyBody(body);
        return service.cancelFor(UUID.fromString(principal.getName()),id);
    }

    @PostMapping("/{id}/retry")
    public ResponseEntity<Job> retry(Principal principal, @PathVariable UUID id,
            @RequestHeader("Idempotency-Key") @Pattern(regexp = "[A-Za-z0-9._:-]{1,128}") String key,
            @RequestBody(required = false) JsonNode body) {
        requireEmptyBody(body);
        var submission = service.retryFor(UUID.fromString(principal.getName()),id, key);
        return ResponseEntity.status(submission.created() ? 202 : 200)
                .location(URI.create("/api/v1/jobs/" + submission.job().id())).body(submission.job());
    }

    private void requireEmptyBody(JsonNode body) {
        if (body != null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "This action accepts no request body");
        }
    }

    public record EventPage(List<JobEvent> items, int page, int size, boolean hasNext) {}

    @GetMapping("/{id}/events")
    public EventPage events(Principal principal, @PathVariable UUID id,
                           @RequestParam(defaultValue = "0") @Min(0) @Max(1000000) int page,
                           @RequestParam(defaultValue = "20") @Min(1) @Max(100) int size) {
        List<JobEvent> events = service.eventsFor(UUID.fromString(principal.getName()),id, page, size);
        return new EventPage(events.subList(0, Math.min(size, events.size())), page, size, events.size() > size);
    }

    public record JobPage(List<Job> items, int page, int size, boolean hasNext) {}

    public record AttemptPage(List<JobAttempt> items, int page, int size, boolean hasNext) {}

    @GetMapping("/{id}/attempts")
    public AttemptPage attempts(Principal principal, @PathVariable UUID id,
                               @RequestParam(defaultValue = "0") @Min(0) @Max(1000000) int page,
                               @RequestParam(defaultValue = "20") @Min(1) @Max(100) int size) {
        List<JobAttempt> attempts = service.attemptsFor(UUID.fromString(principal.getName()),id, page, size);
        return new AttemptPage(attempts.subList(0, Math.min(size, attempts.size())), page, size, attempts.size() > size);
    }

    @GetMapping
    public JobPage list(Principal principal, @RequestParam(required = false) JobStatus status,
                        @RequestParam(defaultValue = "0") @Min(0) @Max(1000000) int page,
                        @RequestParam(defaultValue = "20") @Min(1) @Max(100) int size) {
        // The repository fetches one extra row to determine whether another page exists.
        List<Job> jobs = service.listFor(UUID.fromString(principal.getName()),status, page, size);
        return new JobPage(jobs.subList(0, Math.min(size, jobs.size())), page, size, jobs.size() > size);
    }
}
