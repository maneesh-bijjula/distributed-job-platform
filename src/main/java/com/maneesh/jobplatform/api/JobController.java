package com.maneesh.jobplatform.api;

import com.maneesh.jobplatform.jobs.Job;
import com.maneesh.jobplatform.jobs.JobService;
import com.maneesh.jobplatform.jobs.JobStatus;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/jobs")
public class JobController {
    private final JobService service;

    public JobController(JobService service) {
        this.service = service;
    }

    @PostMapping
    public ResponseEntity<Job> submit(@Valid @RequestBody CreateJobRequest request,
            @RequestHeader(value = "Idempotency-Key", required = false)
            @Pattern(regexp = "[A-Za-z0-9._:-]{1,128}") String key) {
        var submission = service.submit(request.type(), request.text(), key, request.priority(), request.scheduledAt());
        return ResponseEntity.status(submission.created() ? 202 : 200)
                .location(URI.create("/api/v1/jobs/" + submission.job().id())).body(submission.job());
    }

    @GetMapping("/{id}")
    public Job get(@PathVariable UUID id) {
        return service.get(id);
    }

    public record JobPage(List<Job> items, int page, int size, boolean hasNext) {}

    @GetMapping
    public JobPage list(@RequestParam(required = false) JobStatus status,
                        @RequestParam(defaultValue = "0") @Min(0) @Max(1000000) int page,
                        @RequestParam(defaultValue = "20") @Min(1) @Max(100) int size) {
        // The repository fetches one extra row to determine whether another page exists.
        List<Job> jobs = service.list(status, page, size);
        return new JobPage(jobs.subList(0, Math.min(size, jobs.size())), page, size, jobs.size() > size);
    }
}
