package com.maneesh.jobplatform.jobs;

import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

@Service
public class JobService {
    private final JobRepository repository;

    public JobService(JobRepository repository) {
        this.repository = repository;
    }

    public record Submission(Job job, boolean created) {}

    @Transactional
    public Submission submit(JobType type, String text, String key) {
        UUID id = UUID.randomUUID();
        if (repository.insert(id, type, text, key).isPresent()) {
            repository.enqueue(id);
            return new Submission(get(id), true);
        }
        Job existing = repository.findByKey(key).orElseThrow();
        if (existing.type() != type || !existing.text().equals(text)) {
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "Idempotency-Key was already used with a different request");
        }
        return new Submission(existing, false);
    }

    public Job get(UUID id) {
        return repository.find(id).orElseThrow(() ->
                new ResponseStatusException(HttpStatus.NOT_FOUND, "Job not found"));
    }

    public List<Job> list(JobStatus status, int page, int size) {
        return repository.list(status, page, size);
    }
}
