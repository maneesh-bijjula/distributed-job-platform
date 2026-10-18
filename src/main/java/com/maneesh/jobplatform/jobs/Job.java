package com.maneesh.jobplatform.jobs;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;

public record Job(UUID id, JobType type, String text, JobStatus status, JsonNode result,
                  String error, Instant createdAt, Instant completedAt, int priority, Instant scheduledAt) {}
