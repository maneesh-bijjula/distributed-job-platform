package com.maneesh.jobplatform.api;

import com.maneesh.jobplatform.jobs.JobType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.AssertTrue;
import com.fasterxml.jackson.annotation.JsonIgnore;
import java.time.Instant;
import jakarta.validation.Valid;
import com.maneesh.jobplatform.jobs.RetryPolicy;
import com.maneesh.jobplatform.validation.WellFormedUnicode;

public record CreateJobRequest(@NotNull JobType type,
                               @NotBlank @Size(max = 16000) @WellFormedUnicode
                               @Pattern(regexp = "[^\\x00]*", message = "must not contain null characters") String text,
                               @Min(0) @Max(9) int priority,
                               Instant scheduledAt,
                               @Min(1) @Max(3600) Integer timeoutSeconds,
                               @Valid RetryPolicy retryPolicy) {
    private static final Instant MIN_SCHEDULE = Instant.parse("0001-01-01T00:00:00Z");
    private static final Instant MAX_SCHEDULE = Instant.parse("9999-12-31T23:59:59.999999Z");

    @JsonIgnore
    @AssertTrue(message = "scheduledAt must be between years 0001 and 9999")
    public boolean isScheduleSupported() {
        return scheduledAt == null || (!scheduledAt.isBefore(MIN_SCHEDULE) && !scheduledAt.isAfter(MAX_SCHEDULE));
    }
}
