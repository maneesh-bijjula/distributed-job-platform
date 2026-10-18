package com.maneesh.jobplatform.api;

import com.maneesh.jobplatform.jobs.JobType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import jakarta.validation.constraints.Pattern;

public record CreateJobRequest(@NotNull JobType type,
                               @NotBlank @Size(max = 16000)
                               @Pattern(regexp = "[^\\x00]*", message = "must not contain null characters") String text) {}
