package com.maneesh.jobplatform.config;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("jobs.capacity")
public record CapacityProperties(
        @DefaultValue("10") @Min(2) @Max(50) int databasePoolSize,
        @DefaultValue("2") @Min(1) @Max(32) int workerConcurrency,
        @DefaultValue("10") @Min(1) @Max(250) int prefetch) {}
