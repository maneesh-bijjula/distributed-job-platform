package com.maneesh.jobplatform.config;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("jobs.dispatcher")
public record DispatchProperties(
        @DefaultValue("20") @Min(1) @Max(100) int batchSize,
        @DefaultValue("1000") @Min(50) @Max(60000) long delayMs) {}
