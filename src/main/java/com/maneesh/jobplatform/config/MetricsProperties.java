package com.maneesh.jobplatform.config;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("jobs.metrics")
public record MetricsProperties(@DefaultValue("5000") @Min(1000) @Max(60000) long refreshMs) {}
