package com.maneesh.jobplatform.config;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("jobs.execution")
public record ExecutionProperties(
        @DefaultValue("15") @Min(3) @Max(300) int leaseSeconds,
        @DefaultValue("2000") @Min(100) @Max(10000) long heartbeatMillis,
        @DefaultValue("30") @Min(1) @Max(3600) int defaultTimeoutSeconds,
        @DefaultValue("3") @Min(0) @Max(20) int maxRecoveries,
        @DefaultValue("4") @Min(1) @Max(32) int monitorThreads) {
    @AssertTrue(message = "heartbeatMillis must be at most one third of leaseSeconds")
    public boolean isHeartbeatSupported() {
        return heartbeatMillis * 3 <= leaseSeconds * 1000L;
    }
}
