package com.maneesh.jobplatform.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "jobs.dispatcher.enabled", havingValue = "true", matchIfMissing = true)
public class ExecutionRecoveryScheduler {
    private static final Logger log = LoggerFactory.getLogger(ExecutionRecoveryScheduler.class);
    private final ExecutionService execution;
    public ExecutionRecoveryScheduler(ExecutionService execution) { this.execution = execution; }

    @Scheduled(fixedDelayString = "${jobs.execution.recovery-delay-ms:1000}")
    public void recover() {
        try { execution.recoverBatch(); }
        catch (RuntimeException exception) { log.warn("Execution recovery will retry on its next pass", exception); }
    }
}
