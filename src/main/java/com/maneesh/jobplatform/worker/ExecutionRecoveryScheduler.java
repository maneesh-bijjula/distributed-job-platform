package com.maneesh.jobplatform.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import com.maneesh.jobplatform.observability.OperationMetrics;

@Component
@ConditionalOnProperty(name = "jobs.dispatcher.enabled", havingValue = "true", matchIfMissing = true)
public class ExecutionRecoveryScheduler {
    private static final Logger log = LoggerFactory.getLogger(ExecutionRecoveryScheduler.class);
    private final ExecutionService execution;
    private final OperationMetrics metrics;
    public ExecutionRecoveryScheduler(ExecutionService execution,OperationMetrics metrics) { this.execution = execution; this.metrics=metrics; }

    @Scheduled(fixedDelayString = "${jobs.execution.recovery-delay-ms:1000}")
    public void recover() {
        try { execution.recoverBatch(); }
        catch (RuntimeException exception) {
            metrics.failed(OperationMetrics.Operation.EXECUTION_RECOVERY);
            log.atWarn().addKeyValue("event","execution_recovery_failed").addKeyValue("error_kind","STORAGE_UNAVAILABLE")
                    .log("Execution recovery will retry on its next pass");
        }
    }
}
