package com.maneesh.jobplatform.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.maneesh.jobplatform.config.ExecutionProperties;
import java.util.UUID;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class JobExecutor {
    private static final Logger log = LoggerFactory.getLogger(JobExecutor.class);
    private final ExecutionService execution;
    private final JobProcessor processor;
    private final ScheduledThreadPoolExecutor monitor;
    private final ExecutionProperties properties;

    public JobExecutor(ExecutionService execution, JobProcessor processor, ScheduledThreadPoolExecutor monitor,
                       ExecutionProperties properties) {
        this.execution = execution;
        this.processor = processor;
        this.monitor = monitor;
        this.properties = properties;
    }

    public void execute(UUID id) { execute(id, 0); }

    public void execute(UUID id, int generation) {
        var claimed = execution.claim(id, generation);
        if (claimed.isEmpty()) return;
        var claim = claimed.get();
        var control = new ExecutionControl(claim.window());
        var heartbeat = monitor.scheduleWithFixedDelay(() -> {
            try {
                var window = execution.heartbeat(claim);
                if (window.isPresent()) control.renew(window.get());
                else control.stop();
            } catch (RuntimeException exception) {
                // Never extend the local lease on an unconfirmed renewal.
                log.warn("Cannot renew lease for job {} attempt {}", id, claim.attempt().id(), exception);
            }
        }, properties.heartbeatMillis(), properties.heartbeatMillis(), TimeUnit.MILLISECONDS);
        try {
            JsonNode result;
            try {
                result = processor.process(claim.job().type(), claim.job().text(), control);
            } catch (ExecutionControl.Stopped stopped) {
                execution.reconcile(claim);
                return;
            } catch (RuntimeException exception) {
                log.error("Handler failed for job {} attempt {}", id, claim.attempt().id(), exception);
                execution.fail(claim);
                return;
            }
            // Each lifecycle method commits in its own short transaction.
            // No connection or row lock is held while the handler is running.
            execution.succeed(claim, result);
        } finally {
            control.stop();
            heartbeat.cancel(false);
        }
    }
}
