package com.maneesh.jobplatform.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import com.maneesh.jobplatform.observability.OperationMetrics;

@Component
@ConditionalOnProperty(name = "jobs.dispatcher.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxScheduler {
    private static final Logger log = LoggerFactory.getLogger(OutboxScheduler.class);
    private final OutboxRelay relay;
    private final OperationMetrics metrics;

    public OutboxScheduler(OutboxRelay relay, OperationMetrics metrics) {
        this.relay = relay;
        this.metrics = metrics;
    }

    @Scheduled(fixedDelayString = "${jobs.dispatcher.delay-ms:1000}")
    public void dispatch() {
        try {
            relay.dispatchBatch();
        } catch (RuntimeException exception) {
            metrics.failed(OperationMetrics.Operation.OUTBOX_DISPATCH);
            log.atWarn().addKeyValue("event","outbox_dispatch_failed").addKeyValue("error_kind","DISPATCH_UNAVAILABLE")
                    .log("Outbox dispatch failed; committed jobs remain available for the next attempt");
        }
    }
}
