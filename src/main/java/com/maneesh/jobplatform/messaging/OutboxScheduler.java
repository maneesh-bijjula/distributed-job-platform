package com.maneesh.jobplatform.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "jobs.dispatcher.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxScheduler {
    private static final Logger log = LoggerFactory.getLogger(OutboxScheduler.class);
    private final OutboxRelay relay;

    public OutboxScheduler(OutboxRelay relay) {
        this.relay = relay;
    }

    @Scheduled(fixedDelayString = "${jobs.dispatcher.delay-ms:1000}")
    public void dispatch() {
        try {
            relay.dispatchBatch();
        } catch (RuntimeException exception) {
            log.warn("Outbox dispatch failed; committed jobs remain available for the next attempt", exception);
        }
    }
}
