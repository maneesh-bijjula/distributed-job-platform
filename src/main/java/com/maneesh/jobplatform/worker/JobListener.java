package com.maneesh.jobplatform.worker;

import com.maneesh.jobplatform.config.RabbitConfiguration;
import java.util.UUID;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.messaging.handler.annotation.Header;

@Component
@ConditionalOnProperty(name = "jobs.worker.enabled", havingValue = "true", matchIfMissing = true)
public class JobListener {
    private final JobExecutor executor;

    public JobListener(JobExecutor executor) {
        this.executor = executor;
    }

    public void receive(String message) {
        receive(message, null);
    }

    @RabbitListener(queues = {RabbitConfiguration.QUEUE, RabbitConfiguration.LEGACY_QUEUE})
    public void receive(String message, @Header(name = "job-generation", required = false) Integer generation) {
        UUID id;
        try {
            id = UUID.fromString(message);
        } catch (IllegalArgumentException exception) {
            throw new AmqpRejectAndDontRequeueException("Invalid job identifier", exception);
        }
        if (generation != null && generation < 0) {
            throw new AmqpRejectAndDontRequeueException("Invalid delivery generation");
        }
        // UUID-only legacy deliveries belong to generation zero. They cannot
        // claim a recovered execution from a newer generation.
        executor.execute(id, generation == null ? 0 : generation);
    }
}
