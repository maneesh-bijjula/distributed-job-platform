package com.maneesh.jobplatform.worker;

import com.maneesh.jobplatform.config.RabbitConfiguration;
import java.util.UUID;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "jobs.worker.enabled", havingValue = "true", matchIfMissing = true)
public class JobListener {
    private final JobExecutor executor;

    public JobListener(JobExecutor executor) {
        this.executor = executor;
    }

    @RabbitListener(queues = {RabbitConfiguration.QUEUE, RabbitConfiguration.LEGACY_QUEUE})
    public void receive(String message) {
        UUID id;
        try {
            id = UUID.fromString(message);
        } catch (IllegalArgumentException exception) {
            throw new AmqpRejectAndDontRequeueException("Invalid job identifier", exception);
        }
        // The separate service proxy commits before this listener returns and AUTO acknowledges.
        executor.execute(id);
    }
}
