package com.maneesh.jobplatform.deadletter;

import com.maneesh.jobplatform.config.RabbitConfiguration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
public class DeadLetterCollector {
    private final RabbitTemplate rabbit;
    private final DeadLetterService service;
    public DeadLetterCollector(RabbitTemplate rabbit, DeadLetterService service) { this.rabbit = rabbit; this.service = service; }
    public record Collection(int collected, List<UUID> entryIds) {}

    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public Collection collect(int limit) {
        if (limit < 1 || limit > 100) throw new IllegalArgumentException("Collection limit must be between 1 and 100");
        return rabbit.execute(channel -> {
            var ids = new ArrayList<UUID>();
            for (int i = 0; i < limit; i++) {
                var message = channel.basicGet(RabbitConfiguration.DEAD_QUEUE, false);
                if (message == null) break;
                long tag = message.getEnvelope().getDeliveryTag();
                try {
                    var headers = message.getProps().getHeaders();
                    UUID id = service.capture(message.getBody(), headers == null ? null : headers.get("job-generation"));
                    channel.basicAck(tag, false); // capture's DB transaction has already committed.
                    ids.add(id);
                } catch (RuntimeException error) {
                    channel.basicNack(tag, false, true);
                    throw error;
                }
            }
            return new Collection(ids.size(), List.copyOf(ids));
        });
    }
}
