package com.maneesh.jobplatform.messaging;

import java.util.UUID;
import java.util.List;

public interface JobPublisher {
    void publish(UUID jobId, int priority, int generation);

    record Delivery(UUID id,int priority,int generation) {}

    default void publishBatch(List<Delivery> deliveries) {
        for(var delivery:deliveries) publish(delivery.id(),delivery.priority(),delivery.generation());
    }

    default void publish(UUID jobId, int priority) { publish(jobId, priority, 0); }
}
