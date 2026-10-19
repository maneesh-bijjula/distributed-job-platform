package com.maneesh.jobplatform.messaging;

import java.util.UUID;

public interface JobPublisher {
    void publish(UUID jobId, int priority, int generation);

    default void publish(UUID jobId, int priority) { publish(jobId, priority, 0); }
}
