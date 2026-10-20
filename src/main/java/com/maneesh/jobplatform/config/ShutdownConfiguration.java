package com.maneesh.jobplatform.config;

import org.springframework.amqp.rabbit.config.ContainerCustomizer;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ShutdownConfiguration {
    @Bean
    public ContainerCustomizer<SimpleMessageListenerContainer> listenerShutdown(
            @Value("${jobs.worker.shutdown-millis:35000}") long timeoutMillis) {
        if (timeoutMillis < 1000 || timeoutMillis > 120000) {
            throw new IllegalArgumentException("Listener shutdown must be between 1000 and 120000 ms");
        }
        return container -> {
            container.setShutdownTimeout(timeoutMillis);
            // Finish current work, then return prefetched work instead of draining it on shutdown.
            container.setForceStop(true);
        };
    }
}
