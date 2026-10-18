package com.maneesh.jobplatform.config;

import org.springframework.amqp.core.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitConfiguration {
    public static final String EXCHANGE = "jobs.exchange";
    public static final String QUEUE = "jobs.priority.ready";
    public static final String ROUTING_KEY = "jobs.execute.priority";
    public static final String LEGACY_QUEUE = "jobs.ready";
    public static final String LEGACY_ROUTING_KEY = "jobs.execute";
    public static final String DEAD_EXCHANGE = "jobs.dead.exchange";
    public static final String DEAD_QUEUE = "jobs.dead";

    @Bean
    Declarables jobTopology() {
        DirectExchange exchange = new DirectExchange(EXCHANGE, true, false);
        DirectExchange deadExchange = new DirectExchange(DEAD_EXCHANGE, true, false);
        Queue ready = QueueBuilder.durable(QUEUE)
                .maxPriority(9)
                .deadLetterExchange(DEAD_EXCHANGE).deadLetterRoutingKey("dead").build();
        // Queue arguments are immutable. Keep the original queue consumable during upgrades.
        Queue legacy = QueueBuilder.durable(LEGACY_QUEUE)
                .deadLetterExchange(DEAD_EXCHANGE).deadLetterRoutingKey("dead").build();
        Queue dead = QueueBuilder.durable(DEAD_QUEUE).build();
        return new Declarables(exchange, deadExchange, ready, legacy, dead,
                BindingBuilder.bind(ready).to(exchange).with(ROUTING_KEY),
                BindingBuilder.bind(legacy).to(exchange).with(LEGACY_ROUTING_KEY),
                BindingBuilder.bind(dead).to(deadExchange).with("dead"));
    }
}
