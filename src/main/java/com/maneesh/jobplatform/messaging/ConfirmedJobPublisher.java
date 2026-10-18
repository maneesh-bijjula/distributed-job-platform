package com.maneesh.jobplatform.messaging;

import com.maneesh.jobplatform.config.RabbitConfiguration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

@Component
public class ConfirmedJobPublisher implements JobPublisher {
    private final RabbitTemplate rabbit;

    public ConfirmedJobPublisher(RabbitTemplate rabbit) {
        this.rabbit = rabbit;
    }

    @Override
    public void publish(UUID jobId, int priority) {
        CorrelationData correlation = new CorrelationData(UUID.randomUUID().toString());
        rabbit.convertAndSend(RabbitConfiguration.EXCHANGE, RabbitConfiguration.ROUTING_KEY,
                jobId.toString(), message -> {
                    message.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                    message.getMessageProperties().setMessageId(jobId.toString());
                    message.getMessageProperties().setPriority(priority);
                    return message;
                }, correlation);
        try {
            var confirm = correlation.getFuture().get(5, TimeUnit.SECONDS);
            if (!confirm.isAck() || correlation.getReturned() != null) {
                throw new AmqpException("Broker did not accept and route job " + jobId);
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new AmqpException("Interrupted waiting for publisher confirmation", exception);
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException exception) {
            throw new AmqpException("Publisher confirmation failed for job " + jobId, exception);
        }
    }
}
