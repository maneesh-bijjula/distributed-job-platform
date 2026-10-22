package com.maneesh.jobplatform.messaging;

import com.maneesh.jobplatform.config.RabbitConfiguration;
import java.util.UUID;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.core.RabbitOperations;
import org.springframework.stereotype.Component;

@Component
public class ConfirmedJobPublisher implements JobPublisher {
    private final RabbitTemplate rabbit;

    public ConfirmedJobPublisher(RabbitTemplate rabbit) {
        this.rabbit = rabbit;
    }

    @Override
    public void publish(UUID jobId, int priority, int generation) {
        publishBatch(List.of(new Delivery(jobId,priority,generation)));
    }

    @Override
    public void publishBatch(List<Delivery> deliveries) {
        if(deliveries.size()>100) throw new IllegalArgumentException("Publisher batch must contain at most 100 deliveries");
        if(deliveries.isEmpty()) return;
        rabbit.invoke(operations->{ publishOnChannel(operations,deliveries); return null; });
    }

    private void publishOnChannel(RabbitOperations operations,List<Delivery> deliveries) {
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
        var pending=new ArrayList<CorrelationData>(deliveries.size());
        // All messages enter the bounded confirmation window before waiting for any ack.
        for(var delivery:deliveries) {
            if(System.nanoTime()-deadline>=0) throw new AmqpException("Publisher batch confirmation deadline exceeded");
            pending.add(send(operations,delivery));
        }
        try {
            for(var correlation:pending) {
                long remaining=deadline-System.nanoTime();
                if(remaining<=0) throw new java.util.concurrent.TimeoutException();
                var confirm=correlation.getFuture().get(remaining,TimeUnit.NANOSECONDS);
                if(!confirm.isAck() || correlation.getReturned()!=null)
                    throw new AmqpException("Broker did not accept and route the complete batch");
            }
        } catch(InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new AmqpException("Interrupted waiting for publisher confirmation",exception);
        } catch(java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException exception) {
            throw new AmqpException("Publisher batch confirmation failed",exception);
        }
    }

    private CorrelationData send(RabbitOperations operations,Delivery delivery) {
        CorrelationData correlation = new CorrelationData(UUID.randomUUID().toString());
        operations.convertAndSend(RabbitConfiguration.EXCHANGE, RabbitConfiguration.ROUTING_KEY,
                delivery.id().toString(), message -> {
                    message.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                    message.getMessageProperties().setMessageId(delivery.id().toString());
                    message.getMessageProperties().setPriority(delivery.priority());
                    message.getMessageProperties().setHeader("job-generation", delivery.generation());
                    return message;
                }, correlation);
        return correlation;
    }
}
