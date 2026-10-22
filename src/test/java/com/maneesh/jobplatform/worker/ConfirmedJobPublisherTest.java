package com.maneesh.jobplatform.worker;

import com.maneesh.jobplatform.messaging.ConfirmedJobPublisher;
import java.util.UUID;
import java.util.List;
import java.util.ArrayList;
import com.maneesh.jobplatform.messaging.JobPublisher.Delivery;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.core.RabbitOperations;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class ConfirmedJobPublisherTest {
    private final RabbitTemplate rabbit = mock(RabbitTemplate.class,call->{
        if(call.getMethod().getName().equals("invoke"))
            return ((RabbitOperations.OperationsCallback<?>)call.getArgument(0)).doInRabbit((RabbitOperations)call.getMock());
        return org.mockito.Answers.RETURNS_DEFAULTS.answer(call);
    });
    private final ConfirmedJobPublisher publisher = new ConfirmedJobPublisher(rabbit);

    @Test
    void batchPipelinesAllSendsBeforeWaitingForTheirConfirmations() {
        var correlations=new ArrayList<CorrelationData>();
        doAnswer(call->{
            correlations.add(call.getArgument(4));
            if(correlations.size()==3)
                correlations.forEach(c->c.getFuture().complete(new CorrelationData.Confirm(true,null)));
            return null;
        }).when(rabbit).convertAndSend(anyString(),anyString(),any(),any(MessagePostProcessor.class),any(CorrelationData.class));
        publisher.publishBatch(List.of(new Delivery(UUID.randomUUID(),9,1),new Delivery(UUID.randomUUID(),4,2),new Delivery(UUID.randomUUID(),0,3)));
        assertThat(correlations).hasSize(3);
    }

    @Test
    void aSingleNackOrReturnRejectsTheCompleteBatch() {
        for(boolean returned:new boolean[]{false,true}) {
            reset(rabbit);
            var correlations=new ArrayList<CorrelationData>();
            doAnswer(call->{
                CorrelationData correlation=call.getArgument(4);
                correlations.add(correlation);
                if(correlations.size()==2) {
                    if(returned) correlation.setReturned(new ReturnedMessage(new Message(new byte[0]),312,"NO_ROUTE","jobs.exchange","jobs.execute"));
                    correlation.getFuture().complete(new CorrelationData.Confirm(returned,null));
                } else correlation.getFuture().complete(new CorrelationData.Confirm(true,null));
                return null;
            }).when(rabbit).convertAndSend(anyString(),anyString(),any(),any(MessagePostProcessor.class),any(CorrelationData.class));
            assertThatThrownBy(()->publisher.publishBatch(List.of(new Delivery(UUID.randomUUID(),1,0),new Delivery(UUID.randomUUID(),1,0))))
                    .isInstanceOf(AmqpException.class);
            assertThat(correlations).hasSize(2);
        }
    }

    @Test
    void exceptionalConfirmationRejectsBatch() {
        doAnswer(call->{
            ((CorrelationData)call.getArgument(4)).getFuture().completeExceptionally(new IllegalStateException("connection lost"));
            return null;
        }).when(rabbit).convertAndSend(anyString(),anyString(),any(),any(MessagePostProcessor.class),any(CorrelationData.class));
        assertThatThrownBy(()->publisher.publishBatch(List.of(new Delivery(UUID.randomUUID(),0,0)))).isInstanceOf(AmqpException.class);
    }

    @Test
    void interruptedWaitRestoresThreadInterruptStatus() {
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(()->publisher.publishBatch(List.of(new Delivery(UUID.randomUUID(),0,0)))).isInstanceOf(AmqpException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally { Thread.interrupted(); }
    }

    @Test
    void missingConfirmationsFailWithinOneBatchBudget() {
        long start=System.nanoTime();
        var deliveries=java.util.stream.IntStream.range(0,10).mapToObj(i->new Delivery(UUID.randomUUID(),0,0)).toList();
        assertThatThrownBy(()->publisher.publishBatch(deliveries)).isInstanceOf(AmqpException.class)
                .hasCauseInstanceOf(java.util.concurrent.TimeoutException.class);
        assertThat(java.time.Duration.ofNanos(System.nanoTime()-start)).isLessThan(java.time.Duration.ofSeconds(12));
    }

    @Test
    void emptyBatchDoesNoIoAndOversizedBatchIsRejectedBeforeSending() {
        publisher.publishBatch(List.of());
        assertThatThrownBy(()->publisher.publishBatch(java.util.stream.IntStream.range(0,101)
                .mapToObj(i->new Delivery(UUID.randomUUID(),0,0)).toList())).isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(rabbit);
    }

    private void brokerResponse(boolean ack, boolean returned) {
        doAnswer(call -> {
            CorrelationData correlation = call.getArgument(4);
            if (returned) {
                correlation.setReturned(new ReturnedMessage(new Message(new byte[0]),
                        312, "NO_ROUTE", "jobs.exchange", "jobs.execute"));
            }
            correlation.getFuture().complete(new CorrelationData.Confirm(ack, null));
            return null;
        }).when(rabbit).convertAndSend(anyString(), anyString(), any(),
                any(MessagePostProcessor.class), any(CorrelationData.class));
    }

    @Test
    void routedAndConfirmedPublishSucceeds() {
        brokerResponse(true, false);
        assertThatCode(() -> publisher.publish(UUID.randomUUID(), 0)).doesNotThrowAnyException();
    }

    @Test
    void negativeConfirmationDoesNotCountAsDelivery() {
        brokerResponse(false, false);
        assertThatThrownBy(() -> publisher.publish(UUID.randomUUID(), 0)).isInstanceOf(AmqpException.class);
    }

    @Test
    void confirmedButUnroutablePublishDoesNotCountAsDelivery() {
        brokerResponse(true, true);
        assertThatThrownBy(() -> publisher.publish(UUID.randomUUID(), 0)).isInstanceOf(AmqpException.class);
    }

    @Test
    void publishedMessageCarriesPersistentIdentityPriorityAndGeneration() {
        UUID id = UUID.randomUUID();
        doAnswer(call -> {
            MessagePostProcessor postProcessor = call.getArgument(3);
            var message = postProcessor.postProcessMessage(new Message(new byte[0]));
            assertThat(message.getMessageProperties().getMessageId()).isEqualTo(id.toString());
            assertThat(message.getMessageProperties().getPriority()).isEqualTo(7);
            assertThat((Integer) message.getMessageProperties().getHeader("job-generation")).isEqualTo(2);
            assertThat(message.getMessageProperties().getDeliveryMode())
                    .isEqualTo(org.springframework.amqp.core.MessageDeliveryMode.PERSISTENT);
            ((CorrelationData) call.getArgument(4)).getFuture().complete(new CorrelationData.Confirm(true, null));
            return null;
        }).when(rabbit).convertAndSend(anyString(), anyString(), any(), any(MessagePostProcessor.class), any(CorrelationData.class));
        publisher.publish(id, 7, 2);
    }
}
