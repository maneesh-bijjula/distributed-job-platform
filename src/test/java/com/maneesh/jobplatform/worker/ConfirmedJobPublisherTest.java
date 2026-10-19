package com.maneesh.jobplatform.worker;

import com.maneesh.jobplatform.messaging.ConfirmedJobPublisher;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class ConfirmedJobPublisherTest {
    private final RabbitTemplate rabbit = mock(RabbitTemplate.class);
    private final ConfirmedJobPublisher publisher = new ConfirmedJobPublisher(rabbit);

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
