package com.maneesh.jobplatform.worker;

import com.maneesh.jobplatform.config.ShutdownConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class ShutdownConfigurationTest {
    @Test void allowsCurrentWorkToFinishAndReturnsPrefetchedMessages() {
        var container = mock(SimpleMessageListenerContainer.class);
        new ShutdownConfiguration().listenerShutdown(35000).configure(container);
        verify(container).setShutdownTimeout(35000);
        verify(container).setForceStop(true);
    }

    @ParameterizedTest
    @ValueSource(longs = {-1, 0, 999, 120001})
    void rejectsUnsafeShutdownBudgets(long timeout) {
        assertThatThrownBy(() -> new ShutdownConfiguration().listenerShutdown(timeout))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
