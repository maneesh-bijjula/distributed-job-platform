package com.maneesh.jobplatform.worker;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

class ExecutionControlTest {
    private ExecutionControl control(long lease, long deadline) {
        return new ExecutionControl(new ExecutionRepository.Window(lease > 0, deadline > 0, lease, deadline));
    }

    @Test
    void explicitStopAndExpiredWindowPreventFurtherWork() {
        var active = control(1000, 1000);
        assertThatCode(active::checkpoint).doesNotThrowAnyException();
        active.stop();
        assertThatThrownBy(active::checkpoint).isInstanceOf(ExecutionControl.Stopped.class);
        assertThatThrownBy(control(0, 1000)::checkpoint).isInstanceOf(ExecutionControl.Stopped.class);
        assertThatThrownBy(control(1000, 0)::checkpoint).isInstanceOf(ExecutionControl.Stopped.class);
    }

    @Test
    void cooperativePauseStopsPromptlyWhenCancellationArrives() throws Exception {
        var active = control(10000, 10000);
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var future = pool.submit(() -> assertThatThrownBy(() -> active.pause(Duration.ofSeconds(5)))
                    .isInstanceOf(ExecutionControl.Stopped.class));
            active.stop();
            future.get(1, TimeUnit.SECONDS);
        }
    }

    @Test
    void leaseRenewalDoesNotExtendTheExecutionDeadline() {
        var active = control(500, 100);
        active.renew(new ExecutionRepository.Window(true, true, 5000, 5000));
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                assertThatThrownBy(active::checkpoint).isInstanceOf(ExecutionControl.Stopped.class));
    }

    @Test
    void interruptedWorkerStopsAndKeepsItsInterruptFlag() {
        var active = control(1000, 1000);
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(active::checkpoint).isInstanceOf(ExecutionControl.Stopped.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally { Thread.interrupted(); }
    }
}
