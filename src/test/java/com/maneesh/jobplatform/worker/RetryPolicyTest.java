package com.maneesh.jobplatform.worker;

import com.maneesh.jobplatform.jobs.RetryPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.amqp.AmqpConnectException;
import java.net.ConnectException;
import static org.assertj.core.api.Assertions.*;

class RetryPolicyTest {
    @ParameterizedTest
    @CsvSource({"1,2", "2,4", "3,8", "4,10", "25,10", "2147483647,10"})
    void exponentialBackoffIsCappedWithoutOverflow(int attempt, int expected) {
        assertThat(new RetryPolicy(25, 2, 10).delaySeconds(attempt)).isEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource({"0,1,60", "26,1,60", "4,0,60", "4,2,1", "4,1,3601"})
    void rejectsUnboundedOrInconsistentPolicies(int max, int initial, int cap) {
        assertThat(new RetryPolicy(max, initial, cap).valid()).isFalse();
    }

    @Test
    void retriesOnlyExplicitOrKnownTransientDependencyFailures() {
        assertThat(HandlerFailure.classify(new RetryableJobException("private"))).isEqualTo(HandlerFailure.RETRYABLE_INFRASTRUCTURE);
        assertThat(HandlerFailure.classify(new TransientDataAccessResourceException("private"))).isEqualTo(HandlerFailure.RETRYABLE_INFRASTRUCTURE);
        assertThat(HandlerFailure.classify(new AmqpConnectException(new ConnectException("private")))).isEqualTo(HandlerFailure.RETRYABLE_INFRASTRUCTURE);
        assertThat(HandlerFailure.classify(new IllegalArgumentException("private"))).isEqualTo(HandlerFailure.PERMANENT_HANDLER);
        assertThat(HandlerFailure.classify(new IllegalStateException("private"))).isEqualTo(HandlerFailure.PERMANENT_HANDLER);
        assertThat(HandlerFailure.classify(new DataAccessResourceFailureException("unknown persistence error"))).isEqualTo(HandlerFailure.PERMANENT_HANDLER);
        assertThat(HandlerFailure.RETRYABLE_INFRASTRUCTURE.message()).doesNotContain("private");
    }
}
