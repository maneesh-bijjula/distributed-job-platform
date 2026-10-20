package com.maneesh.jobplatform.worker;

import com.maneesh.jobplatform.config.MetricsProperties;
import jakarta.validation.Validation;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;

class MetricsPropertiesTest {
    @ParameterizedTest @ValueSource(longs={0,999,60001})
    void rejectsUnboundedSamplingIntervals(long interval) {
        try(var factory=Validation.buildDefaultValidatorFactory()) {
            assertThat(factory.getValidator().validate(new MetricsProperties(interval))).isNotEmpty();
        }
    }
    @ParameterizedTest @ValueSource(longs={1000,5000,60000})
    void acceptsBoundedSamplingIntervals(long interval) {
        try(var factory=Validation.buildDefaultValidatorFactory()) {
            assertThat(factory.getValidator().validate(new MetricsProperties(interval))).isEmpty();
        }
    }
}
