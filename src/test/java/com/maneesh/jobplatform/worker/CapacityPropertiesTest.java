package com.maneesh.jobplatform.worker;

import com.maneesh.jobplatform.config.CapacityProperties;
import com.maneesh.jobplatform.config.DispatchProperties;
import jakarta.validation.Validation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import static org.assertj.core.api.Assertions.assertThat;

class CapacityPropertiesTest {
    @ParameterizedTest
    @CsvSource({"1,2,10", "51,2,10", "10,0,10", "10,33,10", "10,2,0", "10,2,251"})
    void rejectsUnboundedOrUnusableCapacity(int pool,int concurrency,int prefetch) {
        try(var factory=Validation.buildDefaultValidatorFactory()) {
            assertThat(factory.getValidator().validate(new CapacityProperties(pool,concurrency,prefetch))).isNotEmpty();
        }
    }
    @ParameterizedTest
    @CsvSource({"0,1000", "101,1000", "20,49", "20,60001"})
    void rejectsUnsafeRelayBatchesAndPolling(int batch,long delay) {
        try(var factory=Validation.buildDefaultValidatorFactory()) {
            assertThat(factory.getValidator().validate(new DispatchProperties(batch,delay))).isNotEmpty();
        }
    }
    @Test void acceptsMeasuredAndBoundarySettings() {
        try(var factory=Validation.buildDefaultValidatorFactory()) {
            var validator=factory.getValidator();
            assertThat(validator.validate(new CapacityProperties(6,2,1))).isEmpty();
            assertThat(validator.validate(new CapacityProperties(50,32,250))).isEmpty();
            assertThat(validator.validate(new DispatchProperties(50,250))).isEmpty();
            assertThat(validator.validate(new DispatchProperties(100,50))).isEmpty();
        }
    }
}
