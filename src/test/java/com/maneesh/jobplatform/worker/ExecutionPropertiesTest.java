package com.maneesh.jobplatform.worker;

import com.maneesh.jobplatform.config.ExecutionProperties;
import jakarta.validation.Validation;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ExecutionPropertiesTest {
    @Test
    void supportedDefaultsAndFastTestSettingsValidate() {
        try (var factory = Validation.buildDefaultValidatorFactory()) {
            var validator = factory.getValidator();
            assertThat(validator.validate(new ExecutionProperties(15, 2000, 30, 3, 4))).isEmpty();
            assertThat(validator.validate(new ExecutionProperties(3, 100, 1, 0, 1))).isEmpty();
        }
    }

    @Test
    void heartbeatMustLeaveEnoughRoomBeforeLeaseExpiry() {
        try (var factory = Validation.buildDefaultValidatorFactory()) {
            assertThat(factory.getValidator().validate(new ExecutionProperties(3, 2000, 30, 3, 4)))
                    .anyMatch(violation -> violation.getMessage().contains("one third"));
        }
    }

    @Test
    void unboundedExecutionAndRecoverySettingsAreRejected() {
        try (var factory = Validation.buildDefaultValidatorFactory()) {
            assertThat(factory.getValidator().validate(new ExecutionProperties(0, 0, 3601, -1, 33))).hasSizeGreaterThanOrEqualTo(5);
        }
    }
}
