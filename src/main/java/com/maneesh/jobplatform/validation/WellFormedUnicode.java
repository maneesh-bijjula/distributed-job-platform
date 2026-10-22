package com.maneesh.jobplatform.validation;

import jakarta.validation.Constraint;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import jakarta.validation.Payload;
import java.lang.annotation.*;

/** Java strings may contain isolated UTF-16 surrogates that cannot be stored as UTF-8. */
@Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.RECORD_COMPONENT})
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = WellFormedUnicode.Validator.class)
public @interface WellFormedUnicode {
    String message() default "must contain well-formed Unicode";
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};

    final class Validator implements ConstraintValidator<WellFormedUnicode, String> {
        @Override public boolean isValid(String value, ConstraintValidatorContext context) { return valid(value); }

        public static boolean valid(String value) {
            if (value == null) return true;
            for (int i = 0; i < value.length(); i++) {
                char current = value.charAt(i);
                if (Character.isHighSurrogate(current)) {
                    if (++i >= value.length() || !Character.isLowSurrogate(value.charAt(i))) return false;
                } else if (Character.isLowSurrogate(current)) return false;
            }
            return true;
        }
    }
}
