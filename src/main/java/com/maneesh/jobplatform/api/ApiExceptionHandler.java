package com.maneesh.jobplatform.api;

import java.net.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.amqp.AmqpException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

@RestControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(DataAccessException.class)
    public ProblemDetail unavailable(DataAccessException exception) {
        log.error("Database operation failed", exception);
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE,
                "Job storage is temporarily unavailable");
        problem.setType(URI.create("about:blank"));
        return problem;
    }

    @ExceptionHandler(AmqpException.class)
    public ProblemDetail brokerUnavailable(AmqpException exception) {
        log.warn("Dead-letter broker operation failed: {}", exception.getClass().getSimpleName());
        return ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, "Message broker is temporarily unavailable");
    }
}
