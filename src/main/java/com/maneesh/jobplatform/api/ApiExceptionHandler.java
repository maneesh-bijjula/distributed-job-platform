package com.maneesh.jobplatform.api;

import java.net.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.amqp.AmqpException;
import com.maneesh.jobplatform.auth.AdmissionService;
import org.springframework.http.ResponseEntity;
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
        log.atError().addKeyValue("event","api_storage_failed").addKeyValue("error_kind","STORAGE_UNAVAILABLE").log("Database operation failed");
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE,
                "Job storage is temporarily unavailable");
        problem.setType(URI.create("about:blank"));
        return problem;
    }

    @ExceptionHandler(AmqpException.class)
    public ProblemDetail brokerUnavailable(AmqpException exception) {
        log.atWarn().addKeyValue("event","dead_letter_broker_failed").addKeyValue("error_kind","BROKER_UNAVAILABLE").log("Dead-letter broker operation failed");
        return ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, "Message broker is temporarily unavailable");
    }

    @ExceptionHandler(AdmissionService.Limited.class)
    public ResponseEntity<ProblemDetail> limited(AdmissionService.Limited exception) {
        return ResponseEntity.status(429).header("Retry-After",Integer.toString(exception.retryAfter()))
                .body(ProblemDetail.forStatusAndDetail(HttpStatus.TOO_MANY_REQUESTS,"Owner submission limit exceeded"));
    }
}
