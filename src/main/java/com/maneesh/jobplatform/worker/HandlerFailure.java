package com.maneesh.jobplatform.worker;

import org.springframework.dao.TransientDataAccessException;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.AmqpTimeoutException;

/** Only known transient dependency failures opt into automatic handler retries. */
public enum HandlerFailure {
    RETRYABLE_INFRASTRUCTURE("Temporary handler dependency failure"),
    PERMANENT_HANDLER("Job handler failed");

    private final String message;
    HandlerFailure(String message) { this.message = message; }
    public String message() { return message; }

    public static HandlerFailure classify(RuntimeException error) {
        return error instanceof RetryableJobException || error instanceof TransientDataAccessException
                || error instanceof AmqpConnectException || error instanceof AmqpIOException
                || error instanceof AmqpTimeoutException ? RETRYABLE_INFRASTRUCTURE : PERMANENT_HANDLER;
    }
}
