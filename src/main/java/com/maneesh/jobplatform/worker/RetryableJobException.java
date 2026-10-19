package com.maneesh.jobplatform.worker;

/** Custom handlers may use this for a dependency failure that is safe to retry. */
public final class RetryableJobException extends RuntimeException {
    public RetryableJobException(String message) { super(message); }
}
