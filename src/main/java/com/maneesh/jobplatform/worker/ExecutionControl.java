package com.maneesh.jobplatform.worker;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

/** Cooperative checkpoints; database fencing remains authoritative for results. */
public final class ExecutionControl {
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final long deadlineNanos;
    private volatile long leaseNanos;

    public ExecutionControl(ExecutionRepository.Window window) {
        long now = System.nanoTime();
        deadlineNanos = now + Duration.ofMillis(window.deadlineMillis()).toNanos();
        leaseNanos = now + Duration.ofMillis(window.leaseMillis()).toNanos();
    }

    public void renew(ExecutionRepository.Window window) {
        leaseNanos = System.nanoTime() + Duration.ofMillis(window.leaseMillis()).toNanos();
    }

    public void stop() { stopped.set(true); }

    public void checkpoint() {
        long now = System.nanoTime();
        if (stopped.get() || Thread.currentThread().isInterrupted()
                || now - deadlineNanos >= 0 || now - leaseNanos >= 0) throw new Stopped();
    }

    public void pause(Duration duration) {
        if (duration.isNegative()) throw new IllegalArgumentException("Pause duration cannot be negative");
        long until = System.nanoTime() + duration.toNanos();
        for (;;) {
            checkpoint();
            long remaining = until - System.nanoTime();
            if (remaining <= 0) return;
            try { Thread.sleep(Math.min(100, Math.max(1, Duration.ofNanos(remaining).toMillis()))); }
            catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new Stopped();
            }
        }
    }

    public static final class Stopped extends RuntimeException {
        public Stopped() { super("Execution stopped", null, false, false); }
    }
}
