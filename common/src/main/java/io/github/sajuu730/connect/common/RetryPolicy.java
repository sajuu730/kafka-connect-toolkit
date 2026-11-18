package io.github.sajuu730.connect.common;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/** Immutable retry policy with capped exponential backoff and optional jitter. */
public record RetryPolicy(int maxAttempts, Duration initialDelay, Duration maxDelay, double jitter) {
    public RetryPolicy {
        if (maxAttempts < 1) throw new IllegalArgumentException("maxAttempts must be >= 1");
        Objects.requireNonNull(initialDelay, "initialDelay");
        Objects.requireNonNull(maxDelay, "maxDelay");
        if (initialDelay.isNegative() || maxDelay.isNegative() || maxDelay.compareTo(initialDelay) < 0)
            throw new IllegalArgumentException("delays must be non-negative and maxDelay >= initialDelay");
        if (jitter < 0.0 || jitter > 1.0) throw new IllegalArgumentException("jitter must be between 0 and 1");
    }

    public Duration delayBeforeAttempt(int failedAttempt) {
        if (failedAttempt < 1) throw new IllegalArgumentException("failedAttempt must be >= 1");
        long initial = initialDelay.toMillis();
        long cap = maxDelay.toMillis();
        long exponential;
        try { exponential = Math.multiplyExact(initial, 1L << Math.min(failedAttempt - 1, 30)); }
        catch (ArithmeticException ex) { exponential = Long.MAX_VALUE; }
        long bounded = Math.min(exponential, cap);
        if (bounded == 0 || jitter == 0) return Duration.ofMillis(bounded);
        double factor = ThreadLocalRandom.current().nextDouble(1.0 - jitter, 1.0 + jitter);
        return Duration.ofMillis(Math.max(0, Math.min(cap, (long) (bounded * factor))));
    }
}