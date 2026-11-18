package io.github.sajuu730.connect.common;

import org.junit.jupiter.api.Test;
import java.time.Duration;
import static org.junit.jupiter.api.Assertions.*;

class RetryPolicyTest {
    @Test void exponentialDelayIsCapped() {
        var policy = new RetryPolicy(5, Duration.ofMillis(100), Duration.ofMillis(500), 0);
        assertEquals(Duration.ofMillis(100), policy.delayBeforeAttempt(1));
        assertEquals(Duration.ofMillis(200), policy.delayBeforeAttempt(2));
        assertEquals(Duration.ofMillis(400), policy.delayBeforeAttempt(3));
        assertEquals(Duration.ofMillis(500), policy.delayBeforeAttempt(4));
    }
    @Test void rejectsInvalidConfiguration() {
        assertThrows(IllegalArgumentException.class,
            () -> new RetryPolicy(0, Duration.ZERO, Duration.ZERO, 0));
    }
}