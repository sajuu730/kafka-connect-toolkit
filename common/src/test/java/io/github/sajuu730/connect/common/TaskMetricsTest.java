package io.github.sajuu730.connect.common;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class TaskMetricsTest {
    @Test void tracksConnectorActivityAndClosesCleanly() {
        TaskMetrics metrics = new TaskMetrics("unit-test");
        metrics.request();
        metrics.retry();
        metrics.failure();
        metrics.processed(3);
        metrics.batchCompleted();
        assertEquals(3, metrics.getRecordsProcessed());
        assertEquals(1, metrics.getBatchesCompleted());
        assertEquals(1, metrics.getRequests());
        assertEquals(1, metrics.getRetries());
        assertEquals(1, metrics.getFailures());
        assertTrue(metrics.getLastSuccessTimeEpochMs() > 0);
        assertDoesNotThrow(metrics::close);
    }
}