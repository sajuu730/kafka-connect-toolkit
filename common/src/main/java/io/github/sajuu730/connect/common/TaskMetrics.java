package io.github.sajuu730.connect.common;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Lightweight per-task JMX counters; metrics registration failure never prevents connector startup. */
public final class TaskMetrics implements TaskMetricsMBean, AutoCloseable {
    private static final Logger LOG = Logger.getLogger(TaskMetrics.class.getName());
    private final AtomicLong recordsProcessed = new AtomicLong();
    private final AtomicLong batchesCompleted = new AtomicLong();
    private final AtomicLong requests = new AtomicLong();
    private final AtomicLong retries = new AtomicLong();
    private final AtomicLong failures = new AtomicLong();
    private final AtomicLong dlqRecordsRouted = new AtomicLong();
    private final AtomicLong lastSuccessTimeEpochMs = new AtomicLong();
    private final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    private ObjectName objectName;

    public TaskMetrics(String connectorName) {
        try {
            objectName = new ObjectName("io.github.sajuu730.connect:type=ConnectorTask,connector="
                + ObjectName.quote(connectorName) + ",instance=" + ObjectName.quote(UUID.randomUUID().toString()));
            server.registerMBean(this, objectName);
        } catch (Exception ex) {
            objectName = null;
            LOG.log(Level.WARNING, "Unable to register connector task JMX metrics", ex);
        }
    }

    public void request() { requests.incrementAndGet(); }
    public void retry() { retries.incrementAndGet(); }
    public void failure() { failures.incrementAndGet(); }
    public void dlqRecordRouted() { dlqRecordsRouted.incrementAndGet(); }
    public void processed(long count) {
        recordsProcessed.addAndGet(count);
        lastSuccessTimeEpochMs.set(System.currentTimeMillis());
    }
    public void batchCompleted() {
        batchesCompleted.incrementAndGet();
        lastSuccessTimeEpochMs.set(System.currentTimeMillis());
    }

    @Override public long getRecordsProcessed() { return recordsProcessed.get(); }
    @Override public long getBatchesCompleted() { return batchesCompleted.get(); }
    @Override public long getRequests() { return requests.get(); }
    @Override public long getRetries() { return retries.get(); }
    @Override public long getFailures() { return failures.get(); }
    @Override public long getDlqRecordsRouted() { return dlqRecordsRouted.get(); }
    @Override public long getLastSuccessTimeEpochMs() { return lastSuccessTimeEpochMs.get(); }

    @Override public void close() {
        if (objectName != null) {
            try { server.unregisterMBean(objectName); }
            catch (Exception ex) { LOG.log(Level.FINE, "Unable to unregister connector task JMX metrics", ex); }
            objectName = null;
        }
    }
}