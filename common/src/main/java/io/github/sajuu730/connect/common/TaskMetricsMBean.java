package io.github.sajuu730.connect.common;

/** JMX view of connector task activity. */
public interface TaskMetricsMBean {
    long getRecordsProcessed();
    long getBatchesCompleted();
    long getRequests();
    long getRetries();
    long getFailures();
    long getDlqRecordsRouted();
    long getLastSuccessTimeEpochMs();
}