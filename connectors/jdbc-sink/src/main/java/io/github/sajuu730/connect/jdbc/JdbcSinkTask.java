package io.github.sajuu730.connect.jdbc;

import io.github.sajuu730.connect.common.RetryPolicy;
import io.github.sajuu730.connect.common.TaskMetrics;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.connect.sink.SinkTask;
import java.sql.*;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class JdbcSinkTask extends SinkTask {
    private static final Logger LOG = Logger.getLogger(JdbcSinkTask.class.getName());
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)?");
    private Connection connection;
    private String url, user, password, table, insertMode;
    private int batchSize;
    private List<String> primaryKeyFields;
    private RetryPolicy retryPolicy;
    private TaskMetrics metrics;
    private List<String> columns = List.of();
    private ErrantRecordReporter errantRecordReporter;

    @Override public void start(Map<String, String> props) {
        url = required(props, JdbcSinkConnector.URL);
        user = props.getOrDefault(JdbcSinkConnector.USER, "");
        password = props.getOrDefault(JdbcSinkConnector.PASSWORD, "");
        table = required(props, JdbcSinkConnector.TABLE);
        if (!IDENTIFIER.matcher(table).matches()) throw new ConnectException("Unsafe table.name; use a simple SQL identifier or schema.table");
        batchSize = Integer.parseInt(props.getOrDefault(JdbcSinkConnector.BATCH_SIZE, "500"));
        insertMode = props.getOrDefault(JdbcSinkConnector.INSERT_MODE, "insert");
        primaryKeyFields = java.util.Arrays.stream(props.getOrDefault(JdbcSinkConnector.PK_FIELDS, "").split(","))
            .map(String::trim).filter(value -> !value.isEmpty()).distinct().toList();
        int attempts = Integer.parseInt(props.getOrDefault("retry.max.attempts", "3"));
        long initialBackoff = Long.parseLong(props.getOrDefault("retry.initial.backoff.ms", "500"));
        if (batchSize < 1 || attempts < 1 || initialBackoff < 0 || !List.of("insert", "upsert").contains(insertMode))
            throw new ConnectException("Invalid JDBC batch or retry configuration");
        if ("upsert".equals(insertMode) && primaryKeyFields.isEmpty())
            throw new ConnectException("pk.fields must be configured when insert.mode=upsert");
        retryPolicy = new RetryPolicy(attempts, Duration.ofMillis(initialBackoff), Duration.ofSeconds(30), 0.2);
        openConnection();
        errantRecordReporter = context == null ? null : context.errantRecordReporter();
        metrics = new TaskMetrics("jdbc-sink");
    }

    @Override public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) return;
        List<SinkRecord> validRecords = new ArrayList<>();
        for (SinkRecord record : records) {
            if (!(record.value() instanceof Struct struct)) {
                reportMalformed(record, new ConnectException("JDBC sink requires Struct record values"));
                continue;
            }
            List<String> rowColumns = struct.schema().fields().stream().map(Field::name).toList();
            if (rowColumns.isEmpty()) {
                reportMalformed(record, new ConnectException("JDBC sink cannot insert a Struct with no fields"));
                continue;
            }
            if (columns.isEmpty()) columns = rowColumns;
            if (!columns.equals(rowColumns)) {
                reportMalformed(record, new ConnectException(
                    "Record schema changed within task; schema evolution is not enabled for this task"));
                continue;
            }
            validRecords.add(record);
        }
        if (validRecords.isEmpty()) return;
        for (int attempt = 1; attempt <= retryPolicy.maxAttempts(); attempt++) {
            try {
                executeBatch(validRecords);
            } catch (SQLException ex) {
                rollbackQuietly(ex);
                if (!isRetryable(ex) || attempt == retryPolicy.maxAttempts()) {
                    metrics.failure();
                    throw new ConnectException("JDBC batch execution failed after " + attempt + " attempt(s)", ex);
                }
                metrics.retry();
                LOG.log(Level.WARNING, "Transient JDBC failure; retrying attempt {0}/{1}, SQLState={2}",
                    new Object[]{attempt, retryPolicy.maxAttempts(), ex.getSQLState()});
                reconnect();
                sleepBeforeRetry(attempt);
                continue;
            }
            try {
                connection.commit();
                metrics.batchCompleted();
                metrics.processed(validRecords.size());
                return;
            } catch (SQLException ex) {
                rollbackQuietly(ex);
                metrics.failure();
                throw new ConnectException("JDBC commit failed; outcome may be ambiguous", ex);
            }
        }
    }

    /**
     * Sends malformed individual records to the worker-configured DLQ reporter.
     * Waiting for the reporter future ensures the DLQ write completes before
     * this put call returns and Kafka Connect can commit the corresponding offsets.
     */
    private void reportMalformed(SinkRecord record, ConnectException failure) {
        if (errantRecordReporter == null) {
            throw new ConnectException("Malformed record cannot be skipped because ErrantRecordReporter is unavailable", failure);
        }
        try {
            errantRecordReporter.report(record, failure).get();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new ConnectException("Interrupted while reporting malformed record to the DLQ", interrupted);
        } catch (ExecutionException reportingFailure) {
            throw new ConnectException("Failed to report malformed record to the DLQ", reportingFailure.getCause());
        }
    }

    private void executeBatch(Collection<SinkRecord> records) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(buildInsertSql())) {
            int pending = 0;
            for (SinkRecord record : records) {
                Struct struct = (Struct) record.value();
                for (int i = 0; i < columns.size(); i++) statement.setObject(i + 1, struct.get(columns.get(i)));
                statement.addBatch();
                if (++pending % batchSize == 0) statement.executeBatch();
            }
            if (pending % batchSize != 0) statement.executeBatch();
        }
    }

    private String buildInsertSql() {
        String q = mysql() ? "`" : "\"";
        String sql = "INSERT INTO " + quoteQualified(table, q) + " (" +
            columns.stream().map(c -> quote(c, q)).collect(Collectors.joining(",")) +
            ") VALUES (" + String.join(",", java.util.Collections.nCopies(columns.size(), "?")) + ")";
        if ("insert".equals(insertMode)) return sql;
        if (!columns.containsAll(primaryKeyFields))
            throw new ConnectException("Every pk.fields entry must exist in the incoming Struct schema");
        if (mysql()) {
            List<String> updates = columns.stream().filter(c -> !primaryKeyFields.contains(c)).toList();
            if (updates.isEmpty()) return sql + " ON DUPLICATE KEY UPDATE " + quote(primaryKeyFields.get(0), q) + "=" + quote(primaryKeyFields.get(0), q);
            return sql + " ON DUPLICATE KEY UPDATE " + updates.stream()
                .map(c -> quote(c, q) + "=VALUES(" + quote(c, q) + ")").collect(Collectors.joining(","));
        }
        String keys = primaryKeyFields.stream().map(c -> quote(c, q)).collect(Collectors.joining(","));
        List<String> updates = columns.stream().filter(c -> !primaryKeyFields.contains(c)).toList();
        if (updates.isEmpty()) return sql + " ON CONFLICT (" + keys + ") DO NOTHING";
        return sql + " ON CONFLICT (" + keys + ") DO UPDATE SET " + updates.stream()
            .map(c -> quote(c, q) + "=EXCLUDED." + quote(c, q)).collect(Collectors.joining(","));
    }

    private boolean mysql() { return url.startsWith("jdbc:mysql:"); }
    private static String quoteQualified(String value, String q) {
        return java.util.Arrays.stream(value.split("\\.")).map(v -> quote(v, q)).collect(Collectors.joining("."));
    }
    private static String quote(String identifier, String q) { return q + identifier.replace(q, q + q) + q; }
    private void openConnection() {
        try { connection = DriverManager.getConnection(url, user, password); connection.setAutoCommit(false); }
        catch (SQLException ex) { throw new ConnectException("Unable to open JDBC connection", ex); }
    }
    private void reconnect() {
        try { if (connection != null) connection.close(); } catch (SQLException ignored) { }
        openConnection();
    }
    private void rollbackQuietly(SQLException original) {
        try { if (connection != null) connection.rollback(); }
        catch (SQLException rollback) { original.addSuppressed(rollback); }
    }
    private static boolean isRetryable(SQLException ex) {
        String state = ex.getSQLState();
        return ex instanceof SQLTransientException || ex instanceof SQLRecoverableException ||
            (state != null && (state.startsWith("08") || state.startsWith("40")));
    }
    private void sleepBeforeRetry(int failedAttempt) {
        try { Thread.sleep(retryPolicy.delayBeforeAttempt(failedAttempt).toMillis()); }
        catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ConnectException("Interrupted during JDBC retry backoff", ex);
        }
    }
    @Override public void stop() {
        try { if (connection != null) connection.close(); }
        catch (SQLException ex) { throw new ConnectException("Failed to close JDBC connection", ex); }
        finally { if (metrics != null) metrics.close(); }
    }
    @Override public String version() { return "0.1.0-SNAPSHOT"; }
    private static String required(Map<String, String> props, String key) {
        String value = props.get(key);
        if (value == null || value.isBlank()) throw new ConnectException("Missing required setting: " + key);
        return value;
    }
}