package io.github.sajuu730.connect.jdbc;

import io.github.sajuu730.connect.common.RetryPolicy;
import io.github.sajuu730.connect.common.TaskMetrics;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
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
    private String url;
    private String user;
    private String password;
    private String table;
    private int batchSize;
    private RetryPolicy retryPolicy;
    private TaskMetrics metrics;
    private List<String> columns = List.of();

    @Override public void start(Map<String, String> props) {
        url = required(props, JdbcSinkConnector.URL);
        user = props.getOrDefault(JdbcSinkConnector.USER, "");
        password = props.getOrDefault(JdbcSinkConnector.PASSWORD, "");
        table = required(props, JdbcSinkConnector.TABLE);
        if (!IDENTIFIER.matcher(table).matches()) throw new ConnectException("Unsafe table.name; use a simple SQL identifier or schema.table");
        batchSize = Integer.parseInt(props.getOrDefault(JdbcSinkConnector.BATCH_SIZE, "500"));
        int attempts = Integer.parseInt(props.getOrDefault("retry.max.attempts", "3"));
        long initialBackoff = Long.parseLong(props.getOrDefault("retry.initial.backoff.ms", "500"));
        if (batchSize < 1 || attempts < 1 || initialBackoff < 0)
            throw new ConnectException("Invalid JDBC batch or retry configuration");
        retryPolicy = new RetryPolicy(attempts, Duration.ofMillis(initialBackoff), Duration.ofSeconds(30), 0.2);
        openConnection();
        metrics = new TaskMetrics("jdbc-sink");
    }

    @Override public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) return;
        for (SinkRecord record : records) {
            if (!(record.value() instanceof Struct struct))
                throw new ConnectException("JDBC sink currently requires Struct record values");
            List<String> rowColumns = struct.schema().fields().stream().map(Field::name).toList();
            if (rowColumns.isEmpty()) throw new ConnectException("JDBC sink cannot insert a Struct with no fields");
            if (columns.isEmpty()) columns = rowColumns;
            if (!columns.equals(rowColumns))
                throw new ConnectException("Record schema changed within task; use a stable target schema");
        }

        for (int attempt = 1; attempt <= retryPolicy.maxAttempts(); attempt++) {
            try {
                executeBatch(records);
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
                metrics.processed(records.size());
                return;
            } catch (SQLException ex) {
                rollbackQuietly(ex);
                metrics.failure();
                // Commit outcome can be ambiguous after a network failure; let the Connect worker retry the task.
                throw new ConnectException("JDBC commit failed; outcome may be ambiguous", ex);
            }
        }
    }

    private void executeBatch(Collection<SinkRecord> records) throws SQLException {
        String sql = "INSERT INTO " + quoteQualified(table) + " (" +
            columns.stream().map(JdbcSinkTask::quote).collect(Collectors.joining(",")) +
            ") VALUES (" + String.join(",", java.util.Collections.nCopies(columns.size(), "?")) + ")";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
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

    private void openConnection() {
        try {
            connection = DriverManager.getConnection(url, user, password);
            connection.setAutoCommit(false);
        } catch (SQLException ex) {
            throw new ConnectException("Unable to open JDBC connection", ex);
        }
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
        try {
            if (connection != null) connection.close();
        } catch (SQLException ex) {
            throw new ConnectException("Failed to close JDBC connection", ex);
        } finally {
            if (metrics != null) metrics.close();
        }
    }
    @Override public String version() { return "0.1.0-SNAPSHOT"; }

    private static String quoteQualified(String value) {
        return java.util.Arrays.stream(value.split("\\.")).map(JdbcSinkTask::quote).collect(Collectors.joining("."));
    }
    private static String quote(String identifier) { return "\"" + identifier.replace("\"", "\"\"") + "\""; }
    private static String required(Map<String, String> props, String key) {
        String value = props.get(key);
        if (value == null || value.isBlank()) throw new ConnectException("Missing required setting: " + key);
        return value;
    }
}