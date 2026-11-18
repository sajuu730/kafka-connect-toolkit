package io.github.sajuu730.connect.jdbc;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class JdbcSinkTask extends SinkTask {
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)?");
    private Connection connection;
    private String table;
    private int batchSize;
    private List<String> columns = List.of();

    @Override public void start(Map<String, String> props) {
        table = required(props, JdbcSinkConnector.TABLE);
        if (!IDENTIFIER.matcher(table).matches()) throw new ConnectException("Unsafe table.name; use a simple SQL identifier or schema.table");
        batchSize = Integer.parseInt(props.getOrDefault(JdbcSinkConnector.BATCH_SIZE, "500"));
        if (batchSize < 1) throw new ConnectException("batch.size must be >= 1");
        try {
            connection = DriverManager.getConnection(required(props, JdbcSinkConnector.URL),
                props.getOrDefault(JdbcSinkConnector.USER, ""), props.getOrDefault(JdbcSinkConnector.PASSWORD, ""));
            connection.setAutoCommit(false);
        } catch (SQLException ex) { throw new ConnectException("Unable to open JDBC connection", ex); }
    }

    @Override public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) return;
        try {
            int pending = 0;
            PreparedStatement statement = null;
            try {
                for (SinkRecord record : records) {
                    if (!(record.value() instanceof Struct struct))
                        throw new ConnectException("JDBC sink currently requires Struct record values");
                    List<String> rowColumns = struct.schema().fields().stream().map(Field::name).toList();
                    if (columns.isEmpty()) columns = rowColumns;
                    if (!columns.equals(rowColumns)) throw new ConnectException("Record schema changed within task; use a stable target schema");
                    if (statement == null) {
                        String sql = "INSERT INTO " + table + " (" + columns.stream().map(JdbcSinkTask::quote).collect(Collectors.joining(","))
                            + ") VALUES (" + String.join(",", java.util.Collections.nCopies(columns.size(), "?")) + ")";
                        statement = connection.prepareStatement(sql);
                    }
                    for (int i = 0; i < columns.size(); i++) statement.setObject(i + 1, struct.get(columns.get(i)));
                    statement.addBatch();
                    if (++pending % batchSize == 0) statement.executeBatch();
                }
                if (statement != null) statement.executeBatch();
            } finally {
                if (statement != null) statement.close();
            }
            connection.commit();
        } catch (Exception ex) {
            try { connection.rollback(); } catch (SQLException rollback) { ex.addSuppressed(rollback); }
            if (ex instanceof ConnectException ce) throw ce;
            throw new ConnectException("JDBC batch write failed; transaction rolled back", ex);
        }
    }

    @Override public void stop() {
        if (connection != null) try { connection.close(); } catch (SQLException ex) { throw new ConnectException("Failed to close JDBC connection", ex); }
    }
    @Override public String version() { return "0.1.0-SNAPSHOT"; }

    private static String quote(String identifier) { return "\"" + identifier.replace("\"", "\"\"") + "\""; }
    private static String required(Map<String, String> props, String key) {
        String value = props.get(key);
        if (value == null || value.isBlank()) throw new ConnectException("Missing required setting: " + key);
        return value;
    }
}