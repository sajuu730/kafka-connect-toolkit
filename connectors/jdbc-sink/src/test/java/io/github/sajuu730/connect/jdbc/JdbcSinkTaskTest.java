package io.github.sajuu730.connect.jdbc;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class JdbcSinkTaskTest {
    @Test void rejectsUnsafeTableIdentifiersBeforeOpeningConnection() {
        JdbcSinkTask task = new JdbcSinkTask();
        var error = assertThrows(org.apache.kafka.connect.errors.ConnectException.class, () ->
            task.start(Map.of(
                "connection.url", "jdbc:postgresql://localhost:5432/unused",
                "connection.user", "unused",
                "connection.password", "unused",
                "table.name", "events; DROP TABLE users")));
        assertTrue(error.getMessage().contains("Unsafe table.name"));
    }

    @Test void writesStructRecordsInBatchesAndCommits() throws Exception {
        String url = "jdbc:h2:mem:jdbc_sink_test;DB_CLOSE_DELAY=-1";
        try (var connection = DriverManager.getConnection(url, "sa", "");
             var statement = connection.createStatement()) {
            statement.execute("CREATE TABLE \"events\" (\"id\" VARCHAR(128) PRIMARY KEY, \"event_type\" VARCHAR(128), \"payload\" VARCHAR(512))");
        }

        Schema schema = SchemaBuilder.struct()
            .field("id", Schema.STRING_SCHEMA)
            .field("event_type", Schema.STRING_SCHEMA)
            .field("payload", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
        Struct first = new Struct(schema).put("id", "evt-1").put("event_type", "created").put("payload", "{\"n\":1}");
        Struct second = new Struct(schema).put("id", "evt-2").put("event_type", "updated").put("payload", "{\"n\":2}");
        var records = List.of(
            new SinkRecord("events", 0, null, null, schema, first, 0L),
            new SinkRecord("events", 0, null, null, schema, second, 1L));

        JdbcSinkTask task = new JdbcSinkTask();
        task.start(Map.of(
            "connection.url", url,
            "connection.user", "sa",
            "connection.password", "",
            "table.name", "events",
            "batch.size", "1",
            "retry.max.attempts", "2",
            "retry.initial.backoff.ms", "1"));
        task.put(records);
        task.stop();

        try (var connection = DriverManager.getConnection(url, "sa", "");
             var statement = connection.createStatement();
             var result = statement.executeQuery("SELECT COUNT(*) FROM \"events\"")) {
            assertTrue(result.next());
            assertEquals(2, result.getInt(1));
        }
    }
}