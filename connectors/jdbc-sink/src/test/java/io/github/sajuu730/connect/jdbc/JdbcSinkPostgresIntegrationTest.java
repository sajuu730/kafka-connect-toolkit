package io.github.sajuu730.connect.jdbc;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class JdbcSinkPostgresIntegrationTest {
    @Test
    void writesConnectStructRecordsToPostgresAndCommitsTransaction() throws Exception {
        String url = System.getenv("KCT_POSTGRES_TEST_URL");
        String user = System.getenv("KCT_POSTGRES_TEST_USER");
        String password = System.getenv("KCT_POSTGRES_TEST_PASSWORD");
        assumeTrue(url != null && !url.isBlank(),
            "Set KCT_POSTGRES_TEST_URL to enable the PostgreSQL integration test");
        assumeTrue(user != null && password != null,
            "Set KCT_POSTGRES_TEST_USER and KCT_POSTGRES_TEST_PASSWORD");

        try (var connection = DriverManager.getConnection(url, user, password);
             var statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS events");
            statement.execute("""
                CREATE TABLE events (
                    id VARCHAR(128) PRIMARY KEY,
                    event_type VARCHAR(128) NOT NULL,
                    payload TEXT
                )
                """);
        }

        Schema schema = SchemaBuilder.struct()
            .field("id", Schema.STRING_SCHEMA)
            .field("event_type", Schema.STRING_SCHEMA)
            .field("payload", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
        Struct value = new Struct(schema)
            .put("id", "pg-event-1")
            .put("event_type", "created")
            .put("payload", "{\"source\":\"postgres-integration\"}");
        SinkRecord record = new SinkRecord("events", 0, null, null, schema, value, 0L);
        Struct updatedValue = new Struct(schema)
            .put("id", "pg-event-1")
            .put("event_type", "updated")
            .put("payload", "{\\"source\\":\\"upsert\\"}");
        SinkRecord update = new SinkRecord("events", 0, null, null, schema, updatedValue, 1L);

        JdbcSinkTask task = new JdbcSinkTask();
        task.start(Map.of(
            "connection.url", url,
            "connection.user", user,
            "connection.password", password,
            "table.name", "public.events",
            "insert.mode", "upsert",
            "pk.fields", "id",
            "batch.size", "100",
            "retry.max.attempts", "2",
            "retry.initial.backoff.ms", "10"));
        try {
            task.put(List.of(record, update));
        } finally {
            task.stop();
        }

        try (var connection = DriverManager.getConnection(url, user, password);
             var statement = connection.prepareStatement(
                "SELECT event_type, payload FROM events WHERE id = ?")) {
            statement.setString(1, "pg-event-1");
            try (var result = statement.executeQuery()) {
                assertTrue(result.next());
                assertEquals("created", result.getString("event_type"));
                assertEquals("{\"source\":\"postgres-integration\"}", result.getString("payload"));
            }
        }
    }
}