package io.github.sajuu730.connect.jdbc;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.DriverManager;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers(disabledWithoutDocker = true)
class JdbcSinkPostgresContainerTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES =
        new PostgreSQLContainer<>("postgres:16-alpine");

    @Test
    void writesConnectStructRecordsToPostgresAndCommitsTransaction() throws Exception {
        try (var connection = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             var statement = connection.createStatement()) {
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
            .put("payload", "{\"source\":\"testcontainers\"}");
        SinkRecord record = new SinkRecord("events", 0, null, null, schema, value, 0L);

        JdbcSinkTask task = new JdbcSinkTask();
        task.start(Map.of(
            "connection.url", POSTGRES.getJdbcUrl(),
            "connection.user", POSTGRES.getUsername(),
            "connection.password", POSTGRES.getPassword(),
            "table.name", "public.events",
            "batch.size", "100",
            "retry.max.attempts", "2",
            "retry.initial.backoff.ms", "10"));
        try {
            task.put(List.of(record));
        } finally {
            task.stop();
        }

        try (var connection = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
             var statement = connection.prepareStatement(
                "SELECT event_type, payload FROM events WHERE id = ?")) {
            statement.setString(1, "pg-event-1");
            try (var result = statement.executeQuery()) {
                assertTrue(result.next());
                assertEquals("created", result.getString("event_type"));
                assertEquals("{\"source\":\"testcontainers\"}", result.getString("payload"));
            }
        }
    }
}