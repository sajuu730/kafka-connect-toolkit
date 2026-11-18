package io.github.sajuu730.connect.rest;

import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RestSourceTaskTest {
    private HttpServer server;

    @AfterEach void stopServer() { if (server != null) server.stop(0); }

    @Test void emitsJsonRecordsAndAdvancesSourceOffsets() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/events", exchange -> {
            byte[] body = "[{\"id\":\"a\",\"type\":\"created\"},{\"id\":\"b\",\"type\":\"updated\"}]".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();

        OffsetStorageReader offsets = mock(OffsetStorageReader.class);
        when(offsets.offset(Map.of("endpoint", endpoint()))).thenReturn(null);
        SourceTaskContext context = mock(SourceTaskContext.class);
        when(context.offsetStorageReader()).thenReturn(offsets);

        RestSourceTask task = new RestSourceTask();
        task.initialize(context);
        task.start(Map.of(
            "rest.endpoint", endpoint(), "kafka.topic", "events",
            "poll.interval.ms", "100", "retry.max.attempts", "2",
            "retry.initial.backoff.ms", "1"));

        List<SourceRecord> records = task.poll();
        assertEquals(2, records.size());
        assertEquals(Map.of("id", "a"), records.get(0).sourceOffset());
        assertEquals(Map.of("id", "b"), records.get(1).sourceOffset());
        Struct firstValue = (Struct) records.get(0).value();
        assertEquals("a", firstValue.getString("id"));
        assertEquals("created", firstValue.getString("type"));
        assertEquals(org.apache.kafka.connect.data.Schema.Type.STRING, records.get(0).valueSchema().field("type").schema().type());
        task.commitRecord(records.get(0), null);
        List<SourceRecord> afterAcknowledgement = task.poll();
        assertEquals(1, afterAcknowledgement.size());
        assertEquals(Map.of("id", "b"), afterAcknowledgement.get(0).sourceOffset());
        task.stop();
    }

    @Test void resumesAfterThePersistedRecordId() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/events", exchange -> {
            byte[] body = "[{\"id\":\"a\"},{\"id\":\"b\"},{\"id\":\"c\"}]".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();

        OffsetStorageReader offsets = mock(OffsetStorageReader.class);
        when(offsets.offset(Map.of("endpoint", endpoint()))).thenReturn(Map.of("id", "b"));
        SourceTaskContext context = mock(SourceTaskContext.class);
        when(context.offsetStorageReader()).thenReturn(offsets);

        RestSourceTask task = new RestSourceTask();
        task.initialize(context);
        task.start(Map.of("rest.endpoint", endpoint(), "kafka.topic", "events", "poll.interval.ms", "100"));
        List<SourceRecord> records = task.poll();
        assertEquals(1, records.size());
        assertEquals(Map.of("id", "c"), records.get(0).sourceOffset());
        task.stop();
    }

    @Test void readsAllConfiguredPagesUntilTheEmptyPage() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/events", exchange -> {
            String query = exchange.getRequestURI().getRawQuery();
            String body = switch (query) {
                case "page=1" -> "[{\"id\":\"page-1\"}]";
                case "page=2" -> "[{\"id\":\"page-2\"}]";
                case "page=3" -> "[]";
                default -> throw new IllegalArgumentException("Unexpected page query: " + query);
            };
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();

        OffsetStorageReader offsets = mock(OffsetStorageReader.class);
        when(offsets.offset(Map.of("endpoint", endpoint()))).thenReturn(null);
        SourceTaskContext context = mock(SourceTaskContext.class);
        when(context.offsetStorageReader()).thenReturn(offsets);

        RestSourceTask task = new RestSourceTask();
        task.initialize(context);
        task.start(Map.of(
            "rest.endpoint", endpoint(), "kafka.topic", "events",
            "poll.interval.ms", "100", "rest.pagination.page.parameter", "page",
            "rest.pagination.start.page", "1", "rest.pagination.max.pages", "5"));
        List<SourceRecord> records = task.poll();
        assertEquals(2, records.size());
        assertEquals(Map.of("id", "page-1"), records.get(0).sourceOffset());
        assertEquals(Map.of("id", "page-2"), records.get(1).sourceOffset());
        task.stop();
    }

    @Test void retriesRateLimitedRequestAndHonorsZeroRetryAfter() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        server.createContext("/events", exchange -> {
            byte[] body;
            if (calls.incrementAndGet() == 1) {
                exchange.getResponseHeaders().add("Retry-After", "0");
                body = "{\"error\":\"rate limited\"}".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(429, body.length);
            } else {
                body = "[{\"id\":\"after-limit\"}]".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, body.length);
            }
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();

        OffsetStorageReader offsets = mock(OffsetStorageReader.class);
        when(offsets.offset(Map.of("endpoint", endpoint()))).thenReturn(null);
        SourceTaskContext context = mock(SourceTaskContext.class);
        when(context.offsetStorageReader()).thenReturn(offsets);
        RestSourceTask task = new RestSourceTask();
        task.initialize(context);
        task.start(Map.of("rest.endpoint", endpoint(), "kafka.topic", "events",
            "poll.interval.ms", "100", "retry.max.attempts", "2",
            "retry.initial.backoff.ms", "1000"));
        try {
            List<SourceRecord> records = task.poll();
            assertEquals(1, records.size());
            assertEquals(Map.of("id", "after-limit"), records.get(0).sourceOffset());
            assertEquals(2, calls.get());
        } finally {
            task.stop();
        }
    }

    @Test void parsesRetryAfterDeltaSecondsAndFallsBackForInvalidValues() {
        assertEquals(0L, RestSourceTask.parseRetryAfterMillis("0"));
        assertEquals(2000L, RestSourceTask.parseRetryAfterMillis("2"));
        assertEquals(-1L, RestSourceTask.parseRetryAfterMillis("not-a-date"));
    }

    private String endpoint() { return "http://127.0.0.1:" + server.getAddress().getPort() + "/events"; }
}