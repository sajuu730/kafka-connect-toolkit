package io.github.sajuu730.connect.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class RestSourceTask extends SourceTask {
    private static final ObjectMapper JSON = new ObjectMapper();
    private HttpClient client;
    private String endpoint;
    private String topic;
    private String idField;
    private long pollIntervalMs;
    private volatile boolean running;

    @Override public void start(Map<String, String> props) {
        endpoint = required(props, RestSourceConnector.ENDPOINT);
        topic = required(props, RestSourceConnector.TOPIC);
        idField = props.getOrDefault(RestSourceConnector.ID_FIELD, "id");
        pollIntervalMs = Long.parseLong(props.getOrDefault(RestSourceConnector.POLL_INTERVAL, "5000"));
        if (pollIntervalMs < 100) throw new ConnectException("poll.interval.ms must be >= 100");
        client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        running = true;
    }

    @Override public List<SourceRecord> poll() throws InterruptedException {
        if (!running) return null;
        try {
            var request = HttpRequest.newBuilder(URI.create(endpoint))
                .timeout(Duration.ofSeconds(30)).header("Accept", "application/json").GET().build();
            var response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300)
                throw new ConnectException("REST endpoint returned HTTP " + response.statusCode());
            JsonNode root = JSON.readTree(response.body());
            List<JsonNode> rows = new ArrayList<>();
            if (root.isArray()) root.forEach(rows::add);
            else if (root.isObject()) rows.add(root);
            else throw new ConnectException("REST response must be a JSON object or array");
            List<SourceRecord> records = new ArrayList<>();
            Map<String, ?> partition = Map.of("endpoint", endpoint);
            for (JsonNode row : rows) {
                JsonNode id = row.get(idField);
                if (id == null || id.isNull()) throw new ConnectException("Response record is missing stable id field: " + idField);
                Map<String, ?> offset = Map.of("id", id.asText());
                records.add(new SourceRecord(partition, offset, topic, Schema.STRING_SCHEMA, JSON.writeValueAsString(row)));
            }
            if (records.isEmpty()) Thread.sleep(pollIntervalMs);
            return records;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw ex;
        } catch (Exception ex) {
            if (ex instanceof ConnectException ce) throw ce;
            throw new ConnectException("REST source poll failed", ex);
        }
    }

    @Override public void stop() { running = false; }
    @Override public String version() { return "0.1.0-SNAPSHOT"; }

    private static String required(Map<String, String> props, String key) {
        String value = props.get(key);
        if (value == null || value.isBlank()) throw new ConnectException("Missing required setting: " + key);
        return value;
    }
}