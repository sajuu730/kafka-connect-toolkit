package io.github.sajuu730.connect.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.sajuu730.connect.common.RetryPolicy;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class RestSourceTask extends SourceTask {
    private static final ObjectMapper JSON = new ObjectMapper();
    private HttpClient client;
    private String endpoint;
    private String topic;
    private String idField;
    private long pollIntervalMs;
    private RetryPolicy retryPolicy;
    private volatile boolean running;
    private String lastEmittedId;

    @Override public void start(Map<String, String> props) {
        endpoint = required(props, RestSourceConnector.ENDPOINT);
        topic = required(props, RestSourceConnector.TOPIC);
        idField = props.getOrDefault(RestSourceConnector.ID_FIELD, "id");
        pollIntervalMs = Long.parseLong(props.getOrDefault(RestSourceConnector.POLL_INTERVAL, "5000"));
        int attempts = Integer.parseInt(props.getOrDefault(RestSourceConnector.MAX_ATTEMPTS, "3"));
        long initialBackoff = Long.parseLong(props.getOrDefault(RestSourceConnector.INITIAL_BACKOFF, "500"));
        if (pollIntervalMs < 100 || attempts < 1 || initialBackoff < 0)
            throw new ConnectException("Invalid REST source polling or retry configuration");
        retryPolicy = new RetryPolicy(attempts, Duration.ofMillis(initialBackoff),
            Duration.ofSeconds(30), 0.2);
        client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        Map<String, ?> partition = Map.of("endpoint", endpoint);
        if (context != null && context.offsetStorageReader() != null) {
            Map<String, Object> stored = context.offsetStorageReader().offset(partition);
            if (stored != null && stored.get("id") != null) lastEmittedId = stored.get("id").toString();
        }
        running = true;
    }

    @Override public List<SourceRecord> poll() throws InterruptedException {
        if (!running) return null;
        try {
            HttpResponse<String> response = fetchWithRetry();
            JsonNode root = JSON.readTree(response.body());
            List<JsonNode> rows = new ArrayList<>();
            if (root.isArray()) root.forEach(rows::add);
            else if (root.isObject()) rows.add(root);
            else throw new ConnectException("REST response must be a JSON object or array");

            Map<String, ?> partition = Map.of("endpoint", endpoint);
            List<SourceRecord> records = new ArrayList<>();
            boolean resumeFound = lastEmittedId == null;
            String newestId = lastEmittedId;
            for (JsonNode row : rows) {
                if (!row.isObject()) throw new ConnectException("Every REST response record must be a JSON object");
                JsonNode id = row.get(idField);
                if (id == null || id.isNull()) throw new ConnectException("Response record is missing stable id field: " + idField);
                String idValue = id.asText();
                if (!resumeFound) {
                    if (idValue.equals(lastEmittedId)) resumeFound = true;
                    continue;
                }
                records.add(new SourceRecord(partition, Map.of("id", idValue), topic,
                    org.apache.kafka.connect.data.Schema.STRING_SCHEMA, JSON.writeValueAsString(row)));
                newestId = idValue;
            }
            // If the API no longer contains the saved offset, replay the current snapshot rather than silently dropping it.
            if (!resumeFound && lastEmittedId != null) {
                for (JsonNode row : rows) {
                    String idValue = row.path(idField).asText();
                    records.add(new SourceRecord(partition, Map.of("id", idValue), topic,
                        org.apache.kafka.connect.data.Schema.STRING_SCHEMA, JSON.writeValueAsString(row)));
                    newestId = idValue;
                }
            }
            if (!records.isEmpty()) lastEmittedId = newestId;
            else sleepWhileRunning(pollIntervalMs);
            return records;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw ex;
        } catch (ConnectException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ConnectException("REST source poll failed", ex);
        }
    }

    private HttpResponse<String> fetchWithRetry() throws InterruptedException {
        Exception lastFailure = null;
        for (int attempt = 1; attempt <= retryPolicy.maxAttempts() && running; attempt++) {
            try {
                var request = HttpRequest.newBuilder(URI.create(endpoint))
                    .timeout(Duration.ofSeconds(30)).header("Accept", "application/json").GET().build();
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                int status = response.statusCode();
                if (status >= 200 && status < 300) return response;
                if (status < 500 && status != 429)
                    throw new ConnectException("REST endpoint returned non-retryable HTTP " + status);
                lastFailure = new IOException("Retryable REST response: HTTP " + status);
            } catch (ConnectException ex) {
                throw ex;
            } catch (IOException ex) {
                lastFailure = ex;
            }
            if (attempt < retryPolicy.maxAttempts()) sleepWhileRunning(retryPolicy.delayBeforeAttempt(attempt).toMillis());
        }
        if (!running) throw new ConnectException("REST source task stopped during request");
        throw new ConnectException("REST request failed after " + retryPolicy.maxAttempts() + " attempts", lastFailure);
    }

    private void sleepWhileRunning(long millis) throws InterruptedException {
        if (millis > 0 && running) Thread.sleep(millis);
    }

    @Override public void stop() { running = false; }
    @Override public String version() { return "0.1.0-SNAPSHOT"; }

    private static String required(Map<String, String> props, String key) {
        String value = props.get(key);
        if (value == null || value.isBlank()) throw new ConnectException("Missing required setting: " + key);
        return value;
    }
}