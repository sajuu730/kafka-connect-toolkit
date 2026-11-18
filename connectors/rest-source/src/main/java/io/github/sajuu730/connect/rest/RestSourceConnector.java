package io.github.sajuu730.connect.rest;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;
import java.util.List;
import java.util.Map;

public final class RestSourceConnector extends SourceConnector {
    static final String ENDPOINT = "rest.endpoint";
    static final String TOPIC = "kafka.topic";
    static final String ID_FIELD = "record.id.field";
    static final String POLL_INTERVAL = "poll.interval.ms";
    static final String MAX_ATTEMPTS = "retry.max.attempts";
    static final String INITIAL_BACKOFF = "retry.initial.backoff.ms";
    static final String BEARER_TOKEN = "rest.auth.bearer.token";
    static final ConfigDef CONFIG = new ConfigDef()
        .define(ENDPOINT, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "HTTP endpoint returning a JSON array or object")
        .define(TOPIC, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Kafka topic for emitted records")
        .define(ID_FIELD, ConfigDef.Type.STRING, "id", ConfigDef.Importance.MEDIUM, "Stable JSON field used as the source offset")
        .define(POLL_INTERVAL, ConfigDef.Type.INT, 5000, ConfigDef.Range.atLeast(100), ConfigDef.Importance.LOW, "Delay between polls in milliseconds")
        .define(MAX_ATTEMPTS, ConfigDef.Type.INT, 3, ConfigDef.Range.between(1, 10), ConfigDef.Importance.MEDIUM, "Maximum HTTP attempts per poll")
        .define(INITIAL_BACKOFF, ConfigDef.Type.LONG, 500L, ConfigDef.Range.between(0L, 30000L), ConfigDef.Importance.LOW, "Initial retry backoff in milliseconds")
        .define(BEARER_TOKEN, ConfigDef.Type.PASSWORD, "", ConfigDef.Importance.MEDIUM, "Optional bearer token; never log this value");

    private Map<String, String> props;
    @Override public void start(Map<String, String> props) {
        Map<String, Object> parsed = CONFIG.parse(props);
        Password token = (Password) parsed.get(BEARER_TOKEN);
        this.props = Map.of(
            ENDPOINT, (String) parsed.get(ENDPOINT),
            TOPIC, (String) parsed.get(TOPIC),
            ID_FIELD, (String) parsed.get(ID_FIELD),
            POLL_INTERVAL, parsed.get(POLL_INTERVAL).toString(),
            MAX_ATTEMPTS, parsed.get(MAX_ATTEMPTS).toString(),
            INITIAL_BACKOFF, parsed.get(INITIAL_BACKOFF).toString(),
            BEARER_TOKEN, token.value());
    }
    @Override public Class<? extends Task> taskClass() { return RestSourceTask.class; }
    @Override public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (maxTasks < 1) throw new IllegalArgumentException("maxTasks must be >= 1");
        return java.util.Collections.nCopies(maxTasks, props);
    }
    @Override public void stop() { }
    @Override public ConfigDef config() { return CONFIG; }
    @Override public String version() { return "0.1.0-SNAPSHOT"; }
}