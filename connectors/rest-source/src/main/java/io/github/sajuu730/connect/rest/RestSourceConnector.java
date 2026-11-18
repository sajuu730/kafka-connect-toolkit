package io.github.sajuu730.connect.rest;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.apache.kafka.common.config.ConfigDef;
import java.util.List;
import java.util.Map;

public final class RestSourceConnector extends SourceConnector {
    static final String ENDPOINT = "rest.endpoint";
    static final String TOPIC = "kafka.topic";
    static final String ID_FIELD = "record.id.field";
    static final String POLL_INTERVAL = "poll.interval.ms";
    static final ConfigDef CONFIG = new ConfigDef()
        .define(ENDPOINT, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "HTTP endpoint returning a JSON array or object")
        .define(TOPIC, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Kafka topic for emitted records")
        .define(ID_FIELD, ConfigDef.Type.STRING, "id", ConfigDef.Importance.MEDIUM, "Stable JSON field used as the source offset")
        .define(POLL_INTERVAL, ConfigDef.Type.INT, 5000, ConfigDef.Range.atLeast(100), ConfigDef.Importance.LOW, "Delay between polls in milliseconds");

    private Map<String, String> props;

    @Override public void start(Map<String, String> props) {
        var parsed = new SimpleConfig(CONFIG, props);
        this.props = Map.of(
            ENDPOINT, parsed.getString(ENDPOINT),
            TOPIC, parsed.getString(TOPIC),
            ID_FIELD, parsed.getString(ID_FIELD),
            POLL_INTERVAL, Integer.toString(parsed.getInt(POLL_INTERVAL)));
    }
    @Override public Class<? extends Task> taskClass() { return RestSourceTask.class; }
    @Override public List<Map<String, String>> taskConfigs(int maxTasks) { return List.of(props); }
    @Override public void stop() { }
    @Override public ConfigDef config() { return CONFIG; }
    @Override public String version() { return "0.1.0-SNAPSHOT"; }
}