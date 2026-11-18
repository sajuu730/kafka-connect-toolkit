package io.github.sajuu730.connect.jdbc;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;
import java.util.List;
import java.util.Map;

public final class JdbcSinkConnector extends SinkConnector {
    static final String URL = "connection.url";
    static final String USER = "connection.user";
    static final String PASSWORD = "connection.password";
    static final String TABLE = "table.name";
    static final String BATCH_SIZE = "batch.size";
    static final ConfigDef CONFIG = new ConfigDef()
        .define(URL, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "JDBC connection URL")
        .define(USER, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM, "Database username")
        .define(PASSWORD, ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH, "Database password")
        .define(TABLE, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Target table (optionally schema-qualified)")
        .define(BATCH_SIZE, ConfigDef.Type.INT, 500, ConfigDef.Range.between(1, 10000), ConfigDef.Importance.MEDIUM, "Maximum records per JDBC batch")
        .define("retry.max.attempts", ConfigDef.Type.INT, 3, ConfigDef.Range.between(1, 10), ConfigDef.Importance.MEDIUM, "Maximum retries for transient SQL failures")
        .define("retry.initial.backoff.ms", ConfigDef.Type.LONG, 500L, ConfigDef.Range.atLeast(0L), ConfigDef.Importance.LOW, "Initial retry backoff in milliseconds");

    private Map<String, String> taskProps;
    @Override public void start(Map<String, String> props) {
        Map<String, Object> parsed = CONFIG.parse(props);
        Password password = (Password) parsed.get(PASSWORD);
        taskProps = Map.of(URL, (String) parsed.get(URL), USER, (String) parsed.get(USER),
            PASSWORD, password.value(), TABLE, (String) parsed.get(TABLE), BATCH_SIZE, parsed.get(BATCH_SIZE).toString(),
            "retry.max.attempts", parsed.get("retry.max.attempts").toString(),
            "retry.initial.backoff.ms", parsed.get("retry.initial.backoff.ms").toString());
    }
    @Override public Class<? extends Task> taskClass() { return JdbcSinkTask.class; }
    @Override public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (maxTasks < 1) throw new IllegalArgumentException("maxTasks must be >= 1");
        return java.util.Collections.nCopies(maxTasks, taskProps);
    }
    @Override public void stop() { }
    @Override public ConfigDef config() { return CONFIG; }
    @Override public String version() { return "0.1.0-SNAPSHOT"; }
}