package io.github.sajuu730.connect.jdbc;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
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
        .define(BATCH_SIZE, ConfigDef.Type.INT, 500, ConfigDef.Range.atLeast(1), ConfigDef.Importance.MEDIUM, "Maximum records per JDBC batch");

    private Map<String, String> taskProps;
    @Override public void start(Map<String, String> props) {
        var c = new SimpleConfig(CONFIG, props);
        taskProps = Map.of(URL, c.getString(URL), USER, c.getString(USER), PASSWORD, c.getPassword(PASSWORD).value(),
            TABLE, c.getString(TABLE), BATCH_SIZE, Integer.toString(c.getInt(BATCH_SIZE)));
    }
    @Override public Class<? extends Task> taskClass() { return JdbcSinkTask.class; }
    @Override public List<Map<String, String>> taskConfigs(int maxTasks) { return java.util.Collections.nCopies(maxTasks, taskProps); }
    @Override public void stop() { }
    @Override public ConfigDef config() { return CONFIG; }
    @Override public String version() { return "0.1.0-SNAPSHOT"; }
}