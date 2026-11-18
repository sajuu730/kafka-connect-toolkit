package io.github.sajuu730.connect.jdbc;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.clients.producer.RecordMetadata;
import java.util.concurrent.CompletableFuture;
import static org.mockito.Mockito.*;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class JdbcSinkMySqlContainerTest {
    @Container static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
        .withDatabaseName("kct").withUsername("kct").withPassword("kct-password");

    @Test void writesAndUpsertsAgainstRealMysql() throws Exception {
        try (var c=DriverManager.getConnection(MYSQL.getJdbcUrl(),MYSQL.getUsername(),MYSQL.getPassword());
             var st=c.createStatement()) {
            st.execute("CREATE TABLE events (id VARCHAR(64) PRIMARY KEY, payload VARCHAR(255) NULL)");
        }
        JdbcSinkTask task=new JdbcSinkTask();
        task.initialize(null);
        task.start(Map.of("connection.url",MYSQL.getJdbcUrl(),"connection.user",MYSQL.getUsername(),
            "connection.password",MYSQL.getPassword(),"table.name","events","insert.mode","upsert","pk.fields","id"));
        Schema schema=SchemaBuilder.struct().field("id",Schema.STRING_SCHEMA).field("payload",Schema.STRING_SCHEMA).build();
        try {
            task.put(List.of(new SinkRecord("events",0,null,null,schema,new Struct(schema).put("id","e1").put("payload","first"),0L)));
            task.put(List.of(new SinkRecord("events",0,null,null,schema,new Struct(schema).put("id","e1").put("payload","updated"),1L)));
        } finally { task.stop(); }
        try (var c=DriverManager.getConnection(MYSQL.getJdbcUrl(),MYSQL.getUsername(),MYSQL.getPassword());
             var st=c.createStatement(); var rs=st.executeQuery("SELECT payload FROM events WHERE id='e1'")) {
            assertTrue(rs.next()); assertEquals("updated",rs.getString(1)); assertFalse(rs.next());
        }
    }

    @Test void reportsMalformedRecordsThroughErrantRecordReporter() throws Exception {
        try (var c=DriverManager.getConnection(MYSQL.getJdbcUrl(),MYSQL.getUsername(),MYSQL.getPassword());
             var st=c.createStatement()) {
            st.execute("CREATE TABLE malformed_events (id VARCHAR(64) PRIMARY KEY)");
        }
        ErrantRecordReporter reporter=mock(ErrantRecordReporter.class);
        when(reporter.report(any(SinkRecord.class), any(Throwable.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        SinkTaskContext context=mock(SinkTaskContext.class);
        when(context.errantRecordReporter()).thenReturn(reporter);
        JdbcSinkTask task=new JdbcSinkTask();
        task.initialize(context);
        task.start(Map.of("connection.url",MYSQL.getJdbcUrl(),"connection.user",MYSQL.getUsername(),
            "connection.password",MYSQL.getPassword(),"table.name","malformed_events"));
        try {
            task.put(List.of(new SinkRecord("events",0,null,null,null,"not-a-struct",0L)));
            verify(reporter, times(1)).report(any(SinkRecord.class), any(Throwable.class));
        } finally { task.stop(); }
    }

    @Test void addsNullableColumnWhenStructSchemaGrows() throws Exception {
        try (var c=DriverManager.getConnection(MYSQL.getJdbcUrl(),MYSQL.getUsername(),MYSQL.getPassword());
             var st=c.createStatement()) { st.execute("CREATE TABLE evolving (id VARCHAR(64) PRIMARY KEY)"); }
        JdbcSinkTask task=new JdbcSinkTask();
        task.initialize(null);
        task.start(Map.of("connection.url",MYSQL.getJdbcUrl(),"connection.user",MYSQL.getUsername(),
            "connection.password",MYSQL.getPassword(),"table.name","evolving","insert.mode","upsert","pk.fields","id"));
        Schema schema=SchemaBuilder.struct().field("id",Schema.STRING_SCHEMA)
            .field("new_field",Schema.OPTIONAL_STRING_SCHEMA).build();
        try { task.put(List.of(new SinkRecord("events",0,null,null,schema,
            new Struct(schema).put("id","e2").put("new_field","added"),0L))); }
        finally { task.stop(); }
        try (var c=DriverManager.getConnection(MYSQL.getJdbcUrl(),MYSQL.getUsername(),MYSQL.getPassword());
             var st=c.createStatement(); var rs=st.executeQuery("SELECT new_field FROM evolving WHERE id='e2'")) {
            assertTrue(rs.next()); assertEquals("added",rs.getString(1));
        }
    }
}
