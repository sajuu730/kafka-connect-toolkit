# Local operations

## Start dependencies

```bash
docker compose up -d
```

Kafka is available at `localhost:9092`, Kafka Connect REST at `http://localhost:8083`, and PostgreSQL at `localhost:5432`. The Compose environment is for local development only; it uses plaintext listeners and demo credentials.

## Build

```bash
mvn --batch-mode verify
```

The connector modules are built as Maven artifacts. Packaging and deployment into a Kafka Connect worker plugin directory will be documented after the connector packaging and end-to-end test path is complete.

## REST source configuration

The initial REST source task accepts:

| Property | Meaning |
|---|---|
| `rest.endpoint` | HTTP endpoint returning a JSON object or array |
| `kafka.topic` | Destination topic |
| `record.id.field` | Stable identifier field used as source offset (default `id`) |
| `poll.interval.ms` | Idle poll delay (minimum 100 ms) |

The endpoint must return records with a stable, non-null identifier. The current implementation emits JSON records as strings. Authentication, pagination, rate limiting, incremental cursor strategies, and retry classification are not implemented yet.

## JDBC sink configuration

The initial JDBC sink accepts `connection.url`, `connection.user`, `connection.password`, `table.name`, and `batch.size`. It currently expects Kafka Connect `Struct` values and issues parameterized INSERT statements. Table identifiers are restricted to simple identifiers or schema-qualified identifiers; values are always passed as prepared-statement parameters.

The initial sink does not yet implement upsert, schema evolution, DLQ routing, or configurable error tolerance. Use only with a pre-created table whose columns match the incoming Struct fields.
