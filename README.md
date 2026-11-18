# Kafka Connect Toolkit

[![CI](https://github.com/sajuu730/kafka-connect-toolkit/actions/workflows/ci.yml/badge.svg)](https://github.com/sajuu730/kafka-connect-toolkit/actions/workflows/ci.yml)

A Java 17 multi-module project for custom Kafka Connect integrations: an HTTP REST source and a JDBC sink, with reliability primitives, containerized infrastructure and an evolving observability stack.

> **Status: Active development.** MySQL 8.4 is now the primary local database target. The JDBC sink has a MySQL `ON DUPLICATE KEY UPDATE` SQL path and the local Compose stack includes Kafka, Schema Registry, Kafka Connect, MySQL, Prometheus, a JMX exporter and Grafana. This is not a production-readiness claim: the REST cursor contract is intentionally limited to stable ascending IDs, and full Kafka Connect end-to-end fault-recovery verification remains outstanding.

## Current implementation

### REST source
- Java `HttpClient` polling with bounded connect/request timeouts
- JSON object/array decoding and page-number pagination with a maximum-page guard
- Stable record IDs emitted as Kafka Connect source offsets
- Reads the saved source offset on task startup and filters out IDs at or below the last committed ID (numeric comparison when possible, lexical otherwise)
- Optional `rest.incremental.cursor.parameter` sends the last committed ID to APIs that accept a query cursor
- Capped exponential retry with jitter for transient HTTP and I/O failures
- HTTP 429/503 throttling honors `Retry-After` delta-seconds or HTTP-date headers (bounded to five minutes), falling back to exponential backoff when absent or invalid
- Optional bearer-token authentication

**Limitations:** checkpointing assumes IDs are stable and monotonically ascending. Opaque cursors, arbitrary `next` links/tokens, compound keys and out-of-order APIs are not supported. Configure the cursor query parameter only when the remote API accepts the last ID as its cursor.

### JDBC sink
- `Struct` records written with prepared statements
- Configurable JDBC batching and transaction-per-`put` behavior
- MySQL-specific `INSERT ... ON DUPLICATE KEY UPDATE` when the JDBC URL is `jdbc:mysql:`
- PostgreSQL/H2-compatible `ON CONFLICT` path retained for existing tests
- Bounded retry for transient SQL execution errors; ambiguous commit failures fail the task rather than retrying blindly
- Additive-only schema evolution adds nullable columns for newly observed Struct fields; existing column types are not altered
- Uses Kafka Connect `ErrantRecordReporter` for malformed non-Struct/empty records when the worker provides a DLQ reporter; waits for the report future before returning

**Limitations:** schema evolution is deliberately additive-only and database-specific DDL must be reviewed before production use. A schema/DDL failure is treated as an infrastructure failure and fails the task; it is not mislabeled as a malformed record. Configure the worker DLQ topic and error handling settings for the reporter to route records.

## Technology
Java 17 · Apache Kafka Connect 3.8.1 · Maven · JUnit 5 · MySQL 8.4 · Docker Compose · Prometheus · Grafana

## Repository layout
```text
common/                         shared retry and metrics utilities
connectors/rest-source/         HTTP to Kafka source connector
connectors/jdbc-sink/           Kafka to JDBC sink connector
config/mysql-init.sql           local MySQL schema
monitoring/                     JMX exporter, Prometheus and Grafana provisioning
docs/                           architecture and operations
docker-compose.yml              local infrastructure stack
```

## Quick start
Requirements: JDK 17+, Maven 3.9+, Docker Engine and Docker Compose plugin.
```bash
mvn --batch-mode clean package
docker compose up -d
docker compose ps
curl http://localhost:8083/connector-plugins
```

Local endpoints:
- Kafka: `localhost:9092`
- Schema Registry: `http://localhost:8081`
- Kafka Connect REST: `http://localhost:8083`
- MySQL: `localhost:3306` (database `connect_demo`)
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (local demo credentials only)
- JMX exporter: `http://localhost:5556/metrics`

The Compose stack uses plaintext listeners and demo credentials. It is for local development only. Build the connector JARs before starting Kafka Connect because the worker mounts module `target/` directories.

## MySQL sink configuration example
```json
{
  "name": "mysql-events-sink",
  "config": {
    "connector.class": "io.github.sajuu730.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "connection.url": "jdbc:mysql://mysql:3306/connect_demo",
    "connection.user": "connect",
    "connection.password": "connect_local_only",
    "table.name": "events",
    "insert.mode": "upsert",
    "pk.fields": "id",
    "batch.size": "500"
  }
}
```

The local password is demonstrative and must never be reused in a deployed environment. Configure production credentials through a Kafka Connect ConfigProvider or secret manager.

## Reliability semantics
- Kafka Connect owns durable source offset commits; the connector emits an ID offset with each source record.
- Sink writes are grouped into JDBC transactions for each `put` call.
- SQL execution retry is limited to recognized transient failures.
- A failed commit has an ambiguous outcome and is surfaced as a task failure.
- Malformed record reporting uses the worker-provided `ErrantRecordReporter`; worker DLQ settings must be enabled for routing.
- Delivery should be treated as at-least-once; design keys and upsert semantics for idempotency.

## Roadmap
- [x] Java 17 Maven modules and shared bounded retry policy
- [x] REST polling, bearer auth, page-number pagination and source offsets
- [x] JDBC batching and transaction handling
- [x] MySQL driver and MySQL upsert SQL dialect
- [x] Local Kafka, Schema Registry, Kafka Connect and MySQL Compose services
- [x] Prometheus, JMX exporter and provisioned Grafana dashboard scaffolding
- [x] HTTP 429/503 retry handling with `Retry-After` parsing
- [x] Stable ascending-ID checkpoint filtering and optional query cursor parameter
- [ ] Opaque API cursors, next-page tokens and strict ordering guarantees
- [x] MySQL Testcontainers sink-task integration tests
- [ ] Kafka/Schema Registry/Kafka Connect-to-MySQL end-to-end tests
- [x] Additive nullable-column schema evolution policy
- [ ] Broader database dialect coverage and schema-evolution fault tests
- [x] ErrantRecordReporter-based malformed-record reporting
- [ ] Validate JMX exporter mappings and dashboard metrics against a live stack
- [ ] Fault-injection tests for task restart, rebalance, DB outage and recovery

## Verification
Run `mvn --batch-mode verify`. CI status is shown by the badge above. Infrastructure and connector runtime behavior must be validated with Docker-based integration tests before claiming production readiness.

See [Operations](docs/operations.md), [Architecture](docs/architecture.md) and [Contributing](CONTRIBUTING.md).