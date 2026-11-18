# Kafka Connect Toolkit

[![CI](https://github.com/sajuu730/kafka-connect-toolkit/actions/workflows/ci.yml/badge.svg)](https://github.com/sajuu730/kafka-connect-toolkit/actions/workflows/ci.yml)

A Java 17 multi-module project for custom Kafka Connect integrations: an HTTP REST source and a JDBC sink, with reliability primitives, containerized infrastructure and an evolving observability stack.

> **Status: Active development.** MySQL 8.4 is now the primary local database target. The JDBC sink has a MySQL `ON DUPLICATE KEY UPDATE` SQL path and the local Compose stack includes Kafka, Schema Registry, Kafka Connect, MySQL, Prometheus, a JMX exporter and Grafana. This is not a production-readiness claim: incremental source semantics, per-record DLQ reporting, schema evolution and full end-to-end fault-recovery verification remain under development.

## Current implementation

### REST source
- Java `HttpClient` polling with bounded connect/request timeouts
- JSON object/array decoding and page-number pagination with a maximum-page guard
- Stable record IDs emitted as Kafka Connect source offsets
- Reads the saved source offset on task startup and resumes after that ID when it remains in the fetched snapshot
- Capped exponential retry with jitter for transient HTTP and I/O failures
- HTTP 429/503 throttling honors `Retry-After` delta-seconds or HTTP-date headers (bounded to five minutes), falling back to exponential backoff when absent or invalid
- Optional bearer-token authentication

**Limitations:** the current checkpoint is based on a stable ID in a deterministic snapshot. It is not yet a general cursor/next-page-token implementation; disappearing IDs can cause snapshot replay. Robust API cursor/next-page-token strategies and strict monotonic checkpoint contracts remain roadmap work.

### JDBC sink
- `Struct` records written with prepared statements
- Configurable JDBC batching and transaction-per-`put` behavior
- MySQL-specific `INSERT ... ON DUPLICATE KEY UPDATE` when the JDBC URL is `jdbc:mysql:`
- PostgreSQL/H2-compatible `ON CONFLICT` path retained for existing tests
- Bounded retry for transient SQL execution errors; ambiguous commit failures fail the task rather than retrying blindly

**Limitations:** schema evolution and `ErrantRecordReporter`-based record-level DLQ reporting are not implemented yet. Schema changes within a task currently fail explicitly. Worker-level Kafka Connect DLQ configuration is not equivalent to connector-managed reporting of malformed records.

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
- Worker DLQ settings are configured locally, but record-level `ErrantRecordReporter` support is still planned.
- Delivery should be treated as at-least-once; design keys and upsert semantics for idempotency.

## Roadmap
- [x] Java 17 Maven modules and shared bounded retry policy
- [x] REST polling, bearer auth, page-number pagination and source offsets
- [x] JDBC batching and transaction handling
- [x] MySQL driver and MySQL upsert SQL dialect
- [x] Local Kafka, Schema Registry, Kafka Connect and MySQL Compose services
- [x] Prometheus, JMX exporter and provisioned Grafana dashboard scaffolding
- [x] HTTP 429/503 retry handling with `Retry-After` parsing
- [ ] Robust incremental cursor/checkpoint strategies and strict ordering guarantees
- [ ] MySQL Testcontainers integration and Kafka/Schema Registry end-to-end tests
- [ ] Schema evolution policy and safe DDL handling
- [ ] ErrantRecordReporter-based malformed-record DLQ routing
- [ ] Validate JMX exporter mappings and dashboard metrics against a live stack
- [ ] Fault-injection tests for task restart, rebalance, DB outage and recovery

## Verification
Run `mvn --batch-mode verify`. CI status is shown by the badge above. Infrastructure and connector runtime behavior must be validated with Docker-based integration tests before claiming production readiness.

See [Operations](docs/operations.md), [Architecture](docs/architecture.md) and [Contributing](CONTRIBUTING.md).