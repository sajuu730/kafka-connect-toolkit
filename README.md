# Kafka Connect Toolkit

[![CI](https://github.com/sajuu730/kafka-connect-toolkit/actions/workflows/ci.yml/badge.svg)](https://github.com/sajuu730/kafka-connect-toolkit/actions/workflows/ci.yml)
[![Java](https://img.shields.io/badge/Java-17-orange)](https://openjdk.org/projects/jdk/17/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.8.1-black)](https://kafka.apache.org/)

A Java 17 multi-module project demonstrating custom Kafka Connect integrations: an HTTP REST source connector and a JDBC sink connector, supported by retry primitives, containerized infrastructure, and an observability stack.

> **Portfolio project:** an independently developed engineering demonstration using local infrastructure and synthetic data. It is not affiliated with, or connected to, an employer, customer environment or proprietary system.
>
> **Project status: Active development.** The latest completed CI run passed Maven verification and the end-to-end smoke-test job. Later documentation commits have triggered a fresh CI run; check the Actions page for its final result. This validates only the scenarios exercised by CI, not production readiness. The REST source checkpoint contract remains limited to stable, monotonically ascending IDs.

## Highlights

- Custom REST source and JDBC sink connectors built on the Kafka Connect APIs.
- Incremental source checkpointing using Kafka Connect offsets.
- Bounded retries, exponential backoff, jitter, and HTTP Retry-After handling.
- JDBC batching, transactions, and MySQL upsert support.
- Malformed-record reporting through Kafka Connect's ErrantRecordReporter.
- Docker Compose environment with Kafka, Schema Registry, Kafka Connect, MySQL, Prometheus, JMX Exporter, and Grafana.
- CI verification plus a Docker-based end-to-end smoke test.

## Architecture

```mermaid
flowchart LR
    API[External REST API] --> SRC[REST Source Connector]
    SRC -->|Connect records + offsets| K[Kafka]
    K --> SINK[JDBC Sink Connector]
    SINK --> DB[(MySQL)]
    SINK -. malformed records .-> DLQ[(DLQ topic)]
    SRC -. retry / metrics .-> OBS[JMX Exporter]
    SINK -. retry / metrics .-> OBS
    OBS --> PROM[Prometheus]
    PROM --> GRAF[Grafana]
    SR[Schema Registry] --- K
```

## Implemented connectors

### REST source

- Java HttpClient polling with bounded connect/request timeouts.
- JSON object/array decoding and page-number pagination with a maximum-page guard.
- Converts JSON objects into typed Kafka Connect Struct values; primitive scalar types are retained, while nested objects/arrays are compact JSON strings.
- Emits stable record IDs as Kafka Connect source offsets and reads saved offsets on task startup.
- Filters records at or below the last committed ID (numeric comparison when possible, lexical otherwise).
- Optional incremental cursor query parameter for APIs that accept the last committed ID.
- Capped exponential retry with jitter for transient HTTP and I/O failures.
- HTTP 429/503 handling honors Retry-After delta-seconds or HTTP-date values, bounded to five minutes.
- Optional bearer-token authentication.

**REST source limitations:** checkpointing assumes stable, unique, monotonically ascending IDs. Opaque cursors, arbitrary next-page links/tokens, compound keys, and out-of-order APIs are not supported. Configure the cursor query parameter only when the remote API accepts the last ID as its cursor.

### JDBC sink

- Writes Kafka Connect Struct records with prepared statements.
- Configurable JDBC batching and transaction-per-put behavior.
- MySQL INSERT ... ON DUPLICATE KEY UPDATE upsert path.
- PostgreSQL/H2-compatible ON CONFLICT path retained for tests.
- Bounded retry for transient SQL execution errors; ambiguous commit failures fail the task rather than retrying blindly.
- Additive-only schema evolution adds nullable columns for newly observed fields; existing column types are not altered.
- Uses ErrantRecordReporter for malformed non-Struct/empty records when the worker supplies a DLQ reporter.

**JDBC sink limitations:** schema evolution is additive-only and database-specific DDL must be reviewed before production use. Schema/DDL failures are infrastructure failures and fail the task. Configure sink connector-level errors.tolerance=all and errors.deadletterqueue.topic.name to enable routing through the worker reporter.

## Technology

Java 17 · Apache Kafka Connect 3.8.1 · Avro/Schema Registry · Maven · JUnit 5 · Testcontainers · MySQL 8.4 · Docker Compose · Prometheus · Grafana

## Quick start

Requirements: JDK 17+, Maven 3.9+, Docker Engine, and Docker Compose plugin.

```bash
mvn --batch-mode clean package
docker compose up -d
docker compose ps
curl http://localhost:8083/connector-plugins
```

Local endpoints:

| Service | Local address |
|---|---|
| Kafka | localhost:9092 |
| Schema Registry | http://localhost:8081 |
| Kafka Connect REST | http://localhost:8083 |
| MySQL | localhost:3306 (database: connect_demo) |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 |
| JMX Exporter | http://localhost:5556/metrics |

The Compose stack uses plaintext listeners and demo credentials and is intended for local development only. Build the connector JARs before starting Kafka Connect because the worker mounts module target directories.

## CI and verification

The GitHub Actions workflow runs Maven verification and Docker-based end-to-end smoke tests covering the scenarios encoded in scripts/e2e-smoke.sh.

[View GitHub Actions runs](https://github.com/sajuu730/kafka-connect-toolkit/actions) · [Demo and screenshot capture guide](docs/demo-capture.md)

## Documentation

- [Architecture](docs/architecture.md)
- [Operations guide](docs/operations.md)
- [Demo capture guide](docs/demo-capture.md)
- [Contributing](CONTRIBUTING.md)

## Production considerations

Do not deploy local Compose credentials or plaintext listeners to production. Use TLS/SASL, secret/config providers, replicated internal Connect topics, suitable replication factors, monitored DLQ retention, and an appropriate database connection policy. This project is an engineering demonstration under active development, not a certified production distribution.

## License

MIT. See LICENSE.
