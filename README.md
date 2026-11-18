# Kafka Connect Toolkit

[![CI](https://github.com/sajuu730/kafka-connect-toolkit/actions/workflows/ci.yml/badge.svg)](https://github.com/sajuu730/kafka-connect-toolkit/actions/workflows/ci.yml)

A Java-based toolkit for operating two reusable Kafka Connect integrations: an HTTP REST source and a relational JDBC sink. The repository is structured as independently packaged connector plugins with shared reliability primitives.

> **Status:** Active development. The Maven reactor build and all 7 current unit tests passed in GitHub Actions. Connector runtime behavior in a real Kafka Connect + PostgreSQL deployment is not yet covered by end-to-end tests, so this is not yet a production-readiness claim.

## Capabilities

### REST Source Connector

- Polls an HTTP endpoint returning a JSON object or array
- Emits JSON payloads as Kafka string values
- Stores a stable record identifier as the Kafka Connect source offset
- Resumes after the last committed identifier when that identifier remains in the API snapshot
- Retries HTTP 429 and 5xx responses and I/O failures with capped exponential backoff and jitter
- Supports optional bearer-token authentication without logging token values
- Uses bounded request and connection timeouts

**Current limitations:** no authentication, pagination, incremental cursor support, rate-limit parsing, or custom field mapping. If the saved ID disappears from the returned snapshot, the task replays the snapshot. The API must provide stable, unique IDs and a deterministic order for meaningful resume behavior.

### JDBC Sink Connector

- Writes Kafka Connect `Struct` values to a relational table
- Uses prepared statements for record values and validates/quotes table identifiers
- Executes bounded JDBC batches within a transaction per `put` call
- Rolls back failed batches and retries transient SQL execution failures with capped exponential backoff
- Treats commit failures as ambiguous and delegates recovery to Kafka Connect's task retry behavior

**Current limitations:** INSERT-only; no upsert, schema evolution, deletes, or automatic table creation. The destination table must already exist and match the incoming Struct fields. At-least-once replay can create duplicate-key failures unless the target schema and upstream processing are designed for idempotency.

### Shared engineering

- Java 17 and Maven multi-module build
- Shared bounded exponential retry policy
- Per-task JMX counters for records, batches, requests, retries, failures, and last successful operation
- JUnit 5 tests
- Shaded plugin JARs with Kafka Connect service registration
- Docker Compose environment for Kafka, Kafka Connect, and PostgreSQL
- GitHub Actions verification workflow

## Technology

| Area | Technology |
|---|---|
| Runtime | Java 17 |
| Streaming | Apache Kafka Connect API 3.8.1 |
| Build | Apache Maven |
| Tests | JUnit 5, Mockito, Testcontainers |
| Database | PostgreSQL 16 |
| Local environment | Docker Compose |
| CI | GitHub Actions |

## Repository layout

```text
kafka-connect-toolkit/
├── common/                         # shared retry and reliability primitives
├── connectors/
│   ├── rest-source/                # HTTP -> Kafka source plugin
│   └── jdbc-sink/                  # Kafka -> JDBC sink plugin
├── config/                         # local database initialization
├── docs/                           # architecture and operations
├── docker-compose.yml
└── .github/workflows/ci.yml
```

## Quick start

Prerequisites: JDK 17+, Maven 3.9+, and Docker with the Compose plugin.

1. Build the connector plugin JARs:

   ```bash
   mvn --batch-mode clean package
   ```

2. Start the local Kafka Connect and PostgreSQL environment:

   ```bash
   docker compose up -d
   ```

3. Check that Kafka Connect is responding and discovers the plugins:

   ```bash
   curl http://localhost:8083/connector-plugins
   ```

The shaded plugin JARs are mounted from each module's `target/` directory. Rebuild and restart the Connect worker after changing connector code.

## Connector configuration

### REST Source

POST a connector configuration to `http://localhost:8083/connectors`:

```json
{
  "name": "rest-events-source",
  "config": {
    "connector.class": "io.github.sajuu730.connect.rest.RestSourceConnector",
    "tasks.max": "1",
    "rest.endpoint": "https://api.example.com/events",
    "kafka.topic": "rest-events",
    "record.id.field": "id",
    "rest.auth.bearer.token": "REPLACE_WITH_SECRET_IF_REQUIRED",
    "poll.interval.ms": "5000",
    "retry.max.attempts": "3",
    "retry.initial.backoff.ms": "500"
  }
}
```

The API endpoint must return an object or array of objects, each with a stable `id` field (or the configured `record.id.field`). The bearer-token property is optional; use a Kafka Connect secret/config provider for production secrets and never commit real tokens.

### JDBC Sink

The local Compose environment creates `public.events`. Configure the sink with a password supplied securely in your environment; do not commit real credentials:

```json
{
  "name": "postgres-events-sink",
  "config": {
    "connector.class": "io.github.sajuu730.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "connection.url": "jdbc:postgresql://postgres:5432/connect_demo",
    "connection.user": "connect",
    "connection.password": "REPLACE_WITH_LOCAL_PASSWORD",
    "table.name": "public.events",
    "batch.size": "500",
    "retry.max.attempts": "3",
    "retry.initial.backoff.ms": "500"
  }
}
```

The sink expects Kafka Connect JSON records with schemas enabled and a `Struct` value whose field names match the table columns. The local worker is configured with JSON converters and schema support.

## Reliability and error handling

- Source offsets are emitted with each record; Kafka Connect controls durable offset commits. If the API does not retain the saved ID, the current snapshot is replayed.
- The sink commits each successful `put` batch in one JDBC transaction. A failed SQL execution is rolled back before a retry.
- A connection failure during commit can have an ambiguous outcome; the task fails rather than blindly replaying inside the connector.
- The Compose worker enables Kafka Connect's worker-level error tolerance and dead-letter queue settings. The DLQ is for supported sink record/converter processing failures; it is not a substitute for handling source endpoint failures or task-level outages.
- Kafka Connect worker and connector metrics are available through the Connect runtime/JMX when enabled by the deployment.

See [Architecture](docs/architecture.md) and [Operations](docs/operations.md) for semantics and deployment notes.

## Verification

**Latest verified CI run:** Maven `verify` passed; 7 tests passed across the common, REST source, and JDBC sink modules. The workflow compiles the modules, executes unit tests plus a Docker-backed PostgreSQL Testcontainers integration test (skipped automatically when Docker is unavailable), and packages both shaded connector plugins.

Run the full unit-test and packaging lifecycle:

```bash
mvn --batch-mode verify
```

CI runs this command on pushes and pull requests to `main`. Integration tests against a live Kafka worker and PostgreSQL are being expanded; passing unit tests alone is not a production-readiness claim.

## Engineering roadmap

- [x] Establish Java 17 Maven modules and shared retry policy
- [x] Implement initial REST source polling, bearer authentication, and acknowledged source offsets
- [x] Implement initial JDBC sink batching and transactional writes
- [x] Add plugin packaging and service registration
- [x] Add Docker Compose development environment and worker DLQ configuration
- [ ] Add REST authentication, pagination, and incremental cursor strategies
- [ ] Add configurable JDBC upsert and schema evolution
- [x] Add a PostgreSQL Testcontainers integration test for JDBC sink writes
- [ ] Add end-to-end tests against a running Kafka Connect worker and PostgreSQL
- [x] Add per-task JMX counters and lifecycle cleanup
- [ ] Add structured diagnostic logging and end-to-end operational dashboards
- [ ] Verify supported Kafka/Connect versions and document production deployment

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Changes should include tests, documentation, and explicit failure semantics.
