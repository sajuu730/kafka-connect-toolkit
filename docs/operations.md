# Operations

## Local development

Build the shaded connector plugins before starting the worker so the JARs exist in each module's `target/` directory:

```bash
mvn --batch-mode clean package
docker compose up -d
docker compose ps
curl http://localhost:8083/connector-plugins
```

Kafka is available at `localhost:9092`, Kafka Connect REST at `http://localhost:8083`, and PostgreSQL at `localhost:5432`. The Compose environment uses plaintext listeners and demo credentials; it is strictly for local development.

After changing connector code, rebuild the modules and restart the worker:

```bash
mvn --batch-mode package
docker compose restart connect
```

## REST source configuration

| Property | Default | Meaning |
|---|---:|---|
| `rest.endpoint` | required | HTTP endpoint returning a JSON object or array |
| `kafka.topic` | required | Destination topic |
| `record.id.field` | `id` | Stable identifier field used as the source offset |
| `rest.auth.bearer.token` | empty | Optional bearer token; use a secret/config provider in production |
| `rest.pagination.page.parameter` | empty | Query parameter name for page-number pagination; empty disables pagination |
| `rest.pagination.start.page` | `1` | First page number when pagination is enabled |
| `rest.pagination.max.pages` | `100` | Hard maximum pages fetched per poll; reaching the limit fails the poll rather than silently truncating results |
| `poll.interval.ms` | `5000` | Delay when no new records are available; minimum 100 ms |
| `retry.max.attempts` | `3` | Total attempts for a request |
| `retry.initial.backoff.ms` | `500` | Initial capped exponential retry delay |

Requests have a 10-second connection timeout and 30-second request timeout. HTTP 429, 5xx, and I/O failures are retried; other non-2xx responses fail immediately. Backoff is capped at 30 seconds and includes jitter.

When page-number pagination is enabled, the endpoint must return JSON arrays and an empty array to signal the end of the result set. The configured maximum page count prevents an unbounded poll. The endpoint must return objects with unique, stable IDs in deterministic order. The optional bearer token is sent in the HTTP Authorization header and is never written to application logs. The source task resumes after the last stored ID if it is still present in the snapshot. If it is absent, the task replays the current snapshot. This connector does not yet support authentication, pagination, rate-limit headers, or server-side incremental cursors. The source task emits JSON text using a Kafka Connect string schema.

## JDBC sink configuration

| Property | Default | Meaning |
|---|---:|---|
| `connection.url` | required | JDBC connection URL |
| `connection.user` | empty | Database username |
| `connection.password` | required | Database password |
| `table.name` | required | Target table or schema-qualified table |
| `batch.size` | `500` | Maximum statements per JDBC execution batch |
| `retry.max.attempts` | `3` | Attempts for transient SQL execution failures |
| `retry.initial.backoff.ms` | `500` | Initial capped exponential retry delay |

The sink currently requires `Struct` values and a stable schema across records handled by a task. It uses parameterized INSERT statements and commits a successful `put` call as one transaction. The target table must already exist, and its column names must match the Struct fields.

Only transient SQL execution failures (including connection and transaction SQLState classes) are retried internally after rollback and reconnect. Commit failures are not retried internally because the server may have committed even if the client lost the response. Kafka Connect will handle task failure/retry according to worker policy. The delivery model is at-least-once; duplicates are possible after task restart or ambiguous commits. Upsert, delete, and schema evolution are not implemented.

## Dead-letter queue

The local worker enables Kafka Connect's worker-level DLQ settings:

- `errors.tolerance=all`
- `errors.deadletterqueue.topic.name=connect-dlq`
- `errors.deadletterqueue.context.headers.enable=true`
- `errors.log.enable=true`
- `errors.log.include.messages=false`

DLQ behavior depends on the Connect runtime's error boundaries. It can capture supported sink record/converter processing failures, but it does not turn source HTTP outages or connector task failures into individual DLQ records. In production, review the security, retention, replication, and access controls of the DLQ topic. Keep message logging disabled unless payload exposure has been explicitly assessed.

## Packaging and deployment

Each connector module produces a shaded plugin JAR containing its non-Kafka runtime dependencies and a service registration entry. Kafka Connect APIs are provided by the worker and are not bundled. Deploy each plugin in its own directory under `plugin.path`, then restart the worker and verify discovery with `GET /connector-plugins`.

Do not deploy the local Compose credentials or plaintext listeners to production. Use TLS/SASL, a secret/config provider for credentials, replicated internal Connect topics, suitable replication factors, and a managed database connection policy.

## Observability

Kafka Connect exposes worker and connector/task metrics through its runtime metrics system. In addition, each toolkit task registers a per-instance JMX MBean under the domain `io.github.sajuu730.connect` with counters for `RecordsProcessed`, `BatchesCompleted`, `Requests`, `Retries`, `Failures`, and `LastSuccessTimeEpochMs`. The MBean is unregistered when the task stops. If JMX registration fails, the connector continues and logs a warning. Monitor task state, source record poll rates, sink record write rates, error rates, task restarts, DLQ volume, JDBC connection health, and database latency. Structured diagnostic logging and operational dashboards remain planned work.
