# Operations

## Local development
Build the shaded connector plugins before starting the worker so the JARs exist in each module's `target/` directory:
```bash
mvn --batch-mode clean package
docker compose up -d
docker compose ps
curl http://localhost:8083/connector-plugins

# Exercise REST -> Kafka/Avro/Schema Registry -> MySQL, DLQ routing, rebalances, MySQL recovery and monitoring
bash scripts/e2e-smoke.sh
```
Local services: Kafka `localhost:9092`, Schema Registry `http://localhost:8081`, Kafka Connect `http://localhost:8083`, MySQL `localhost:3306`, Prometheus `http://localhost:9090`, Grafana `http://localhost:3000`, and JMX exporter `http://localhost:5556/metrics`.

The Compose environment uses plaintext listeners and demo credentials; it is strictly for local development. After changing connector code, rebuild and restart the worker:
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
| `rest.pagination.page.parameter` | empty | Query parameter name for page-number pagination |
| `rest.pagination.start.page` | `1` | First page number |
| `rest.pagination.max.pages` | `100` | Hard maximum pages fetched per poll |
| `rest.incremental.cursor.parameter` | empty | Optional query parameter populated with the last committed record ID |
| `poll.interval.ms` | `5000` | Delay when no new records are available |
| `retry.max.attempts` | `3` | Total attempts for a request |
| `retry.initial.backoff.ms` | `500` | Initial capped exponential retry delay |

The source task reads the saved ID offset during startup and filters out records at or below that ID. Numeric IDs use numeric comparison; other IDs use lexical comparison. When `rest.incremental.cursor.parameter` is set, the last committed ID is also sent to the API as a query parameter. The source emits typed Kafka Connect `Struct` values: primitive JSON values retain their scalar types, while nested objects and arrays are encoded as compact JSON strings. This supports schema-aware converters such as AvroConverter.

This checkpoint contract requires stable, unique, monotonically ascending IDs. It does not implement opaque cursors, arbitrary next-page tokens/links, compound keys or out-of-order API semantics. The optional query cursor must match the remote API's contract. HTTP 429/503 responses honor `Retry-After` delta-seconds or HTTP-date values (capped at five minutes); other transient HTTP/I/O errors use bounded exponential backoff.

## JDBC sink configuration
| Property | Default | Meaning |
|---|---:|---|
| `connection.url` | required | JDBC URL; `jdbc:mysql:` selects MySQL SQL generation |
| `connection.user` | empty | Database username |
| `connection.password` | required | Database password |
| `table.name` | required | Target table or schema-qualified table |
| `batch.size` | `500` | Maximum statements per JDBC execution batch |
| `insert.mode` | `insert` | `insert` or `upsert` |
| `pk.fields` | empty | Comma-separated key fields required for upsert |
| `retry.max.attempts` | `3` | Attempts for transient SQL execution failures |
| `retry.initial.backoff.ms` | `500` | Initial capped exponential retry delay |

For MySQL URLs, `insert.mode=upsert` generates `INSERT ... ON DUPLICATE KEY UPDATE`. MySQL uses the table's PRIMARY KEY or UNIQUE index to identify a duplicate; `pk.fields` is validated against the incoming Struct fields but does not appear as a MySQL conflict target. PostgreSQL/H2 test URLs retain the `ON CONFLICT` branch. The target table should exist before connector startup. When new Struct fields appear, the sink performs additive-only schema evolution by adding nullable columns; it never alters existing column types or drops columns. Review database DDL permissions and schema policy before production use. A DDL/schema failure is treated as an infrastructure failure and fails the task rather than routing valid data to the DLQ.

## Dead-letter queue
Set these **sink connector configuration** properties: `errors.tolerance=all`, `errors.deadletterqueue.topic.name=connect-dlq`, `errors.deadletterqueue.topic.replication.factor=1` for the single-broker local stack, and optionally `errors.deadletterqueue.context.headers.enable=true`. The JDBC sink invokes Kafka Connect `ErrantRecordReporter` for malformed non-Struct and empty-Struct records, waits for the report future, and increments the toolkit DLQ JMX counter after a successful report. The worker must supply the reporter by enabling DLQ/error handling. Database outages, DDL failures and other task-level infrastructure failures fail/retry the task; they are not mislabeled as bad individual records. Review DLQ topic security, retention, replication and payload sensitivity before production use.

## Observability
The Compose stack includes a JMX exporter endpoint, Prometheus scrape configuration and provisioned Grafana dashboard. Prometheus loads alert rules for task failures, DLQ routing and sustained sink lag. These rules create alert states in Prometheus; configure Alertmanager separately if notifications are required. The JMX exporter configuration maps toolkit MBeans to `kct_records_processed_total`, `kct_batches_completed_total`, `kct_http_requests_total`, `kct_retries_total`, `kct_failures_total`, `kct_dlq_records_routed_total` and `kct_last_success_time_epoch_ms`. It also maps Kafka Connect's `sink-record-lag-max` attribute to `kafka_connect_connector_task_sink_record_lag_max`. Grafana panels use these explicit names. Verify `/metrics` contains them and confirm Prometheus targets are UP after starting the stack; metric names and scrape behavior still require a successful live CI run.

## Production considerations
Do not deploy local Compose credentials or plaintext listeners to production. Use TLS/SASL, secret/config providers, replicated internal Connect topics, suitable replication factors, monitored DLQ retention and a managed database connection policy. The current project is under active development and is not yet certified production-ready.