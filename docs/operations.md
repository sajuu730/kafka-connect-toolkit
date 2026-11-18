# Operations

## Local development
Build the shaded connector plugins before starting the worker so the JARs exist in each module's `target/` directory:
```bash
mvn --batch-mode clean package
docker compose up -d
docker compose ps
curl http://localhost:8083/connector-plugins
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
| `poll.interval.ms` | `5000` | Delay when no new records are available |
| `retry.max.attempts` | `3` | Total attempts for a request |
| `retry.initial.backoff.ms` | `500` | Initial capped exponential retry delay |

The source task reads the saved ID offset during startup and resumes after that ID if it is present in the fetched snapshot. If the ID disappears, the current implementation replays the snapshot. It requires stable unique IDs and deterministic API ordering. General cursor strategies and HTTP `Retry-After` support are not yet implemented.

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

For MySQL URLs, `insert.mode=upsert` generates `INSERT ... ON DUPLICATE KEY UPDATE`. MySQL uses the table's PRIMARY KEY or UNIQUE index to identify a duplicate; `pk.fields` is validated against the incoming Struct fields but does not appear as a MySQL conflict target. PostgreSQL/H2 test URLs retain the `ON CONFLICT` branch. The target table must already exist and match the incoming Struct schema.

Schema changes within a task currently fail explicitly; automatic schema evolution is not implemented. The sink currently requires Kafka Connect `Struct` values. The local Connect worker is configured with Avro for values, so records produced by the REST source (currently string-schema JSON payloads) need a compatible transformation/converter pipeline before they can be consumed by this sink. A complete source-to-sink Avro pipeline is part of the integration-test roadmap.

## Dead-letter queue
The local worker enables `errors.tolerance=all` and `errors.deadletterqueue.topic.name=connect-dlq`. This is worker-level Connect error handling; the connector does not yet invoke `ErrantRecordReporter` for individual malformed records. DLQ behavior does not turn source HTTP outages or task-level failures into individual bad-record messages. Review DLQ topic security, retention, replication and payload sensitivity before production use.

## Observability
The Compose stack includes a JMX exporter endpoint, Prometheus scrape configuration and provisioned Grafana dashboard. Treat the dashboard and metric mappings as scaffolding until validated against a running deployment. The toolkit task metrics expose counters for records, batches, requests, retries, failures and last successful operation when JMX registration is available.

## Production considerations
Do not deploy local Compose credentials or plaintext listeners to production. Use TLS/SASL, secret/config providers, replicated internal Connect topics, suitable replication factors, monitored DLQ retention and a managed database connection policy. The current project is under active development and is not yet certified production-ready.