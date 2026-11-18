# Architecture

## Module boundaries

- `connect-common`: reusable retry policy and cross-connector primitives.
- `rest-source-connector`: Kafka Connect source integration for HTTP APIs.
- `jdbc-sink-connector`: Kafka Connect sink integration for relational databases.

Each connector owns its configuration, task lifecycle, and external-system interaction. Shared code must remain independent of connector-specific configuration.

## Reliability model

The toolkit distinguishes transport retries from Kafka delivery semantics. A successful HTTP response is not, by itself, proof that downstream records have been committed by Kafka. Source offsets must be committed by the Connect runtime. Sink writes can be replayed after task restart; database-side idempotency therefore depends on the configured write strategy and target schema.

No exactly-once claim is made for arbitrary HTTP APIs or JDBC targets. Connector documentation must state the actual behavior and failure window for each supported mode.

## Operational requirements

Connector implementations should validate configuration before task startup, bound network and database operations, close resources on stop, expose actionable errors, and avoid logging credentials or record payloads by default.
