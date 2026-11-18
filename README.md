# Kafka Connect Toolkit

A modular toolkit for building, operating, and testing reliable Apache Kafka Connect integrations.

> **Project status:** Initial implementation in progress. The repository currently establishes the project scope and engineering standards; connector capabilities will be marked complete only after implementation and automated verification.

## Objectives

Kafka Connect deployments often repeat the same reliability and operational concerns across integrations. This project aims to provide reusable connector components and operational patterns for two common integration directions:

- **REST Source Connector** — ingest records from HTTP APIs into Kafka, with configurable polling, pagination, request controls, and source offsets.
- **JDBC Sink Connector** — deliver Kafka records to relational databases using batched writes, prepared statements, and configurable insert/upsert behavior.

Shared capabilities are planned around configuration validation, retry and backoff policies, dead-letter handling, structured diagnostics, metrics, and lifecycle management.

## Design principles

- Explicit delivery guarantees and documented failure semantics
- Safe configuration defaults and fail-fast validation
- Bounded retries, timeouts, and resource usage
- Testable components with clear separation of concerns
- Observable operation through metrics and structured logs
- Reproducible local development and CI

## Target technology

| Area | Technology |
|---|---|
| Language | Java |
| Connector API | Apache Kafka Connect |
| Build | Apache Maven |
| Tests | JUnit 5, Testcontainers |
| Local environment | Docker Compose |
| CI | GitHub Actions |
| Sink database | PostgreSQL |

Exact dependency versions and compatibility targets will be pinned in the build as implementation lands.

## Repository layout

The intended structure is:

```text
kafka-connect-toolkit/
├── connectors/
│   ├── rest-source/
│   └── jdbc-sink/
├── common/
├── integration-tests/
├── config/
├── docs/
└── .github/workflows/
```

The layout may evolve as the implementation is validated.

## Getting started

The first implementation milestone will provide a reproducible local Kafka Connect environment and working connector examples. Until that milestone is merged, there is no supported quick-start command.

## Engineering roadmap

- [ ] Define module boundaries and supported Kafka/Connect versions
- [ ] Implement shared configuration and error-handling primitives
- [ ] Implement REST Source connector with offset management
- [ ] Implement JDBC Sink connector with batching and safe SQL handling
- [ ] Add unit and integration coverage
- [ ] Add Docker-based local environment
- [ ] Add metrics, operational documentation, and CI checks
- [ ] Document delivery semantics, limitations, and production deployment guidance

## Contributing

Contributions should include tests and documentation for behavior changes. See [CONTRIBUTING.md](CONTRIBUTING.md) once the contributor workflow is added.

## License

A license will be added before the project is distributed for reuse.
