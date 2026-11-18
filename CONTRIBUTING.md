# Contributing

## Development requirements

- JDK 17+
- Apache Maven 3.9+
- Docker (required for integration environments as they are introduced)

## Workflow

1. Create a focused branch from `main`.
2. Make a narrowly scoped change.
3. Add or update unit and integration tests.
4. Run `mvn -B verify` from the repository root.
5. Update relevant documentation and configuration examples.

## Quality bar

Changes should preserve clear task lifecycle behavior, validate configuration early, bound retries and resource use, avoid leaking secrets, and document delivery semantics. Do not describe functionality as supported until tests exercise its success and failure paths.
