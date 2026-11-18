# Demo capture guide

Capture evidence from the real local stack after following the README quick start. Do not use mockups as proof of a running integration.

## Recommended screenshots

1. Connector inventory: GET /connector-plugins showing the custom REST source and JDBC sink plugins.
2. Pipeline result: one record from the REST source visible in Kafka and the matching row in MySQL.
3. DLQ behavior: a deliberately malformed record visible in the configured dead-letter topic.
4. Grafana: the provisioned dashboard populated with live JMX/Prometheus metrics.
5. GitHub Actions: both verify and end-to-end jobs green for the same commit.

Suggested filenames under docs/assets/: toolkit-connectors.png, toolkit-pipeline.png, toolkit-dlq.png, toolkit-grafana.png, toolkit-ci.png.

## Short walkthrough

Record 60–90 seconds: introduce the architecture, show the running connectors, demonstrate one record flowing to MySQL, show the malformed-record DLQ route, and finish with the Grafana dashboard and green CI run.

Hide credentials, bearer tokens, database passwords, private endpoints and personal information before publishing. Show the actual local or test environment and label it accurately; do not imply production deployment.
