#!/usr/bin/env bash
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
MYSQL_CONTAINER="${MYSQL_CONTAINER:-$(docker compose ps -q mysql)}"
MYSQL_USER="${MYSQL_USER:-connect}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-connect_local_only}"
MYSQL_DATABASE="${MYSQL_DATABASE:-connect_demo}"

wait_for() {
  local description="$1"
  local command="$2"
  for _ in $(seq 1 90); do
    if eval "$command" >/dev/null 2>&1; then
      echo "ready: $description"
      return 0
    fi
    sleep 2
  done
  echo "timed out waiting for $description" >&2
  return 1
}

wait_for "Kafka Connect REST API" "curl -fsS '$CONNECT_URL/'"
wait_for "REST source plugin" "curl -fsS '$CONNECT_URL/connector-plugins' | grep -q 'io.github.sajuu730.connect.rest.RestSourceConnector'"
wait_for "JDBC sink plugin" "curl -fsS '$CONNECT_URL/connector-plugins' | grep -q 'io.github.sajuu730.connect.jdbc.JdbcSinkConnector'"

curl -fsS -X PUT "$CONNECT_URL/connectors/kct-e2e-source/config" \
  -H 'Content-Type: application/json' \
  -d '{
    "connector.class":"io.github.sajuu730.connect.rest.RestSourceConnector",
    "tasks.max":"1",
    "rest.endpoint":"http://mock-api:1080/events",
    "kafka.topic":"kct-e2e-events",
    "poll.interval.ms":"500",
    "retry.max.attempts":"3",
    "retry.initial.backoff.ms":"100"
  }' >/dev/null

curl -fsS -X PUT "$CONNECT_URL/connectors/kct-e2e-sink/config" \
  -H 'Content-Type: application/json' \
  -d '{
    "connector.class":"io.github.sajuu730.connect.jdbc.JdbcSinkConnector",
    "tasks.max":"1",
    "topics":"kct-e2e-events",
    "connection.url":"jdbc:mysql://mysql:3306/connect_demo",
    "connection.user":"connect",
    "connection.password":"connect_local_only",
    "table.name":"events",
    "insert.mode":"upsert",
    "pk.fields":"id",
    "batch.size":"2"
  }' >/dev/null

wait_for "source and sink connectors RUNNING" "curl -fsS '$CONNECT_URL/connectors/kct-e2e-source/status' | grep -q 'RUNNING' && curl -fsS '$CONNECT_URL/connectors/kct-e2e-sink/status' | grep -q 'RUNNING'"

wait_for "record delivered to MySQL" "docker exec '$MYSQL_CONTAINER' mysql -N -u'$MYSQL_USER' -p'$MYSQL_PASSWORD' '$MYSQL_DATABASE' -e \"SELECT id FROM events WHERE id='e2e-1'\" | grep -q '^e2e-1$'"

# Allow the worker to flush source offsets before restarting it.
sleep 3
docker compose restart connect >/dev/null
wait_for "Connect worker after restart" "curl -fsS '$CONNECT_URL/'"
wait_for "connectors RUNNING after restart" "curl -fsS '$CONNECT_URL/connectors/kct-e2e-source/status' | grep -q 'RUNNING' && curl -fsS '$CONNECT_URL/connectors/kct-e2e-sink/status' | grep -q 'RUNNING'"
wait_for "idempotent row remains after worker restart" "docker exec '$MYSQL_CONTAINER' mysql -N -u'$MYSQL_USER' -p'$MYSQL_PASSWORD' '$MYSQL_DATABASE' -e \"SELECT COUNT(*) FROM events WHERE id='e2e-1'\" | grep -q '^1$'"

echo "E2E smoke test passed: REST -> Kafka/Avro/Schema Registry -> JDBC/MySQL, including Connect worker restart."
