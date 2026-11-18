#!/usr/bin/env bash
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
MYSQL_CONTAINER="${MYSQL_CONTAINER:-$(docker compose ps -q mysql)}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-$(docker compose ps -q kafka)}"
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

connector_tasks_running() {
  local connector="$1"
  curl --connect-timeout 2 --max-time 5 -fsS "$CONNECT_URL/connectors/$connector/status" |
    jq -e '.connector.state == "RUNNING" and (.tasks | length > 0) and all(.tasks[]; .state == "RUNNING")' >/dev/null
}

wait_for "Kafka Connect REST API" "curl --connect-timeout 2 --max-time 5 -fsS '$CONNECT_URL/'"
wait_for "REST source plugin" "curl --connect-timeout 2 --max-time 5 -fsS '$CONNECT_URL/connector-plugins' | grep -q 'io.github.sajuu730.connect.rest.RestSourceConnector'"
wait_for "JDBC sink plugin" "curl --connect-timeout 2 --max-time 5 -fsS '$CONNECT_URL/connector-plugins' | grep -q 'io.github.sajuu730.connect.jdbc.JdbcSinkConnector'"

curl --connect-timeout 2 --max-time 5 -fsS -X PUT "$CONNECT_URL/connectors/kct-e2e-source/config" -H 'Content-Type: application/json' -d @config/e2e-source-connector.json >/dev/null
curl --connect-timeout 2 --max-time 5 -fsS -X PUT "$CONNECT_URL/connectors/kct-e2e-sink/config" -H 'Content-Type: application/json' -d @config/e2e-sink-connector.json >/dev/null

wait_for "source and sink tasks RUNNING" "connector_tasks_running kct-e2e-source && connector_tasks_running kct-e2e-sink"
wait_for "initial REST record delivered to MySQL" "docker exec '$MYSQL_CONTAINER' mysql -N -u'$MYSQL_USER' -p'$MYSQL_PASSWORD' '$MYSQL_DATABASE' -e \"SELECT id FROM events WHERE id='e2e-1'\" | grep -q '^e2e-1$'"

# Route a non-Struct Connect value through ErrantRecordReporter into the DLQ.
curl --connect-timeout 2 --max-time 5 -fsS -X PUT "$CONNECT_URL/connectors/kct-e2e-dlq-sink/config" -H 'Content-Type: application/json' -d @config/e2e-dlq-sink-connector.json >/dev/null
wait_for "DLQ sink task RUNNING" "connector_tasks_running kct-e2e-dlq-sink"
# Keep one consumer attached while the poison record is produced. Recreating a
# consumer on every poll can race topic assignment and makes diagnostics opaque.
DLQ_CAPTURE="$(mktemp)"
DLQ_CONSUMER_PID=""
cleanup_dlq_consumer() {
  if [[ -n "$DLQ_CONSUMER_PID" ]]; then
    kill "$DLQ_CONSUMER_PID" >/dev/null 2>&1 || true
    wait "$DLQ_CONSUMER_PID" >/dev/null 2>&1 || true
  fi
  rm -f "$DLQ_CAPTURE"
}
trap cleanup_dlq_consumer EXIT

docker exec "$KAFKA_CONTAINER" kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic connect-dlq --from-beginning --property print.headers=true --timeout-ms 180000 \
  >"$DLQ_CAPTURE" 2>&1 &
DLQ_CONSUMER_PID=$!
sleep 2
printf '%s\n' '{"schema":{"type":"string","optional":false},"payload":"not-a-struct"}' | docker exec -i "$KAFKA_CONTAINER" kafka-console-producer --bootstrap-server kafka:29092 --topic kct-dlq-test
if ! wait_for "malformed record delivered to DLQ" "grep -q 'not-a-struct' '$DLQ_CAPTURE'"; then
  echo "DLQ consumer output:" >&2
  cat "$DLQ_CAPTURE" >&2
  exit 1
fi
kill "$DLQ_CONSUMER_PID" >/dev/null 2>&1 || true
wait "$DLQ_CONSUMER_PID" >/dev/null 2>&1 || true
DLQ_CONSUMER_PID=""


wait_for "JMX exporter metrics endpoint" "curl --connect-timeout 2 --max-time 5 -fsS http://localhost:5556/metrics"
wait_for "toolkit records metric exposed" "curl --connect-timeout 2 --max-time 5 -fsS http://localhost:5556/metrics | grep -q 'kct_records_processed_total'"
wait_for "DLQ routing counter incremented" "curl --connect-timeout 2 --max-time 5 -fsS http://localhost:5556/metrics | awk '\$1 ~ /^kct_dlq_records_routed_total/ && \$NF > 0 {found=1} END {exit !found}'"
wait_for "Prometheus ready" "curl --connect-timeout 2 --max-time 5 -fsS http://localhost:9090/-/ready"
wait_for "Prometheus alert rules loaded" "curl --connect-timeout 2 --max-time 5 -fsS http://localhost:9090/api/v1/rules | grep -q KctTaskFailures"
wait_for "Prometheus scrape target UP" 'curl --connect-timeout 2 --max-time 5 -fsS "http://localhost:9090/api/v1/query?query=up%7Bjob%3D%22kafka-connect-jmx%22%7D" | jq -e ".data.result[0].value[1] == \"1\"" >/dev/null'
wait_for "Prometheus sees toolkit metric" 'curl --connect-timeout 2 --max-time 5 -fsS "http://localhost:9090/api/v1/query?query=kct_records_processed_total" | grep -q "kct_records_processed_total"'
wait_for "Grafana dashboard provisioned" "curl --connect-timeout 2 --max-time 5 -fsS http://localhost:3000/api/dashboards/uid/kct-overview"
wait_for "REST source offset persisted in Connect" "curl --connect-timeout 2 --max-time 5 -fsS '$CONNECT_URL/connectors/kct-e2e-source/offsets' | grep -q 'e2e-1'"

# Join and remove a second worker to force distributed Connect rebalances.
docker compose --profile rebalance up -d connect-worker-2 >/dev/null
wait_for "second Connect worker REST API" "curl --connect-timeout 2 --max-time 5 -fsS http://localhost:8084/"
wait_for "connectors RUNNING after worker join" "connector_tasks_running kct-e2e-source && connector_tasks_running kct-e2e-sink"
docker compose stop connect-worker-2 >/dev/null
wait_for "connectors RUNNING after worker leave" "connector_tasks_running kct-e2e-source && connector_tasks_running kct-e2e-sink"

# Stop MySQL while a new source record is produced; then restart the sink task.
docker compose stop mysql >/dev/null
curl --connect-timeout 2 --max-time 5 -fsS -X PUT "$CONNECT_URL/connectors/kct-recovery-source/config" -H 'Content-Type: application/json' -d @config/e2e-recovery-source-connector.json >/dev/null
wait_for "recovery source RUNNING" "curl --connect-timeout 2 --max-time 5 -fsS '$CONNECT_URL/connectors/kct-recovery-source/status' | grep -q 'RUNNING'"
wait_for "sink task failure during MySQL outage" 'curl --connect-timeout 2 --max-time 5 -fsS "$CONNECT_URL/connectors/kct-e2e-sink/status" | jq -e "any(.tasks[]; .state == \"FAILED\")" >/dev/null'

docker compose start mysql >/dev/null
wait_for "MySQL recovery" "docker exec '$MYSQL_CONTAINER' mysqladmin ping -h 127.0.0.1 -u root -proot_local_only"
curl --connect-timeout 2 --max-time 5 -fsS -X POST "$CONNECT_URL/connectors/kct-e2e-sink/tasks/0/restart" >/dev/null
wait_for "retained Kafka record delivered after MySQL recovery" "docker exec '$MYSQL_CONTAINER' mysql -N -u'$MYSQL_USER' -p'$MYSQL_PASSWORD' '$MYSQL_DATABASE' -e \"SELECT id FROM events WHERE id='e2e-2'\" | grep -q '^e2e-2$'"

# Allow source offsets to flush, then restart the primary Connect worker.
sleep 3
docker compose restart connect >/dev/null
wait_for "Connect worker REST API after restart" "curl --connect-timeout 2 --max-time 5 -fsS '$CONNECT_URL/'"
wait_for "connectors RUNNING after worker restart" "connector_tasks_running kct-e2e-source && connector_tasks_running kct-e2e-sink"
wait_for "source offset survives Connect restart" "curl --connect-timeout 2 --max-time 5 -fsS '$CONNECT_URL/connectors/kct-e2e-source/offsets' | grep -q 'e2e-1'"
wait_for "both records remain in MySQL after restart" "docker exec '$MYSQL_CONTAINER' mysql -N -u'$MYSQL_USER' -p'$MYSQL_PASSWORD' '$MYSQL_DATABASE' -e \"SELECT COUNT(*) FROM events WHERE id IN ('e2e-1','e2e-2')\" | grep -q '^2$'"

echo "E2E smoke test passed: REST -> Kafka/Avro/Schema Registry -> MySQL, DLQ routing, offset persistence, worker rebalance, MySQL outage recovery, JMX, Prometheus and Grafana."
