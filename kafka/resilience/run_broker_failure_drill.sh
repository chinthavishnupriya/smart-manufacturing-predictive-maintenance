#!/usr/bin/env bash
# Isolated Kafka 3-broker drill. Does not touch kafka/docker-compose.yml.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
PROJECT="smart-manufacturing-resilience"
compose() { docker compose -p "$PROJECT" -f "$COMPOSE_FILE" "$@"; }

command -v docker >/dev/null 2>&1 || { echo "ERROR: Docker CLI is required." >&2; exit 1; }
docker compose version >/dev/null 2>&1 || { echo "ERROR: Docker Compose v2 is required." >&2; exit 1; }

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
TOPIC="resilience-drill-${RUN_ID//[^a-zA-Z0-9._-]/-}"
EXPECTED_FILE="$(mktemp)"
ACTUAL_FILE="$(mktemp)"
BATCH_FILE="$(mktemp)"
BROKER1_STOPPED=0
cleanup() {
  if [[ "$BROKER1_STOPPED" -eq 1 ]]; then
    echo "Cleanup: attempting to restart broker 1..." >&2
    compose start kafka-1 >/dev/null 2>&1 || true
  fi
  rm -f "$EXPECTED_FILE" "$ACTUAL_FILE" "$BATCH_FILE"
}
trap cleanup EXIT

wait_for_kafka() {
  local container="$1" bootstrap="$2" tries=0
  until docker exec "$container" /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$bootstrap" --list >/dev/null 2>&1; do
    tries=$((tries + 1))
    if [[ "$tries" -ge 60 ]]; then
      echo "ERROR: Kafka did not become ready using $container/$bootstrap" >&2
      compose ps >&2 || true
      exit 1
    fi
    sleep 2
  done
}

produce_messages() {
  local container="$1" bootstrap="$2" phase="$3" count="$4"
  : > "$BATCH_FILE"
  for ((i=1; i<=count; i++)); do
    printf '%s-%s-%03d\n' "$RUN_ID" "$phase" "$i" >> "$BATCH_FILE"
  done
  cat "$BATCH_FILE" >> "$EXPECTED_FILE"
  docker exec -i "$container" /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server "$bootstrap" \
    --topic "$TOPIC" \
    --producer-property acks=all \
    --producer-property enable.idempotence=true \
    --producer-property retries=2147483647 < "$BATCH_FILE"
  echo "Produced $(wc -l < "$BATCH_FILE") $phase messages with acks=all."
}

wait_for_topic_isr_three() {
  local tries=0 desc
  while [[ "$tries" -lt 45 ]]; do
    desc="$(docker exec sm-kafka-resilience-2 /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server kafka-2:9092 --describe --topic "$TOPIC" 2>/dev/null || true)"
    if [[ -n "$desc" ]] && printf '%s\n' "$desc" | awk -F'Isr: ' '/Isr:/ { n=split($2, a, ","); if (n != 3) bad=1; rows++ } END { exit !(rows >= 3 && !bad) }'; then
      return 0
    fi
    tries=$((tries + 1))
    sleep 2
  done
  echo "ERROR: All three replicas did not return to ISR within 90 seconds." >&2
  printf '%s\n' "$desc" >&2
  return 1
}

TOTAL_PER_PHASE="${MESSAGES_PER_PHASE:-20}"
[[ "$TOTAL_PER_PHASE" =~ ^[1-9][0-9]*$ ]] || { echo "ERROR: MESSAGES_PER_PHASE must be a positive integer." >&2; exit 1; }

echo "Starting isolated 3-broker resilience cluster..."
compose up -d
wait_for_kafka sm-kafka-resilience-1 kafka-1:9092

docker exec sm-kafka-resilience-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 --create --if-not-exists \
  --topic "$TOPIC" --partitions 3 --replication-factor 3 \
  --config min.insync.replicas=2

docker exec sm-kafka-resilience-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 --describe --topic "$TOPIC"
produce_messages sm-kafka-resilience-1 kafka-1:9092 before-outage "$TOTAL_PER_PHASE"

echo "Stopping broker 1 to simulate one broker failure..."
compose stop kafka-1
BROKER1_STOPPED=1
wait_for_kafka sm-kafka-resilience-2 kafka-2:9092
sleep 5
produce_messages sm-kafka-resilience-2 kafka-2:9092 during-outage "$TOTAL_PER_PHASE"

echo "Restarting broker 1 and waiting for all replicas to rejoin ISR..."
compose start kafka-1
BROKER1_STOPPED=0
wait_for_kafka sm-kafka-resilience-1 kafka-1:9092
wait_for_topic_isr_three

EXPECTED_COUNT=$((TOTAL_PER_PHASE * 2))
echo "Consuming $EXPECTED_COUNT records from the start of the acceptance topic..."
timeout 60s docker exec sm-kafka-resilience-2 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-2:9092 --topic "$TOPIC" --from-beginning \
  --max-messages "$EXPECTED_COUNT" > "$ACTUAL_FILE"

[[ "$(wc -l < "$ACTUAL_FILE" | tr -d ' ')" -eq "$EXPECTED_COUNT" ]] || {
  echo "FAIL: expected $EXPECTED_COUNT records, read $(wc -l < "$ACTUAL_FILE")." >&2
  exit 1
}
if ! diff -u <(LC_ALL=C sort "$EXPECTED_FILE") <(LC_ALL=C sort "$ACTUAL_FILE"); then
  echo "FAIL: message IDs were missing or duplicated; see diff above." >&2
  exit 1
fi

echo "PASS: all $EXPECTED_COUNT acknowledged message IDs were read back exactly once."
echo "Topic: $TOPIC"
echo "Record the topic description and this output in docs/resilience_acceptance.md."
echo "Cluster remains running. Stop without deleting its volumes using:"
echo "docker compose -p $PROJECT -f kafka/resilience/docker-compose.yml down"
