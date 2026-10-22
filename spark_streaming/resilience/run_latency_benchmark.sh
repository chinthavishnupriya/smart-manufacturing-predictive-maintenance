#!/usr/bin/env bash
# Measure Kafka-ingest-to-HBase completion latency on isolated test resources.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-$ROOT/.venv-spark/bin/python}"
STREAM="$ROOT/spark_streaming/sensor_stream_part3_test.py"
PUBLISHER="$ROOT/spark_streaming/resilience/publish_event.py"
PREPARE="$ROOT/spark_streaming/resilience/prepare_test_environment.py"

[[ -x "$PYTHON_BIN" ]] || {
  echo "ERROR: Python environment not found: $PYTHON_BIN. Set PYTHON_BIN." >&2
  exit 1
}
command -v timeout >/dev/null 2>&1 || {
  echo "ERROR: timeout command is required." >&2
  exit 1
}
command -v setsid >/dev/null 2>&1 || {
  echo "ERROR: setsid is required." >&2
  exit 1
}

EVENT_COUNT="${RESILIENCE_LATENCY_EVENTS:-10}"
WARMUP_COUNT="${RESILIENCE_LATENCY_WARMUP_EVENTS:-3}"
LATENCY_MODE="${RESILIENCE_LATENCY_MODE:-sequential}"
[[ "$LATENCY_MODE" == "sequential" || "$LATENCY_MODE" == "burst" ]] || {
  echo "ERROR: RESILIENCE_LATENCY_MODE must be sequential or burst." >&2
  exit 1
}
[[ "$EVENT_COUNT" =~ ^[0-9]+$ ]] && (( EVENT_COUNT >= 3 )) || {
  echo "ERROR: RESILIENCE_LATENCY_EVENTS must be an integer >= 3." >&2
  exit 1
}

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
CHECKPOINT_DIR="$ROOT/checkpoints/spark-latency-$RUN_ID"
CHECKPOINT_URI="file://$CHECKPOINT_DIR"
LOG_FILE="$ROOT/outputs/spark-latency-$RUN_ID.log"
RESULT_FILE="$ROOT/outputs/spark-latency-$RUN_ID.json"
TEMP_DIR="$(mktemp -d)"
SPARK_PID=""
MACHINE_IDS=()

mkdir -p "$ROOT/outputs" "$ROOT/checkpoints"
[[ ! -e "$CHECKPOINT_DIR" ]] || {
  echo "ERROR: Refusing to reuse checkpoint path: $CHECKPOINT_DIR" >&2
  exit 1
}

export KAFKA_SERVER="${KAFKA_SERVER:-localhost:19092}"
export KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-$KAFKA_SERVER}"
export KAFKA_TOPIC="${KAFKA_TOPIC:-sensor_raw_resilience_test}"
export HBASE_HOST="${HBASE_HOST:-localhost}"
export HBASE_PORT="${HBASE_PORT:-9090}"
export HBASE_TABLE="${HBASE_TABLE:-sensor_anomalies_resilience_test}"
export SPARK_TRIGGER_INTERVAL="${SPARK_TRIGGER_INTERVAL:-1 second}"
export SPARK_CHECKPOINT_LOCATION="$CHECKPOINT_URI"
export PYTHONUNBUFFERED=1
unset RESILIENCE_PAUSE_AFTER_HBASE_SECONDS || true

[[ "$KAFKA_TOPIC" != "sensor_raw" ]] || {
  echo "ERROR: Refusing to use production topic sensor_raw." >&2
  exit 1
}
[[ "$HBASE_TABLE" != "sensor_anomalies" ]] || {
  echo "ERROR: Refusing to use production table sensor_anomalies." >&2
  exit 1
}

cleanup() {
  if [[ -n "$SPARK_PID" ]] && kill -0 "$SPARK_PID" 2>/dev/null; then
    kill -TERM -- "-$SPARK_PID" 2>/dev/null || true
    sleep 2
    kill -KILL -- "-$SPARK_PID" 2>/dev/null || true
    wait "$SPARK_PID" 2>/dev/null || true
  fi
  rm -rf "$TEMP_DIR"
  echo "Latency log: $LOG_FILE"
  echo "Latency metrics: $RESULT_FILE"
  echo "Checkpoint retained: $CHECKPOINT_DIR"
}

wait_for_log() {
  local marker="$1" limit="$2" i=0
  while (( i < limit )); do
    grep -Fq "$marker" "$LOG_FILE" 2>/dev/null && return 0
    if [[ -n "$SPARK_PID" ]] && ! kill -0 "$SPARK_PID" 2>/dev/null; then
      echo "ERROR: Spark exited before log marker: $marker" >&2
      tail -100 "$LOG_FILE" >&2 || true
      return 1
    fi
    i=$((i + 1))
    sleep 1
  done
  echo "ERROR: Timed out waiting for: $marker" >&2
  tail -100 "$LOG_FILE" >&2 || true
  return 1
}

create_event() {
  local machine_id="$1" event_file="$2"
  "$PYTHON_BIN" - "$machine_id" > "$event_file" <<'PY'
import json
import sys
from datetime import datetime
event = {
    "machine_id": sys.argv[1],
    "event_id": f"{sys.argv[1]}-event-1",
    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "temperature": 96.0,
    "vibration": 6.2,
    "rpm": 2750.0,
    "pressure": 8.7,
    "operating_hours": 1280.0,
    "failure": 1,
}
print(json.dumps(event))
PY
}

publish_event() {
  local event_file="$1"
  "$PYTHON_BIN" "$PUBLISHER" --event-file "$event_file" \
    --bootstrap "$KAFKA_BOOTSTRAP" --topic "$KAFKA_TOPIC"
}

publish_and_wait() {
  local machine_id="$1" event_file="$2"
  publish_event "$event_file"
  wait_for_log "LATENCY machine=$machine_id" 180
  # Let foreachBatch return and Spark commit before the next input.
  sleep 2
}

trap cleanup EXIT INT TERM

echo "Preparing isolated topic $KAFKA_TOPIC and table $HBASE_TABLE..."
"$PYTHON_BIN" "$PREPARE" --bootstrap "$KAFKA_BOOTSTRAP" --topic "$KAFKA_TOPIC" \
  --hbase-host "$HBASE_HOST" --hbase-port "$HBASE_PORT" --hbase-table "$HBASE_TABLE"

echo "Starting stream: trigger=$SPARK_TRIGGER_INTERVAL checkpoint=$CHECKPOINT_URI"
setsid "$PYTHON_BIN" "$STREAM" > "$LOG_FILE" 2>&1 &
SPARK_PID=$!
wait_for_log "Smart Manufacturing Streaming Started" 300
# The startup banner appears before the query resolves Kafka startingOffsets.
# Wait for the first completed microbatch before publishing any benchmark event.
wait_for_log "STREAMING_QUERY_ACTIVE" 180

echo "Warming up stream with $WARMUP_COUNT unmeasured events..."
for (( n=1; n<=WARMUP_COUNT; n++ )); do
  machine_id="WARMUP-$RUN_ID-$n"
  event_file="$TEMP_DIR/warmup-$n.json"
  create_event "$machine_id" "$event_file"
  echo "Publishing warm-up event $n/$WARMUP_COUNT: $machine_id"
  publish_and_wait "$machine_id" "$event_file"
done

echo "Starting measured workload: mode=$LATENCY_MODE, events=$EVENT_COUNT..."
if [[ "$LATENCY_MODE" == "sequential" ]]; then
  for (( n=1; n<=EVENT_COUNT; n++ )); do
    machine_id="LATENCY-$RUN_ID-$n"
    MACHINE_IDS+=("$machine_id")
    event_file="$TEMP_DIR/event-$n.json"
    create_event "$machine_id" "$event_file"
    echo "Publishing latency event $n/$EVENT_COUNT: $machine_id"
    publish_and_wait "$machine_id" "$event_file"
  done
else
  # Publish the whole workload without waiting for each HBase result, to measure
  # queueing and batch processing under a short burst of incoming events.
  for (( n=1; n<=EVENT_COUNT; n++ )); do
    machine_id="LATENCY-$RUN_ID-$n"
    MACHINE_IDS+=("$machine_id")
    event_file="$TEMP_DIR/event-$n.json"
    create_event "$machine_id" "$event_file"
    echo "Publishing burst event $n/$EVENT_COUNT: $machine_id"
    publish_event "$event_file"
  done
  for machine_id in "${MACHINE_IDS[@]}"; do
    wait_for_log "LATENCY machine=$machine_id" 300
  done
fi



"$PYTHON_BIN" - "$LOG_FILE" "$RESULT_FILE" "$SPARK_TRIGGER_INTERVAL" "$WARMUP_COUNT" "${MACHINE_IDS[@]}" <<'PY'
import json
import os
import math
import re
import sys
from pathlib import Path
from datetime import datetime, timezone

log_path = Path(sys.argv[1])
result_path = Path(sys.argv[2])
trigger = sys.argv[3]
warmup_count = int(sys.argv[4])
expected_ids = sys.argv[5:]
pattern = re.compile(
    r"LATENCY machine=(\S+) kafka_to_hbase_complete_ms=([0-9]+(?:\.[0-9]+)?)"
)
first_sample = {}
for line in log_path.read_text(encoding="utf-8", errors="replace").splitlines():
    match = pattern.search(line)
    if match and match.group(1) in expected_ids:
        first_sample.setdefault(match.group(1), float(match.group(2)))

missing = [machine_id for machine_id in expected_ids if machine_id not in first_sample]
samples = [first_sample[machine_id] for machine_id in expected_ids if machine_id in first_sample]
ordered = sorted(samples)

def percentile(values, p):
    if not values:
        return None
    # Nearest-rank percentile.
    return values[max(0, math.ceil(p * len(values)) - 1)]

metrics = {
    "measured_at_utc": datetime.now(timezone.utc).isoformat(),
    "trigger_interval": trigger,
    "measurement_mode": os.getenv("RESILIENCE_LATENCY_MODE", "sequential"),
    "topic_isolated": True,
    "table_isolated": True,
    "warmup_events_excluded": warmup_count,
    "expected_events": len(expected_ids),
    "observed_events": len(samples),
    "missing_machine_ids": missing,
    "samples_ms": dict(zip(
        [machine_id for machine_id in expected_ids if machine_id in first_sample],
        samples,
    )),
    "p50_ms": percentile(ordered, 0.50),
    "p95_ms": percentile(ordered, 0.95),
    "max_ms": max(ordered) if ordered else None,
    "target_p95_ms": 10000,
    "target_max_ms": 10000,
    "latency_target_met": not missing and bool(ordered) and percentile(ordered, 0.95) < 10000 and max(ordered) < 10000,
    "note": (
        "Each sample is the first Spark-reported Kafka-ingest-to-HBase-completion "
        "latency for one uniquely identified machine event. This is a small local "
        "benchmark, not a production percentile guarantee."
    ),
}
result_path.write_text(json.dumps(metrics, indent=2) + "\n", encoding="utf-8")
print(json.dumps(metrics, indent=2))
if missing:
    raise SystemExit("FAIL: one or more latency events were not observed.")
if not metrics["latency_target_met"]:
    raise SystemExit("LATENCY TARGET NOT MET: p95 and every measured sample must be below 10000 ms.")
print("PASS: p95 and every measured Kafka-ingest-to-HBase latency are below 10000 ms.")
PY
