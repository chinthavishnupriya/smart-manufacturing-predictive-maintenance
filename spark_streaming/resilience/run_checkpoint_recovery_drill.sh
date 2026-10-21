#!/usr/bin/env bash
# Integration drill for checkpoint recovery and idempotent HBase upserts.
# Uses the test-only stream with a brand-new checkpoint; production checkpoint is untouched.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-$ROOT/.venv-spark/bin/python}"
STREAM="$ROOT/spark_streaming/sensor_stream_part3_test.py"
PUBLISHER="$ROOT/spark_streaming/resilience/publish_event.py"
VERIFIER="$ROOT/spark_streaming/resilience/verify_hbase_event.py"

[[ -x "$PYTHON_BIN" ]] || { echo "ERROR: Python environment not found: $PYTHON_BIN. Set PYTHON_BIN to the Spark/Kafka/HBase environment." >&2; exit 1; }
[[ -f "$ROOT/models/rul_model.joblib" ]] || { echo "ERROR: models/rul_model.joblib is missing; required by the streaming test job." >&2; exit 1; }
command -v timeout >/dev/null 2>&1 || { echo "ERROR: timeout command is required." >&2; exit 1; }

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
MACHINE_ID="RESILIENCE-${RUN_ID//[^a-zA-Z0-9_-]/-}"
CHECKPOINT_DIR="$ROOT/checkpoints/spark-resilience-$RUN_ID"
CHECKPOINT_URI="file://$CHECKPOINT_DIR"
EVENT_FILE="$(mktemp)"
LOG_FILE="$ROOT/outputs/spark-resilience-$RUN_ID.log"
FIRST_REPORT="$(mktemp)"
SECOND_REPORT="$(mktemp)"
SPARK_PID=""
mkdir -p "$ROOT/outputs" "$ROOT/checkpoints"

cleanup() {
  if [[ -n "$SPARK_PID" ]] && kill -0 "$SPARK_PID" 2>/dev/null; then
    kill -TERM "$SPARK_PID" 2>/dev/null || true
    wait "$SPARK_PID" 2>/dev/null || true
  fi
  rm -f "$EVENT_FILE" "$FIRST_REPORT" "$SECOND_REPORT" "$FIRST_REPORT.err" "$SECOND_REPORT.err"
  echo "Spark drill log: $LOG_FILE"
  echo "Checkpoint retained for inspection: $CHECKPOINT_DIR"
}
trap cleanup EXIT

[[ ! -e "$CHECKPOINT_DIR" ]] || { echo "ERROR: Refusing to reuse checkpoint path: $CHECKPOINT_DIR" >&2; exit 1; }

"$PYTHON_BIN" - "$MACHINE_ID" > "$EVENT_FILE" <<'PY'
import json
import sys
from datetime import datetime
machine_id = sys.argv[1]
event = {
    "machine_id": machine_id,
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

wait_for_log() {
  local marker="$1" timeout_seconds="$2" i=0
  while [[ "$i" -lt "$timeout_seconds" ]]; do
    if grep -Fq "$marker" "$LOG_FILE" 2>/dev/null; then return 0; fi
    if [[ -n "$SPARK_PID" ]] && ! kill -0 "$SPARK_PID" 2>/dev/null; then
      echo "ERROR: Spark exited before marker: $marker" >&2
      tail -100 "$LOG_FILE" >&2 || true
      return 1
    fi
    i=$((i + 1))
    sleep 1
  done
  echo "ERROR: Timed out waiting for marker: $marker" >&2
  tail -100 "$LOG_FILE" >&2 || true
  return 1
}

verify_with_retry() {
  local output_file="$1" attempts="${2:-60}" i=0
  while [[ "$i" -lt "$attempts" ]]; do
    if "$PYTHON_BIN" "$VERIFIER" --machine-id "$MACHINE_ID" --min-rows 1 --expected-event-count 1 > "$output_file" 2> "$output_file.err"; then
      return 0
    fi
    i=$((i + 1))
    sleep 2
  done
  cat "$output_file.err" >&2 || true
  cat "$output_file" >&2 || true
  tail -100 "$LOG_FILE" >&2 || true
  return 1
}

export PYTHONUNBUFFERED=1
export SPARK_CHECKPOINT_LOCATION="$CHECKPOINT_URI"
export RESILIENCE_PAUSE_AFTER_HBASE_SECONDS=120

echo "Starting test stream with isolated checkpoint: $CHECKPOINT_URI"
"$PYTHON_BIN" "$STREAM" > "$LOG_FILE" 2>&1 &
SPARK_PID=$!
wait_for_log "Smart Manufacturing Streaming Started" 120
# Allow the query to establish its Kafka source before publishing.
sleep 10

echo "Publishing first occurrence..."
"$PYTHON_BIN" "$PUBLISHER" --event-file "$EVENT_FILE" --copies 1
wait_for_log "RESILIENCE_TEST_PAUSE_AFTER_HBASE_WRITE" 150
FIRST_BATCH_ID="$(awk '/RESILIENCE_BATCH_ID=/ { id=$0; sub(/^.*=/, "", id) } /RESILIENCE_TEST_PAUSE_AFTER_HBASE_WRITE/ { print id; exit }' "$LOG_FILE")"
[[ "$FIRST_BATCH_ID" =~ ^[0-9]+$ ]] || {
  echo "ERROR: Could not determine the paused Spark batch ID." >&2
  tail -100 "$LOG_FILE" >&2 || true
  exit 1
}
NEXT_BATCH_ID=$((FIRST_BATCH_ID + 1))
echo "Paused Spark batch: $FIRST_BATCH_ID; duplicate should be handled by batch $NEXT_BATCH_ID."
echo "HBase write completed while Spark's foreachBatch callback is paused."
verify_with_retry "$FIRST_REPORT" 30
cat "$FIRST_REPORT"

# Send an exact duplicate after the first microbatch's source offsets were selected.
echo "Publishing exact duplicate during the pause..."
"$PYTHON_BIN" "$PUBLISHER" --event-file "$EVENT_FILE" --copies 1
sleep 2

echo "Killing the Spark test process before the microbatch callback returns..."
kill -KILL "$SPARK_PID"
wait "$SPARK_PID" 2>/dev/null || true
SPARK_PID=""

unset RESILIENCE_PAUSE_AFTER_HBASE_SECONDS
echo "Restarting test stream against the same checkpoint..."
"$PYTHON_BIN" "$STREAM" >> "$LOG_FILE" 2>&1 &
SPARK_PID=$!

wait_for_log "RESILIENCE_BATCH_RESULT=$NEXT_BATCH_ID:PASS" 180
# Give Spark time to commit the completed batch's offsets to the checkpoint.
sleep 5
sleep 8
verify_with_retry "$SECOND_REPORT" 30
cat "$SECOND_REPORT"

FIRST_ROWS="$("$PYTHON_BIN" -c 'import json,sys; print(json.load(open(sys.argv[1]))["row_count"])' "$FIRST_REPORT")"
SECOND_ROWS="$("$PYTHON_BIN" -c 'import json,sys; print(json.load(open(sys.argv[1]))["row_count"])' "$SECOND_REPORT")"
[[ "$FIRST_ROWS" -eq "$SECOND_ROWS" ]] || {
  echo "FAIL: HBase row count changed across restart ($FIRST_ROWS -> $SECOND_ROWS)." >&2
  exit 1
}

echo "PASS: output rows all have event_count=1 and row count stayed $SECOND_ROWS across crash/restart."
echo "Machine ID: $MACHINE_ID"
echo "Review the log and save the JSON output as acceptance evidence."
echo "Scope: test stream and local Kafka/HBase only; this does not prove all crash timings or production-scale reliability."
