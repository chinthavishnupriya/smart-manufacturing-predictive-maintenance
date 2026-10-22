#!/usr/bin/env bash
# Detect whether publishing one identical logical event twice double-counts it.
# Uses only isolated Kafka/HBase resources and a fresh Spark checkpoint.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-$ROOT/.venv-spark/bin/python}"
STREAM="$ROOT/spark_streaming/sensor_stream_part3_test.py"
PUBLISHER="$ROOT/spark_streaming/resilience/publish_event.py"
PREPARE="$ROOT/spark_streaming/resilience/prepare_test_environment.py"
VERIFIER="$ROOT/spark_streaming/resilience/verify_hbase_event.py"

[[ -x "$PYTHON_BIN" ]] || {
  echo "ERROR: Python environment not found: $PYTHON_BIN. Set PYTHON_BIN." >&2
  exit 1
}
command -v setsid >/dev/null 2>&1 || {
  echo "ERROR: setsid is required to stop the Spark parent and JVM together." >&2
  exit 1
}

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
MACHINE_ID="DUPLICATE-$RUN_ID"
CHECKPOINT_DIR="$ROOT/checkpoints/spark-duplicate-$RUN_ID"
CHECKPOINT_URI="file://$CHECKPOINT_DIR"
LOG_FILE="$ROOT/outputs/spark-duplicate-$RUN_ID.log"
REPORT_FILE="$ROOT/outputs/spark-duplicate-$RUN_ID.json"
EVENT_FILE="$(mktemp)"
PUBLISH_REPORT="$(mktemp)"
SPARK_PID=""

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
  rm -f "$EVENT_FILE" "$PUBLISH_REPORT"
  echo "Duplicate-event log: $LOG_FILE"
  echo "Duplicate-event report: $REPORT_FILE"
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

# Return the Spark batch ID whose checkpoint offset log includes the second
# acknowledged Kafka record. Offset files are written before their batch runs;
# the caller then waits for that batch's sink callback to finish.
find_batch_for_offset() {
  "$PYTHON_BIN" - "$CHECKPOINT_DIR/offsets" "$KAFKA_TOPIC" "$EXPECTED_END_OFFSET" <<'PY'
import json
import sys
from pathlib import Path

offset_dir = Path(sys.argv[1])
topic = sys.argv[2]
target_end_offset = int(sys.argv[3])
if not offset_dir.exists():
    raise SystemExit(1)

eligible = []
for path in offset_dir.iterdir():
    if not path.name.isdigit():
        continue
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
        # Spark Kafka offset-log files contain a metadata JSON object followed
        # by the Kafka source-offset JSON object. Inspect every JSON line,
        # rather than mistaking the metadata object for the offset map.
        for line in lines:
            if not line.startswith("{"):
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            offsets = payload.get(topic)
            if not isinstance(offsets, dict):
                continue
            end_offset = max(
                (int(value) for value in offsets.values()),
                default=-1,
            )
            if end_offset >= target_end_offset:
                eligible.append(int(path.name))
                break
    except (OSError, ValueError, StopIteration, json.JSONDecodeError, TypeError):
        continue

if eligible:
    print(max(eligible))
    raise SystemExit(0)
raise SystemExit(1)
PY
}

trap cleanup EXIT INT TERM

echo "Preparing isolated Kafka topic $KAFKA_TOPIC and HBase table $HBASE_TABLE..."
"$PYTHON_BIN" "$PREPARE" --bootstrap "$KAFKA_BOOTSTRAP" --topic "$KAFKA_TOPIC" \
  --hbase-host "$HBASE_HOST" --hbase-port "$HBASE_PORT" --hbase-table "$HBASE_TABLE"

# Both sends load and publish this exact same payload, including the same event timestamp.
"$PYTHON_BIN" - "$MACHINE_ID" > "$EVENT_FILE" <<'PY'
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
print(json.dumps(event, separators=(",", ":")))
PY

echo "Starting test stream with fresh checkpoint: $CHECKPOINT_URI"
setsid "$PYTHON_BIN" "$STREAM" > "$LOG_FILE" 2>&1 &
SPARK_PID=$!
wait_for_log "Smart Manufacturing Streaming Started" 120
sleep 8

echo "Publishing two identical copies for machine $MACHINE_ID..."
"$PYTHON_BIN" "$PUBLISHER" --event-file "$EVENT_FILE" --copies 2 \
  --bootstrap "$KAFKA_BOOTSTRAP" --topic "$KAFKA_TOPIC" | tee "$PUBLISH_REPORT"

ACKED_OFFSETS="$("$PYTHON_BIN" - "$PUBLISH_REPORT" <<'PY'
import re
import sys
from pathlib import Path
text = Path(sys.argv[1]).read_text(encoding="utf-8")
offsets = [int(value) for value in re.findall(r"\boffset=(\d+)", text)]
if len(offsets) < 2:
    raise SystemExit(f"Expected two Kafka acknowledgements, found offsets={offsets}")
print(offsets[0], offsets[1])
PY
)"
read -r FIRST_OFFSET SECOND_OFFSET <<< "$ACKED_OFFSETS"
[[ "$FIRST_OFFSET" -lt "$SECOND_OFFSET" ]] || {
  echo "ERROR: The duplicate copies did not receive increasing Kafka offsets: $ACKED_OFFSETS" >&2
  exit 1
}
EXPECTED_END_OFFSET=$((SECOND_OFFSET + 1))
echo "Kafka acknowledged offsets $FIRST_OFFSET and $SECOND_OFFSET for identical payloads."

echo "Waiting for Spark checkpoint offsets to include both records..."
BATCH_ID=""
for (( i=0; i<180; i++ )); do
  if BATCH_ID="$(find_batch_for_offset)"; then
    break
  fi
  if [[ -n "$SPARK_PID" ]] && ! kill -0 "$SPARK_PID" 2>/dev/null; then
    echo "ERROR: Spark exited before checkpoint covered both duplicate records." >&2
    tail -100 "$LOG_FILE" >&2 || true
    exit 1
  fi
  sleep 1
done
[[ -n "$BATCH_ID" ]] || {
  echo "ERROR: Spark checkpoint did not reach Kafka end offset $EXPECTED_END_OFFSET." >&2
  tail -100 "$LOG_FILE" >&2 || true
  exit 1
}

wait_for_log "RESILIENCE_BATCH_RESULT=$BATCH_ID:PASS" 180
echo "Spark completed batch $BATCH_ID after its checkpoint included the second duplicate offset."

# Read until all overlapping windows agree on the final event count.
for (( i=0; i<30; i++ )); do
  set +e
  "$PYTHON_BIN" "$VERIFIER" --machine-id "$MACHINE_ID" --min-rows 10 \
    --expected-event-count 1 --host "$HBASE_HOST" --port "$HBASE_PORT" \
    --table "$HBASE_TABLE" > "$REPORT_FILE" 2>"$REPORT_FILE.err"
  VERIFY_STATUS=$?
  set -e
  if [[ "$VERIFY_STATUS" -eq 0 ]]; then
    cat "$REPORT_FILE"
    echo "PASS: both Kafka copies were processed, but all HBase aggregate rows remain event_count=1."
    echo "DUPLICATE_EVENT_RESULT=DEDUPLICATED"
    exit 0
  fi

  if "$PYTHON_BIN" - "$REPORT_FILE" <<'PY'
import json
import sys
try:
    report = json.load(open(sys.argv[1], encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    raise SystemExit(1)
counts = report.get("event_counts", [])
rows = report.get("rows", [])
raise SystemExit(0 if len(rows) >= 10 and counts == [2] and all(r.get("event_count") == 2 for r in rows) else 1)
PY
  then
    cat "$REPORT_FILE"
    echo "FAIL: identical payloads at Kafka offsets $FIRST_OFFSET and $SECOND_OFFSET were counted twice." >&2
    echo "DUPLICATE_EVENT_RESULT=DOUBLE_COUNT_CONFIRMED" >&2
    echo "End-to-end event deduplication is not implemented by this stream." >&2
    exit 1
  fi
  sleep 2
done

cat "$REPORT_FILE" 2>/dev/null || true
cat "$REPORT_FILE.err" 2>/dev/null || true
echo "INCONCLUSIVE: all 10 HBase window rows did not settle to event_count=1 or event_count=2." >&2
tail -100 "$LOG_FILE" >&2 || true
exit 2
