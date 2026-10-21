#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MAPPER="$ROOT/hadoop/mapreduce/machine_failure_mapper.py"
REDUCER="$ROOT/hadoop/mapreduce/machine_failure_reducer.py"
LOCAL_INPUT="${1:-$ROOT/data/raw/sensor_data_100k.csv}"
HDFS_INPUT="${HDFS_INPUT:-/user/$(id -un)/smart-manufacturing/input/sensor_data_100k.csv}"
HDFS_OUTPUT="${2:-/user/$(id -un)/smart-manufacturing/output/machine_failure_rates_$(date +%Y%m%d_%H%M%S)}"

command -v hadoop >/dev/null 2>&1 || { echo "ERROR: hadoop command not found; configure Hadoop/YARN first." >&2; exit 1; }
command -v hdfs >/dev/null 2>&1 || { echo "ERROR: hdfs command not found; configure HDFS first." >&2; exit 1; }
[[ -f "$LOCAL_INPUT" ]] || { echo "ERROR: sensor CSV not found: $LOCAL_INPUT" >&2; exit 1; }

if [[ -n "${HADOOP_STREAMING_JAR:-}" ]]; then
  STREAMING_JAR="$HADOOP_STREAMING_JAR"
else
  HADOOP_HOME="${HADOOP_HOME:-}"
  if [[ -z "$HADOOP_HOME" ]]; then
    for candidate in /usr/local/hadoop /opt/hadoop /usr/lib/hadoop; do
      if [[ -d "$candidate/share/hadoop/tools/lib" ]]; then HADOOP_HOME="$candidate"; break; fi
    done
  fi
  STREAMING_JAR=""
  if [[ -n "$HADOOP_HOME" && -d "$HADOOP_HOME/share/hadoop/tools/lib" ]]; then
    STREAMING_JAR="$(find "$HADOOP_HOME/share/hadoop/tools/lib" -maxdepth 1 -type f -name 'hadoop-streaming*.jar' -print -quit)"
  fi
fi
[[ -n "${STREAMING_JAR:-}" && -f "$STREAMING_JAR" ]] || {
  echo "ERROR: Hadoop Streaming JAR not found. Set HADOOP_STREAMING_JAR to its full path." >&2
  exit 1
}

hdfs dfs -mkdir -p "$(dirname "$HDFS_INPUT")"
hdfs dfs -put -f "$LOCAL_INPUT" "$HDFS_INPUT"
if hdfs dfs -test -e "$HDFS_OUTPUT"; then
  echo "ERROR: HDFS output already exists: $HDFS_OUTPUT" >&2
  exit 1
fi

hadoop jar "$STREAMING_JAR" \
  -D mapreduce.job.name=smart-manufacturing-machine-failure-summary \
  -D mapreduce.job.reduces=1 \
  -files "$MAPPER,$REDUCER" \
  -input "$HDFS_INPUT" \
  -output "$HDFS_OUTPUT" \
  -mapper "python3 machine_failure_mapper.py" \
  -reducer "python3 machine_failure_reducer.py"

echo "Hadoop Streaming job completed. Output: $HDFS_OUTPUT"
hdfs dfs -cat "$HDFS_OUTPUT"/part-*
