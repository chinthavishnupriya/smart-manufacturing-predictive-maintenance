# Spark checkpoint and duplicate-event acceptance

This is an isolated test harness for the two streaming failure modes in the capstone acceptance rubric: restarting a Spark job from its checkpoint and replaying an identical sensor event.

## Preconditions

- Kafka available at localhost:9092 (or KAFKA_SERVER) and HBase Thrift available at localhost:9090 (or HBASE_HOST/HBASE_PORT).
- The runner creates/reuses the isolated topic `sensor_raw_resilience_test` and HBase table `sensor_anomalies_resilience_test`; do not override them to the production names.
- Local artifact models/rul_model.joblib exists.
- Python environment includes PySpark, kafka-python and happybase.

## Run

    PYTHON_BIN="$PWD/.venv-spark/bin/python" bash spark_streaming/resilience/run_checkpoint_recovery_drill.sh

Optional overrides: `KAFKA_SERVER`, `KAFKA_BOOTSTRAP`, `HBASE_HOST`, `HBASE_PORT`. The dedicated test topic/table names can be changed with `KAFKA_TOPIC`/`HBASE_TABLE`, but must remain different from the live demo names.

The runner assigns a unique machine ID and an isolated checkpoint, publishes an event and pauses the test stream after the HBase put but before the foreachBatch callback returns. It publishes an exact duplicate, kills the process, restarts against the same checkpoint, and checks that each HBase window row has event_count=1 and that row count does not increase across restart.

The test-stream changes are intentionally isolated from spark_streaming/sensor_stream.py and its production checkpoint. Logs are written to outputs/ and checkpoints are retained under checkpoints/ for inspection.

A successful run is evidence for this exact local test path, not a universal exactly-once or production reliability guarantee. Save the full terminal output and log, and do not mark acceptance PASS if a prerequisite is missing or any check fails.
