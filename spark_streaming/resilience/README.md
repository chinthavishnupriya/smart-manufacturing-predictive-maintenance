# Spark checkpoint replay and idempotent HBase acceptance

This is an isolated test harness for checkpoint recovery: Spark is killed after HBase receives a microbatch but before Spark marks that microbatch complete, then restarted against the same checkpoint so the same Kafka offsets are replayed.

## Preconditions

- Kafka available at localhost:9092 (or KAFKA_SERVER) and HBase Thrift available at localhost:9090 (or HBASE_HOST/HBASE_PORT).
- The runner creates/reuses the isolated topic `sensor_raw_resilience_test` and HBase table `sensor_anomalies_resilience_test`; do not override them to the production names.
- Local artifact models/rul_model.joblib exists.
- Python environment includes PySpark, kafka-python and happybase.

## Run

    PYTHON_BIN="$PWD/.venv-spark/bin/python" bash spark_streaming/resilience/run_checkpoint_recovery_drill.sh

Optional overrides: `KAFKA_SERVER`, `KAFKA_BOOTSTRAP`, `HBASE_HOST`, `HBASE_PORT`. The dedicated test topic/table names can be changed with `KAFKA_TOPIC`/`HBASE_TABLE`, but must remain different from the live demo names.

The runner assigns a unique machine ID and an isolated checkpoint, publishes one event and pauses the test stream after the HBase put but before the foreachBatch callback returns. It kills the Python/JVM process group, restarts against the same checkpoint, verifies the interrupted batch ID is replayed, and checks that each HBase window row still has event_count=1 and that row count does not increase across restart. It does not publish a second logical duplicate record.

The test-stream changes are intentionally isolated from spark_streaming/sensor_stream.py and its production checkpoint. Logs are written to outputs/ and checkpoints are retained under checkpoints/ for inspection.

A successful run is evidence for this exact local test path, not a universal exactly-once or production reliability guarantee. Save the full terminal output and log, and do not mark acceptance PASS if a prerequisite is missing or any check fails.

## Latency benchmark

Run the isolated Kafka-to-HBase latency measurement with the local streaming environment:

    PYTHON_BIN="$PWD/.venv-spark/bin/python" KAFKA_SERVER=localhost:19092 KAFKA_BOOTSTRAP=localhost:19092 bash spark_streaming/resilience/run_latency_benchmark.sh

The benchmark uses the same dedicated test topic/table names but a fresh checkpoint and unique machine IDs. It defaults to a one-second trigger only for this benchmark; the production stream now defaults to a one-second trigger for the latency objective; the test stream defaults to five seconds unless `SPARK_TRIGGER_INTERVAL` is set. It sends 3 unmeasured warm-up events, then measures 10 uniquely identified events sequentially, waiting for each to be observed by the HBase sink before sending the next. Configure these counts with `RESILIENCE_LATENCY_WARMUP_EVENTS` and `RESILIENCE_LATENCY_EVENTS`. Per-event samples and p50/p95/max metrics are saved to `outputs/spark-latency-<run-id>.json`. It fails if any expected sample is missing or p95 is not below 5,000 ms. The sequential workload measures a warmed local path; it is not a production-scale percentile guarantee.
