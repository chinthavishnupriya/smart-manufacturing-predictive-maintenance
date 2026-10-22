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

The runner assigns a unique machine ID and an isolated checkpoint, waits for the first completed Spark microbatch before publishing, then pauses the test stream after the HBase put but before the foreachBatch callback returns. It kills the Python/JVM process group, restarts against the same checkpoint, verifies the interrupted batch ID is replayed, and checks that each HBase window row still has event_count=1 and that row count does not increase across restart. It does not publish a second logical duplicate record.

The crash-pause behavior is test-only and enabled only with `RESILIENCE_PAUSE_AFTER_HBASE_SECONDS`. The production stream and test stream share event-ID fingerprinting and window-aggregation logic, but the drill uses a unique machine ID, isolated Kafka topic/table names, and a fresh checkpoint. Logs are written to `outputs/` and checkpoints are retained under `checkpoints/` for inspection.

A successful run is evidence for this exact local test path, not a universal exactly-once or production reliability guarantee. Save the full terminal output and log, and do not mark acceptance PASS if a prerequisite is missing or any check fails.

## Latency benchmark

Run the isolated Kafka-to-HBase latency measurement with the local streaming environment:

    PYTHON_BIN="$PWD/.venv-spark/bin/python" KAFKA_SERVER=localhost:19092 KAFKA_BOOTSTRAP=localhost:19092 bash spark_streaming/resilience/run_latency_benchmark.sh

The benchmark uses the same dedicated test topic/table names but a fresh checkpoint and unique machine IDs. The benchmark defaults to a one-second trigger. The production stream also now defaults to one second for the latency objective; the test stream defaults to five seconds unless `SPARK_TRIGGER_INTERVAL` is set. It sends 3 unmeasured warm-up events, then measures 10 uniquely identified events sequentially, waiting for each to be observed by the HBase sink before sending the next. Configure these counts with `RESILIENCE_LATENCY_WARMUP_EVENTS` and `RESILIENCE_LATENCY_EVENTS`. Per-event samples and p50/p95/max metrics are saved to `outputs/spark-latency-<run-id>.json`. It fails if any expected sample is missing or either p95 or the maximum measured latency is 10,000 ms or higher, matching the project's under-10-second acceptance target. The sequential workload measures a warmed local path; it is not a production-scale percentile guarantee.

## Duplicate-event acceptance drill

Run this to publish the **same JSON payload twice** to the isolated topic and inspect the aggregate written to the isolated HBase table:

    PYTHON_BIN="$PWD/.venv-spark/bin/python" KAFKA_SERVER=localhost:19092 KAFKA_BOOTSTRAP=localhost:19092 bash spark_streaming/resilience/run_duplicate_event_drill.sh

The runner uses a fresh checkpoint and unique machine ID. It waits for the first Spark microbatch to complete before publishing, so Kafka `startingOffsets=latest` has already been resolved. It then checks both Kafka acknowledgement offsets, waits for Spark's checkpoint to include both records and for the corresponding sink batch to finish, and verifies all ten sliding-window HBase rows.

- `DUPLICATE_EVENT_RESULT=DEDUPLICATED` means all rows stayed at `event_count=1` after both Kafka offsets were processed.
- `DUPLICATE_EVENT_RESULT=DOUBLE_COUNT_CONFIRMED` means all rows ended at `event_count=2`; the script exits nonzero to flag a deduplication regression or a stale code version.

The stream prefers a producer-supplied stable `event_id`. Older payloads without one get a SHA-256 fingerprint of machine, timestamp, measurements, and failure. That fallback can treat legitimate identical-valued readings at the same timestamp as duplicates, so live producers should supply stable unique IDs. The windowed `collect_set` holds the unique event payloads for each active window; verify memory and latency under representative load. This isolated test is not proof of universal exactly-once processing.
