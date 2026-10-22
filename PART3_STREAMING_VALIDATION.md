# Part 3 — MQTT/Kafka/Spark/HBase Validation

**Validation date:** 2026-10-21
**Status:** End-to-end path operational; latency target remains unverified after the latest code optimization.

## Architecture tested
MQTT (HiveMQ) → Python MQTT-to-Kafka bridge → Kafka topic `sensor_raw` → Spark Structured Streaming → HBase table `sensor_anomalies`.

## Test evidence
- Published a fresh telemetry event for machine `PART3-FINAL-TEST-01` via MQTT.
- Spark consumed the event and wrote aggregated records to HBase.
- HBase scan returned **10 rows** for the test machine. The 10 rows correspond to overlapping sliding-window aggregates; they are not 10 distinct input events.
- Sample row values: `event_count=1`, `predicted_rul_hours=0.6452996031746033`, `anomalous_event_count=1`, `status=ANOMALY`.
- After restarting the test stream with its existing test checkpoint, a subsequent HBase scan still returned **10 rows**.
- Spark log reported `kafka_to_hbase_complete_ms ≈ 8755.678` and a processing batch duration of about `6891 ms` against a `5000 ms` trigger.

## Latest code change and next latency measurement

- Both `spark_streaming/sensor_stream.py` and the isolated test stream now use `batch_df.collect()` instead of `list(batch_df.toLocalIterator())` before model inference. This avoids sequential partition retrieval before the callback already materializes rows for pandas; it is an optimization candidate, not proof of lower latency.
- Both streams accept `SPARK_TRIGGER_INTERVAL`; the default remains `5 seconds`. The isolated benchmark defaults to `1 second` without changing the production default.
- Run the isolated benchmark against the separate resilience resources:

  ```bash
  PYTHON_BIN="$PWD/.venv-spark/bin/python" \
  KAFKA_SERVER=localhost:19092 \
  KAFKA_BOOTSTRAP=localhost:19092 \
  bash spark_streaming/resilience/run_latency_benchmark.sh
  ```

- It publishes five uniquely identified events and writes per-event samples, p50, p95, and maximum latency to `outputs/spark-latency-<run-id>.json`. It uses only `sensor_raw_resilience_test`, `sensor_anomalies_resilience_test`, and a fresh checkpoint. The target is p95 below 5,000 ms; do not mark PASS unless a real run produces that result.
- The existing 8.76-second observation remains the last completed latency measurement until the benchmark is run.

## Acceptance status
| Check | Result | Notes |
|---|---|---|
| MQTT → Kafka bridge | PASS | MQTT publish succeeded and stream consumed the fresh event. |
| Spark streaming processing | PASS | Fresh event processed in a new test checkpoint. |
| HBase persistence | PASS | 10 sliding-window aggregate rows found. |
| Restart row-count check | PASS (limited) | Row count remained 10 after restart. This alone does not prove end-to-end exactly-once processing. |
| Alert/processing latency <5 s | NOT MET | Measured completion latency was approximately 8.76 s; batch exceeded the 5 s trigger. |
| Exactly-once/event deduplication | NOT FULLY VERIFIED | Deterministic HBase row keys support idempotent row upserts, but replayed duplicate input events have not been tested for aggregate double-counting. |

## Caveats and next steps
1. Optimize and profile Spark batch processing and HBase writes, then repeat the latency test with fresh events and record p50/p95 latency.
2. Test broker interruption, Spark restart/checkpoint recovery, and duplicate-event replay explicitly before claiming exactly-once semantics.
3. The current sensor RUL model is experimental; machine-held-out validation was weak (R² below zero). Do not treat its predictions as production-ready.
4. The original production checkpoint was left untouched; the validation used `spark_streaming/sensor_stream_part3_test.py` and its separate `sensor_stream_hbase_rul_latency_v4_test` checkpoint.

## Conclusion
Part 3 demonstrates a functioning live MQTT → Kafka → Spark → HBase path and persistence across a test-stream restart. It is **partially complete**, not fully acceptance-complete, because the <5-second latency goal and rigorous exactly-once/recovery tests remain outstanding.
