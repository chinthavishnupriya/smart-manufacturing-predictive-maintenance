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
- Both streams accept `SPARK_TRIGGER_INTERVAL`. The production stream default is now `1 second` to reduce trigger-wait time for the <5-second latency objective; it can be overridden with `SPARK_TRIGGER_INTERVAL`. The isolated benchmark also defaults to `1 second`. This configuration change does not itself establish that latency acceptance passes.
- Run the isolated benchmark against the separate resilience resources:

  ```bash
  PYTHON_BIN="$PWD/.venv-spark/bin/python" \
  KAFKA_SERVER=localhost:19092 \
  KAFKA_BOOTSTRAP=localhost:19092 \
  bash spark_streaming/resilience/run_latency_benchmark.sh
  ```

- The revised benchmark publishes three unmeasured warm-up events, then measures ten uniquely identified events sequentially, waiting for each to reach HBase before publishing the next. It writes per-event samples, p50, p95, and maximum latency to `outputs/spark-latency-<run-id>.json`. It uses only `sensor_raw_resilience_test`, `sensor_anomalies_resilience_test`, and a fresh checkpoint.
- The warmed sequential run (`20261022T062539Z-335222`) passed with p95 2.832 seconds. The earlier burst-style run (`20261022T061620Z-328915`) failed with p95 18.75 seconds, so burst-load and production-scale latency remain unverified.


### Recorded latency benchmark — 2026-10-22

- Run ID: `20261022T061620Z-328915`; isolated topic `sensor_raw_resilience_test`; isolated HBase table `sensor_anomalies_resilience_test`.
- The first post-optimization benchmark published five unique events two seconds apart, using a one-second trigger, but did not wait for each prior event to reach HBase before publishing the next. All five events were eventually observed.
- Per-event Kafka-ingest-to-HBase completion samples (ms): **18,753.364; 14,184.019; 8,218.649; 4,602.779; 2,261.059**.
- Reported p50: **8,218.649 ms**; p95 (nearest-rank on five samples): **18,753.364 ms**; max: **18,753.364 ms**.
- Result: **FAIL — the p95 target below 5,000 ms was not met in this run**. The declining values are consistent with startup/queueing effects, but that explanation is only a hypothesis; it does not erase the failed measurement.
- The benchmark has since been revised to send three unmeasured warm-up events and then measure ten events sequentially, waiting for each to reach HBase before publishing the next. Run that version to measure steady-state latency. Keep the failed run as historical evidence rather than replacing it.



### Recorded warmed sequential latency benchmark — 2026-10-22

- Run ID: `20261022T062539Z-335222`.
- Isolated Kafka topic: `sensor_raw_resilience_test`; isolated HBase table: `sensor_anomalies_resilience_test`.
- Trigger interval: `1 second`; 3 warm-up events excluded; 10 uniquely identified measured events.
- All **10/10** measured events were observed. No machine IDs were missing.
- Per-event Kafka-ingest-to-HBase completion samples (ms): 2,832.237; 2,163.814; 2,628.891; 2,286.865; 2,381.211; 2,255.396; 2,186.979; 2,006.450; 1,809.567; 1,931.678.
- p50: **2,186.979 ms**; p95 (nearest-rank over 10 samples): **2,832.237 ms**; max: **2,832.237 ms**.
- Result: **PASS for the warmed, sequential local benchmark**, with p95 below 5,000 ms. This does not guarantee burst-load or production-scale latency; the earlier burst-style five-event run failed and remains recorded above.
- Log: `outputs/spark-latency-20261022T062539Z-335222.log`; JSON metrics: `outputs/spark-latency-20261022T062539Z-335222.json`; isolated checkpoint: `checkpoints/spark-latency-20261022T062539Z-335222`.


## Acceptance status
| Check | Result | Notes |
|---|---|---|
| MQTT → Kafka bridge | PASS | MQTT publish succeeded and stream consumed the fresh event. |
| Spark streaming processing | PASS | Fresh event processed in a new test checkpoint. |
| HBase persistence | PASS | 10 sliding-window aggregate rows found. |
| Restart row-count check | PASS (limited) | Row count remained 10 after restart. This alone does not prove end-to-end exactly-once processing. |
| Alert/processing latency <5 s | PASS for warmed sequential local benchmark; burst-load/production latency unverified | Run `20261022T062539Z-335222`; 10/10 measured events; p50 2.187 s; p95/max 2.832 s. The earlier burst-style run p95 was 18.753 s. |
| Exactly-once/event deduplication | NOT FULLY VERIFIED | Deterministic HBase row keys support idempotent row upserts, but replayed duplicate input events have not been tested for aggregate double-counting. |

## Caveats and next steps
1. Run a burst-load latency test with fresh events and record p50/p95; the current pass applies only to the warmed sequential local workload.
2. Test broker interruption, Spark restart/checkpoint recovery, and duplicate-event replay explicitly before claiming exactly-once semantics.
3. The current sensor RUL model is experimental; machine-held-out validation was weak (R² below zero). Do not treat its predictions as production-ready.
4. The original production checkpoint was left untouched; the validation used `spark_streaming/sensor_stream_part3_test.py` and its separate `sensor_stream_hbase_rul_latency_v4_test` checkpoint.

## Conclusion
Part 3 demonstrates a functioning live MQTT → Kafka → Spark → HBase path, persistence across a test-stream restart, and a passing <5-second p95 target for the warmed sequential local benchmark. It remains **partially complete**: burst-load latency and intentional duplicate-event aggregation behavior have not been accepted, and the RUL model remains experimental.
