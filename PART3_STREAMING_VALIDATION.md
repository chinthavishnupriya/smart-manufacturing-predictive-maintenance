# Part 3 — MQTT/Kafka/Spark/HBase Validation

**Validation date:** 2026-10-21
**Status:** End-to-end path operational; the latest warmed sequential local latency run passes the instructor's under-10-second target. Burst-load performance remains unverified.

## Architecture tested
MQTT (HiveMQ) → Python MQTT-to-Kafka bridge → Kafka topic `sensor_raw` → Spark Structured Streaming → HBase table `sensor_anomalies`.

## Test evidence
- Published a fresh telemetry event for machine `PART3-FINAL-TEST-01` via MQTT.
- Spark consumed the event and wrote aggregated records to HBase.
- HBase scan returned **10 rows** for the test machine. The 10 rows correspond to overlapping sliding-window aggregates; they are not 10 distinct input events.
- Sample row values: `event_count=1`, `predicted_rul_hours=0.6452996031746033`, `anomalous_event_count=1`, `status=ANOMALY`.
- After restarting the test stream with its existing test checkpoint, a subsequent HBase scan still returned **10 rows**.
- Spark log reported `kafka_to_hbase_complete_ms ≈ 8755.678` and a processing batch duration of about `6891 ms` against a `5000 ms` trigger.

## Streaming latency acceptance

- Both `spark_streaming/sensor_stream.py` and the isolated test stream now use `batch_df.collect()` instead of `list(batch_df.toLocalIterator())` before model inference. This avoids sequential partition retrieval before the callback already materializes rows for pandas; it is an optimization candidate, not proof of lower latency.
- Both streams accept `SPARK_TRIGGER_INTERVAL`. The production stream default is now `1 second` to reduce trigger-wait time for the instructor's under-10-second latency objective; it can be overridden with `SPARK_TRIGGER_INTERVAL`. The isolated benchmark also defaults to `1 second`. This configuration change does not itself establish that latency acceptance passes.
- Run the isolated benchmark against the separate resilience resources:

  ```bash
  PYTHON_BIN="$PWD/.venv-spark/bin/python" \
  KAFKA_SERVER=localhost:19092 \
  KAFKA_BOOTSTRAP=localhost:19092 \
  bash spark_streaming/resilience/run_latency_benchmark.sh
  ```

- The revised benchmark publishes three unmeasured warm-up events, then measures ten uniquely identified events sequentially, waiting for each to reach HBase before publishing the next. It writes per-event samples, p50, p95, and maximum latency to `outputs/spark-latency-<run-id>.json`. It uses only `sensor_raw_resilience_test`, `sensor_anomalies_resilience_test`, and a fresh checkpoint.
- The warmed sequential run (`20261022T062539Z-335222`) passed with p95 2.832 seconds. The latest post-dedup run (`20261022T082940Z-408408`) measured p95/max 3.336 seconds across 10 events, also passing the required under-10-second threshold. The earlier burst-style run (`20261022T061620Z-328915`) had p95 18.75 seconds, so burst-load and production-scale latency remain unverified.


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
- Result: **PASS for the warmed, sequential local benchmark**, with p95 and all observed samples below 10,000 ms. This does not guarantee burst-load or production-scale latency; the earlier burst-style five-event run failed and remains recorded above.
- Log: `outputs/spark-latency-20261022T062539Z-335222.log`; JSON metrics: `outputs/spark-latency-20261022T062539Z-335222.json`; isolated checkpoint: `checkpoints/spark-latency-20261022T062539Z-335222`.



### Duplicate-event test and remediation

- Initial failing run: `20261022T064346Z-349036`; identical payloads were acknowledged at Kafka offsets `22` and `23`. All 10 HBase sliding-window rows ended at `event_count=2`, confirming that the previous implementation counted a duplicate logical reading twice.
- Remediation committed on the acceptance branch: add an optional `event_id` to the sensor schema, prefer the producer-supplied stable ID, and derive a SHA-256 fingerprint of the complete sensor payload for legacy events without an ID. One windowed `collect_set` retains event payloads; the HBase callback deduplicates by ID before recomputing averages, variances, event counts, and anomaly counts.
- This uses one stateful window aggregation instead of chaining a separate streaming deduplication operator with aggregation in Update mode. The windowed payload set grows with unique events in each active window; validate with representative load before treating it as a high-volume production design.
- **Verified:** run `20261022T071051Z-365425` printed `DUPLICATE_EVENT_RESULT=DEDUPLICATED`; all ten HBase rows remained at `event_count=1` after identical payloads at Kafka offsets 26 and 27. This validates the tested duplicate-payload scenario, not every exactly-once failure mode.
- Identity caveat: the payload fingerprint fallback treats two readings with identical machine, timestamp, measurements, and failure value as one event. Live producers should include a stable, unique `event_id` per logical reading to distinguish legitimate identical-valued readings.



### Recorded duplicate-event deduplication pass — 2026-10-22

- Run ID: `20261022T071051Z-365425`; isolated Kafka topic `sensor_raw_resilience_test`; isolated HBase table `sensor_anomalies_resilience_test`; fresh checkpoint `checkpoints/spark-duplicate-20261022T071051Z-365425`.
- Identical payloads were acknowledged at Kafka offsets `26` and `27`. Spark completed batch `1` after the checkpoint included the second duplicate offset.
- HBase returned **10/10 expected sliding-window rows**, with every `event_count=1`.
- Result: **PASS for identical-payload deduplication in this tested local scenario**. Report: `outputs/spark-duplicate-20261022T071051Z-365425.json`; log: `outputs/spark-duplicate-20261022T071051Z-365425.log`.
- This does not prove universal exactly-once processing. The legacy payload fingerprint can collapse legitimate same-machine events with identical timestamp and measurements; live producers should supply stable unique event IDs.



### Recorded latency benchmark after event deduplication — 2026-10-22

- Run ID: `20261022T082940Z-408408`; isolated topic/table; fresh checkpoint.
- Trigger: 1 second; 3 warm-up events excluded; 10/10 measured events observed.
- Per-event latencies (ms): 1,887.190; 2,546.593; 2,775.899; 2,133.358; 2,962.819; 2,473.644; 3,336.116; 2,901.138; 2,410.647; 3,095.206.
- p50: **2,546.593 ms**; p95: **3,336.116 ms**; maximum: **3,336.116 ms**.
- Result: **PASS** against the instructor's requirement that latency remain below 10 seconds. This is the warmed, sequential local workload; burst load and production-scale performance remain unverified.
- Metrics: `outputs/spark-latency-20261022T082940Z-408408.json`; log: `outputs/spark-latency-20261022T082940Z-408408.log`.

### Latency benchmark attempt after event-deduplication change — inconclusive

- Run ID: `20261022T071921Z-371811`.
- The runner published warm-up event `WARMUP-20261022T071921Z-371811-1` at Kafka offset `29` before Spark had resolved its `startingOffsets=latest` and completed its initial empty batch.
- The runner timed out waiting for the event's latency log line. Spark then reported an empty batch; no latency samples were recorded.
- Result: **INCONCLUSIVE — this is a benchmark startup/synchronization failure, not a latency measurement**.
- Fix committed: the latency runner now waits for `STREAMING_QUERY_ACTIVE`, which the test stream emits only after the first microbatch completes, before publishing any warm-up events. Rerun before updating latency acceptance.



### Latency runner startup attempt — 2026-10-22

- Run ID: `20261022T082358Z-404964`.
- Python compilation and shell syntax checks passed, but the resilience contract suite failed because three duplicate-drill assertions were accidentally placed in the latency test. That test placement is fixed on the branch.
- The latency runner timed out waiting for the stream startup banner after 120 seconds while Spark was still initializing; no latency measurements were produced.
- The startup banner wait has been increased to 300 seconds. The runner still waits for `STREAMING_QUERY_ACTIVE` (emitted after the first microbatch) before publishing warm-up events.
- Result: **INCONCLUSIVE — no new latency result from this run**. Rerun after pulling the latest branch.



### Recorded checkpoint recovery after event deduplication — 2026-10-22

- Run ID: `20261022T090407Z-429792`; machine ID: `RESILIENCE-20261022T090407Z-429792`.
- The test stream wrote 10 HBase sliding-window rows before the crash. All rows had `event_count=1`.
- Spark was killed after the HBase write and restarted with the same checkpoint. Batch `1` replayed; after restart the row count remained 10 and all rows remained at `event_count=1`.
- Result: **PASS for this tested crash timing with the event-ID/window-aggregation code**. This is not proof for every failure timing or production-scale load.
- Log: `outputs/spark-resilience-20261022T090407Z-429792.log`; checkpoint retained at `checkpoints/spark-resilience-20261022T090407Z-429792`.


### Burst-load latency validation

The benchmark now has two modes. The measured sequential mode has passed the under-10-second target after deduplication (run `20261022T082940Z-408408`, p95/max 3.336 seconds). A prior non-warmed burst-style run failed with p95 18.753 seconds. Run the warmed burst mode to decide whether the project can claim the target under burst load:

```bash
RESILIENCE_LATENCY_MODE=burst \
PYTHON_BIN="$PWD/.venv-spark/bin/python" \
KAFKA_SERVER=localhost:19092 \
KAFKA_BOOTSTRAP=localhost:19092 \
bash spark_streaming/resilience/run_latency_benchmark.sh
```

Only mark burst-load latency PASS if the new run observes every measured event and both p95 and maximum remain below 10 seconds. A sequential pass does not substitute for this result.

## Acceptance status
| Check | Result | Notes |
|---|---|---|
| MQTT → Kafka bridge | PASS | MQTT publish succeeded and stream consumed the fresh event. |
| Spark streaming processing | PASS | Fresh event processed in a new test checkpoint. |
| HBase persistence | PASS | 10 sliding-window aggregate rows found. |
| Restart row-count check | PASS (limited) | Latest run `20261022T090407Z-429792`: batch 1 replayed with the same checkpoint; row count remained 10 and every `event_count` stayed 1. This covers the tested crash timing only. |
| Alert/processing latency <10 s | PASS for warmed sequential local benchmark under <10 s target; burst-load/production latency unverified | Latest run `20261022T082940Z-408408`; 10/10 events; p50 2.547 s; p95/max 3.336 s, all below 10 s. Earlier burst-style sample p95 was 18.753 s and remains a burst-load risk. |
| Duplicate logical event handling | PASS for identical-payload test; broader semantics still limited | Run `20261022T071051Z-365425`; Kafka offsets 26/27; Spark batch 1 included both; all 10 HBase rows remained at `event_count=1`. Report `outputs/spark-duplicate-20261022T071051Z-365425.json`. This validates this duplicate payload scenario, not all exactly-once failure modes. |

## Caveats and next steps
1. Run a burst-load latency test with fresh events and record p50/p95; the current pass applies only to the warmed sequential local workload.
2. Burst-load latency and Databricks run-scoped reconciliation remain open. Keep the event identity caveat documented: legacy fingerprinting can merge legitimate readings with identical payload fields.
3. The current sensor RUL model is experimental; machine-held-out validation was weak (R² below zero). Do not treat its predictions as production-ready.
4. The prior checkpoint path is left untouched, and the production stream now defaults to a versioned `sensor_stream_hbase_event_dedup_v1` path to avoid reusing old state after the aggregation schema change. Acceptance drills use fresh run-specific checkpoints. The checkpoint crash/replay drill must be rerun after this code change.

## Conclusion
Part 3 demonstrates a functioning live MQTT → Kafka → Spark → HBase path, persistence across a test-stream restart, a passing warmed sequential latency test against the instructor's under-10-second target (latest p95/max 3.336 seconds), and a passing identical-payload deduplication test. It remains **partially complete** until burst-load latency is validated or explicitly accepted as a limitation, Databricks run-scoped reconciliation is completed, and the experimental RUL model's limitations are accepted.
