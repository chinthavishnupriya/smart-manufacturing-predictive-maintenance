# Hadoop, Sqoop and resilience acceptance plan

Acceptance status must reflect tests actually run. The presence of source code or a test harness alone is not a PASS.

## 1. Hadoop MapReduce

The Hadoop Streaming job at hadoop/mapreduce summarizes sensor rows by machine_id, counts failures and computes failure rate. HDFS hosts input/output; YARN schedules the MapReduce job. Local contract tests can be run without Hadoop:

    python3 -m unittest discover -s tests -p 'test_hadoop_streaming.py' -v

For cluster acceptance, configure Hadoop/HDFS/YARN and Python 3 on worker nodes, then run:

    hadoop version
    yarn node -list
    hdfs dfs -ls /
    bash hadoop/mapreduce/run_hadoop_streaming.sh

Capture successful YARN job status and output. The local contract test is not a cluster acceptance PASS.

### Recorded Hadoop MapReduce cluster run

- YARN application: `application_1792644122633_0001`; MapReduce job: `job_1792644122633_0001`.
- Status: completed successfully on 2026-10-22.
- Counters: 100,001 map-input records (CSV header included), 100,000 mapper output records, 100 reducer input groups, and 100 reducer output records.
- HDFS output: `/user/vishnupriya/smart-manufacturing/output/machine_failure_rates_20261022_044209`.
- Each output group reports 1,000 sensor records for one machine.
- Caveat: the first `yarn node -list` call reported zero nodes, yet the submitted job completed. If cluster scheduling reliability matters for the final demo, investigate NodeManager registration later; do not rerun this successful job merely for that warning.

## 2. MySQL -> Sqoop -> HDFS

The SQL schema and seeded demo rows are in hadoop/sqoop/mysql_source_schema.sql. Run against a demo MySQL database, create a least-privilege read-only account, and provide MySQL Connector/J to the installed Sqoop environment. Put the password in a restrictive HDFS password file; do not commit or print database passwords.

    export MYSQL_JDBC_URL='jdbc:mysql://localhost:3306/smart_manufacturing'
    export MYSQL_USER='sqoop_user'
    export SQOOP_PASSWORD_FILE="/user/$USER/.credentials/mysql.pwd"
    bash hadoop/sqoop/import_reference_tables.sh

Compare source SQL COUNT(*) for machine_master, maintenance_history and work_orders with counts in their HDFS import directories. Save Sqoop logs and HDFS count/sample output. Do not mark Sqoop PASS before all three table imports and reconciliation succeed.

### Recorded MySQL -> Sqoop -> HDFS run

- Imported tables: `machine_master`, `maintenance_history`, `work_orders`.
- All three Sqoop 1.4.7 MapReduce import jobs completed successfully under YARN.
- HDFS base: `/user/vishnupriya/smart-manufacturing/sqoop-imports/20261022_045148`.
- Count reconciliation: `machine_master` MySQL 5 / HDFS 5; `maintenance_history` MySQL 3 / HDFS 3; `work_orders` MySQL 2 / HDFS 2.
- Used a separate `sqoop_acceptance` account with SELECT grants and an HDFS password file; the pre-existing `sqoop_user` account was not changed.
- Result: **PASS for the three demo-table imports and row-count reconciliation**.

## 3. Kafka broker-failure drill

The existing kafka/docker-compose.yml is a one-broker development stack with replication factor 1, so it cannot prove broker fault tolerance. The acceptance harness uses separate containers, ports and volumes and does not modify the existing stack. The isolated stack at kafka/resilience/docker-compose.yml uses three KRaft broker/controller nodes, topic replication factor 3, min.insync.replicas=2 and acks=all. It uses ports 19092-39092 and separate named volumes.

    bash kafka/resilience/run_broker_failure_drill.sh

Expected result with default MESSAGES_PER_PHASE=20:

    PASS: all 40 acknowledged message IDs were read back exactly once.

The script stops broker 1, produces through the remaining brokers, restarts broker 1, waits for all replicas to return to ISR and compares all message IDs. Preserve its output and topic description. The stack remains running for inspection; stop containers without deleting their volumes with:

    docker compose -p smart-manufacturing-resilience -f kafka/resilience/docker-compose.yml down

## Recorded Kafka broker-failure run

- Run timestamp from the generated run ID: `2026-10-21T22:29:27Z`.
- Topic: `resilience-drill-20261021T222927Z-235155`.
- Topic settings: 3 partitions, replication factor 3, `min.insync.replicas=2`.
- ISR before outage: `1,2,3` for all three partitions.
- Broker 1 was stopped successfully; another broker accepted 20 messages with `acks=all`.
- After broker 1 restarted, the consumer read 40 total records.
- Expected-vs-observed message ID comparison passed with no missing or duplicate IDs.

Result: **PASS for this one-broker outage drill**. This does not establish resilience to simultaneous failures, network partitions, or every possible broker/controller failure.

## 4. Spark checkpoint/replay and idempotent HBase write

This isolated integration drill uses spark_streaming/sensor_stream_part3_test.py and a new checkpoint path per run. The test script supports a test-only pause after the HBase write but before the Spark microbatch completes. The drill uses the existing single windowed aggregation state operator so the configured Update output mode remains supported. The production stream and its existing checkpoint are not modified by this drill.

Prerequisites: working Kafka at localhost:9092 and HBase Thrift at localhost:9090, models/rul_model.joblib, and a Python environment containing PySpark, kafka-python and happybase. The runner creates/reuses isolated resources named sensor_raw_resilience_test and sensor_anomalies_resilience_test by default; do not override these to the production topic/table.

    PYTHON_BIN="$PWD/.venv-spark/bin/python" bash spark_streaming/resilience/run_checkpoint_recovery_drill.sh

The runner publishes one event, pauses after HBase has been written but before foreachBatch returns, kills the Spark process group, and restarts using the same checkpoint. It verifies the interrupted batch ID is replayed and that event_count remains 1 and HBase row count remains stable. This tests replay of the same uncommitted Kafka offsets; it does not test a new intentional duplicate Kafka record. Save output and the generated log. A pass covers this tested local path and crash timing; it is not proof of every possible failure timing or production-scale exactly-once semantics.

### Recorded first Spark-drill attempt

On 2026-10-22, the runner successfully found the existing isolated Kafka topic but failed to connect to HBase Thrift at `localhost:9090` (`TTransportException`). This is a **prerequisite blocker**, not a Spark recovery-test failure: Spark had not started and no checkpoint/replay acceptance result was produced. Confirm the HBase Thrift service is running and reachable at the configured host/port before rerunning.

### Recorded Spark checkpoint-recovery run

- Run ID: `20261022T043526Z-273114`; batch ID replayed: `0`.
- The first HBase write produced 10 sliding-window rows for machine `RESILIENCE-20261022T043526Z-273114`, each with `event_count=1`.
- Spark was killed before the callback returned, then restarted using the same checkpoint.
- The same batch ID replayed; the post-restart result still had 10 rows and all `event_count` values remained 1.
- Log: `outputs/spark-resilience-20261022T043526Z-273114.log`. Local checkpoint retained under `checkpoints/spark-resilience-20261022T043526Z-273114`.

Result: **PASS for this tested crash timing and isolated local stack**. This does not establish exactly-once behavior under every failure timing or at production scale.


### Recorded Spark checkpoint-recovery run after microbatch collection optimization

- Run ID: `20261022T060638Z-322538`; batch ID replayed: `1`.
- The first HBase write produced 10 sliding-window rows for machine `RESILIENCE-20261022T060638Z-322538`, and each row had `event_count=1`.
- The Spark test process was killed before the callback returned, then restarted with the same checkpoint. Batch `1` replayed.
- After replay, HBase still had 10 rows and every row still had `event_count=1`.
- Log: `outputs/spark-resilience-20261022T060638Z-322538.log`. Checkpoint retained under `checkpoints/spark-resilience-20261022T060638Z-322538`.

Result: **PASS for this tested crash timing after the microbatch collection change**. This remains limited to the isolated local stack and does not prove all crash timings or intentional duplicate-event aggregation behavior.


### GitHub Actions contract workflow

- Workflow run: `39755265566`, "Acceptance contract tests".
- Result: **PASS**. The Python compile step, shell syntax checks, Hadoop mapper/reducer contract tests, and resilience/Databricks contract tests all completed successfully.
- The workflow is static/contract validation. It does not start Kafka, Spark, HBase, Hadoop/YARN, Sqoop/MySQL, or Databricks, so it does not replace the environment-level acceptance drills.


## 5. Databricks Bronze/Silver/Gold reconciliation

The tracked repository has an Airflow DAG invoking a Databricks job, but the workspace pipeline source/table identifiers and current run evidence are not fully tracked here. Use the actual table names from the Databricks workspace and the same run ID/time range in every comparison:

    SELECT COUNT(*) FROM <catalog>.<schema>.<bronze_table> WHERE <run_filter>;
    SELECT COUNT(*) FROM <catalog>.<schema>.<silver_table> WHERE <run_filter>;
    SELECT COUNT(*) FROM <catalog>.<schema>.<gold_grain_table> WHERE <run_filter>;

Only expect counts to match where row grain/filter definitions are the same. Silver may reject/quarantine invalid records and Gold may aggregate to machine/day, so also document expected rejects and aggregate reconciliation. Record the real execution IDs, counts, and exceptions before calling it a PASS.

### Recorded Databricks run-history check

- Workspace authentication succeeded with CLI profile `dbc-bdad902e-fcf6`.
- Job: `Smart Manufacturing ETL Pipeline`, job ID `74232591887481`.
- The run-history query returned numerous runs with lifecycle `TERMINATED` and result `SUCCESS`.
- The returned history also includes run `303157520146527`, lifecycle `INTERNAL_ERROR`, result `FAILED`, with message `RESOURCE_EXHAUSTED`: the Free Edition daily resource limit prevented cluster launch and downstream tasks were skipped; run `310546345110678` was canceled.
- This confirms previous successful job executions but provides no Bronze/Silver/Gold table counts, schemas, grain, or run-filter reconciliation. Do **not** mark data reconciliation PASS based on run states alone. Fresh execution may remain blocked while the daily quota is exhausted.


### Recorded Databricks table-count snapshot

- Date: 2026-10-22. Query used the `Serverless Starter Warehouse` (`aa7aac0d7e51d340`); it was stopped before the check, reached `RUNNING`, and a stop was requested afterward.
- `workspace.default.sensor_bronze`: **100,000** rows.
- `workspace.default.sensor_silver`: **100,000** rows.
- `workspace.default.sensor_invalid_records`: **0** rows.
- `workspace.default.machine_health`: **100** rows.
- `workspace.default.downtime_kpis`: **100** rows.
- The SQL count query completed successfully for all five tables. The query did not filter by a specific ETL run ID/time window, so this is a current table-count snapshot—not run-scoped Bronze/Silver/Gold reconciliation. Gold's 100-row grain is also aggregated relative to sensor-level Bronze/Silver.
- The local SDK error from passing a raw string for `on_wait_timeout` was corrected in `scripts/databricks_reconcile_counts.py` by using `ExecuteStatementRequestOnWaitTimeout.CONTINUE`.

Result: **Current-snapshot row-count and machine-grain checks PASS**. Run-scoped lineage remains unavailable without a shared run-ID column in the tables.





### Recorded warmed burst latency benchmark

- Run: `20261022T092818Z-443844`; isolated topic `sensor_raw_resilience_test`, isolated table `sensor_anomalies_resilience_test`.
- Three warm-up events excluded; 10 measured records were published in burst mode; all 10 were observed.
- p50: `2,946.715 ms`; p95: `3,970.166 ms`; maximum: `3,970.166 ms`.
- Result: **PASS** for this warmed 10-event burst test. This establishes the under-10-second target for this tested local workload, not production-scale sustained throughput.
- Metrics: `outputs/spark-latency-20261022T092818Z-443844.json`; log: `outputs/spark-latency-20261022T092818Z-443844.log`.


### Recorded checkpoint replay after event deduplication change

- Run: `20261022T090407Z-429792`; machine `RESILIENCE-20261022T090407Z-429792`; batch `1`.
- Before crash and after restart, HBase returned 10 sliding-window rows and every row had `event_count=1`.
- Spark was killed after HBase write but before the batch callback returned, then restarted with the same checkpoint. The same batch replayed and the HBase row count remained stable.
- Result: **PASS for this tested crash timing after the event-ID/window-aggregation change**. This is not a universal exactly-once guarantee.
- Log: `outputs/spark-resilience-20261022T090407Z-429792.log`; checkpoint: `checkpoints/spark-resilience-20261022T090407Z-429792`.


### Recorded duplicate-event failure before remediation

- Run ID: `20261022T064346Z-349036`; isolated topic `sensor_raw_resilience_test`; isolated HBase table `sensor_anomalies_resilience_test`.
- The exact same JSON payload was acknowledged at Kafka offsets `22` and `23`.
- Spark checkpoint confirmed the batch included both offsets. HBase contained 10 sliding-window rows, and all rows had `event_count=2`.
- Result: **FAIL for event-level duplicate suppression in the pre-fix code**. New event-ID/aggregate-payload deduplication code is committed; re-run the duplicate drill before accepting the fix.

### Recorded warmed Spark streaming latency benchmark

- Run: `20261022T062539Z-335222`; isolated topic `sensor_raw_resilience_test`; isolated table `sensor_anomalies_resilience_test`.
- Trigger interval was one second. Three warm-up events were excluded, followed by ten unique events sent sequentially; the next event was sent after the previous event had been observed by the HBase sink.
- All 10 measured events were observed. p50 was 2,186.979 ms, p95 2,832.237 ms, and maximum 2,832.237 ms.
- Result: **PASS for this warmed sequential local measurement** with p95 and max below 10,000 ms. The prior burst-style sample had p95 18,753.364 ms; burst-load and production-scale latency are not established by the sequential test.
- Log: `outputs/spark-latency-20261022T062539Z-335222.log`; metrics: `outputs/spark-latency-20261022T062539Z-335222.json`; checkpoint: `checkpoints/spark-latency-20261022T062539Z-335222`.



### Recorded post-dedup latency benchmark

- Run: `20261022T082940Z-408408`; 3 warm-up events, 10 measured events, all 10 observed.
- p50 `2,546.593 ms`; p95 `3,336.116 ms`; max `3,336.116 ms`. All observed samples were below the instructor's 10-second limit.
- Result: **PASS for warmed sequential local workload**. Burst-load and production-scale latency remain unverified.
- Metrics: `outputs/spark-latency-20261022T082940Z-408408.json`; log: `outputs/spark-latency-20261022T082940Z-408408.log`.

### Recorded duplicate-event deduplication pass

- Run: `20261022T071051Z-365425`; identical payload Kafka offsets `26` and `27`; completed Spark batch `1`.
- All 10 HBase sliding-window rows for machine `DUPLICATE-20261022T071051Z-365425` remained at `event_count=1`.
- Result: **PASS for this tested identical-payload scenario**.
- Report: `outputs/spark-duplicate-20261022T071051Z-365425.json`; log: `outputs/spark-duplicate-20261022T071051Z-365425.log`.
- Scope caveat: this is not a universal exactly-once guarantee. Re-run the checkpoint recovery and latency acceptance tests because event aggregation changed after those prior runs.



### Updated Databricks reconciliation script

The script now queries row counts and distinct machine IDs for each Bronze/Silver/Gold-related table, checks Silver/invalid counts against Bronze, checks each Gold output against Silver's distinct machine count, and saves a JSON report under `outputs/databricks-reconciliation-<UTC timestamp>.json`.

Run it against the current snapshot:

```bash
DATABRICKS_CONFIG_PROFILE=dbc-bdad902e-fcf6 \
.venv-airflow-clean/bin/python scripts/databricks_reconcile_counts.py
```

If the workspace later adds a common pipeline-run ID column to every target table, filter all five tables consistently with `--run-id` and `--run-id-column`. The script deliberately rejects that mode when the named column is missing. Recorded current-snapshot run below confirms the updated grain checks passed and the JSON report was written.

### Recorded current-snapshot Databricks reconciliation — 2026-10-22

- Reconciliation report: `outputs/databricks-reconciliation-20261022T093138Z.json`.
- Bronze: 100,000 rows / 100 distinct machines.
- Silver: 100,000 rows / 100 distinct machines.
- Invalid records: 0 rows.
- Machine health Gold: 100 rows / 100 distinct machines.
- Downtime KPI Gold: 100 rows / 100 distinct machines.
- All five checks passed: Silver <= Bronze, invalid <= Bronze, each Gold table matches Silver's distinct machine count, and both Gold row counts agree.
- Result: **PASS for current-snapshot count and grain reconciliation**. Because a common pipeline-run ID was not used, this is not a run-scoped row comparison.



### Latest Spark checkpoint replay after deduplication

Run `20261022T090407Z-429792` passed: batch 1 replayed from the same checkpoint, HBase stayed at 10 rows, and every `event_count` remained 1. This proves the tested local crash timing, not every failure mode or production-scale reliability.


## Status at implementation time

| Check | Status | Evidence still required |
|---|---|---|
| Hadoop mapper/reducer local contract tests | PASS in clean validation mirror (3 tests) | Re-run in the project checkout after fetching this branch |
| Hadoop HDFS/YARN MapReduce job | PASS — job `job_1792644122633_0001` completed successfully | Output `/user/vishnupriya/smart-manufacturing/output/machine_failure_rates_20261022_044209`; 100,001 input lines including header, 100,000 mapper records, 100 reducer groups, 100 output rows; note initial `yarn node -list` showed 0 nodes despite successful job |
| MySQL/Sqoop imports | PASS — all three tables imported through Sqoop/YARN; source and HDFS counts match | Run `2026-10-22 04:51–04:54 UTC`, HDFS base `/user/vishnupriya/smart-manufacturing/sqoop-imports/20261022_045148`; machine_master 5/5, maintenance_history 3/3, work_orders 2/2 |
| Three-broker Kafka failure/zero-loss drill | PASS — 40/40 expected message IDs recovered exactly once | Run `2026-10-21T22:29:27Z`, topic `resilience-drill-20261021T222927Z-235155`; one broker stopped; 20 messages acknowledged during outage; RF=3, min ISR=2 |
| Spark checkpoint crash/replay | PASS for the tested crash timing after event-ID/window-state change | Latest run `2026-10-22T09:04:07Z`, machine `RESILIENCE-20261022T090407Z-429792`, batch 1 replayed; 10 rows remained and every `event_count` stayed 1; log `outputs/spark-resilience-20261022T090407Z-429792.log`. Other failure timings/production scale not proven. |
| Spark streaming latency | PASS — warmed sequential and warmed 10-event burst both meet <10 s target; sustained production load not tested | Sequential run `2026-10-22T08:29:40Z`: p95/max 3,336.116 ms. Burst run `2026-10-22T09:28:18Z`: 10/10 events, p95/max 3,970.166 ms; metrics `outputs/spark-latency-20261022T092818Z-443844.json`. |
| Duplicate logical event handling | PASS for the tested identical-payload case; broader semantics limited | Run `20261022T071051Z-365425`: offsets 26/27; all 10 HBase windows stayed at `event_count=1`; report `outputs/spark-duplicate-20261022T071051Z-365425.json`. After the deduplication change, checkpoint replay `20261022T090407Z-429792`, sequential latency `20261022T082940Z-408408`, and warmed burst latency `20261022T092818Z-443844` all passed their defined local test cases. |
| Databricks current-snapshot count/grain reconciliation | PASS — all five checks passed; run-scoped lineage unavailable without a common run ID column | Report `outputs/databricks-reconciliation-20261022T093138Z.json`: Bronze 100,000 / 100 machines; Silver 100,000 / 100 machines; invalid 0; machine_health 100 / 100 machines; downtime_kpis 100 / 100 machines. Checks passed for Silver <= Bronze, invalid <= Bronze, Gold rows matching Silver's distinct machine count, and equal Gold output counts. |
