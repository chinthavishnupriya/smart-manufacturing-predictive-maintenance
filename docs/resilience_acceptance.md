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

## 2. MySQL -> Sqoop -> HDFS

The SQL schema and seeded demo rows are in hadoop/sqoop/mysql_source_schema.sql. Run against a demo MySQL database, create a least-privilege read-only account, and provide MySQL Connector/J to the installed Sqoop environment. Put the password in a restrictive HDFS password file; do not commit or print database passwords.

    export MYSQL_JDBC_URL='jdbc:mysql://localhost:3306/smart_manufacturing'
    export MYSQL_USER='sqoop_user'
    export SQOOP_PASSWORD_FILE="/user/$USER/.credentials/mysql.pwd"
    bash hadoop/sqoop/import_reference_tables.sh

Compare source SQL COUNT(*) for machine_master, maintenance_history and work_orders with counts in their HDFS import directories. Save Sqoop logs and HDFS count/sample output. Do not mark Sqoop PASS before all three table imports and reconciliation succeed.

## 3. Kafka broker-failure drill

The existing kafka/docker-compose.yml is a one-broker development stack with replication factor 1, so it cannot prove broker fault tolerance. The acceptance harness uses separate containers, ports and volumes and does not modify the existing stack. The isolated stack at kafka/resilience/docker-compose.yml uses three KRaft broker/controller nodes, topic replication factor 3, min.insync.replicas=2 and acks=all. It uses ports 19092-39092 and separate named volumes.

    bash kafka/resilience/run_broker_failure_drill.sh

Expected result with default MESSAGES_PER_PHASE=20:

    PASS: all 40 acknowledged message IDs were read back exactly once.

The script stops broker 1, produces through the remaining brokers, restarts broker 1, waits for all replicas to return to ISR and compares all message IDs. Preserve its output and topic description. The stack remains running for inspection; stop containers without deleting their volumes with:

    docker compose -p smart-manufacturing-resilience -f kafka/resilience/docker-compose.yml down

## 4. Spark checkpoint/replay and idempotent HBase write

This isolated integration drill uses spark_streaming/sensor_stream_part3_test.py and a new checkpoint path per run. The test script supports a test-only pause after the HBase write but before the Spark microbatch completes. The drill uses the existing single windowed aggregation state operator so the configured Update output mode remains supported. The production stream and its existing checkpoint are not modified by this drill.

Prerequisites: working Kafka at localhost:9092 and HBase Thrift at localhost:9090, models/rul_model.joblib, and a Python environment containing PySpark, kafka-python and happybase. The runner creates/reuses isolated resources named sensor_raw_resilience_test and sensor_anomalies_resilience_test by default; do not override these to the production topic/table.

    PYTHON_BIN="$PWD/.venv-spark/bin/python" bash spark_streaming/resilience/run_checkpoint_recovery_drill.sh

The runner publishes one event, pauses after HBase has been written but before foreachBatch returns, kills the Spark process group, and restarts using the same checkpoint. It verifies the interrupted batch ID is replayed and that event_count remains 1 and HBase row count remains stable. This tests replay of the same uncommitted Kafka offsets; it does not test a new intentional duplicate Kafka record. Save output and the generated log. A pass covers this tested local path and crash timing; it is not proof of every possible failure timing or production-scale exactly-once semantics.

## 5. Databricks Bronze/Silver/Gold reconciliation

The tracked repository has an Airflow DAG invoking a Databricks job, but the workspace pipeline source/table identifiers and current run evidence are not fully tracked here. Use the actual table names from the Databricks workspace and the same run ID/time range in every comparison:

    SELECT COUNT(*) FROM <catalog>.<schema>.<bronze_table> WHERE <run_filter>;
    SELECT COUNT(*) FROM <catalog>.<schema>.<silver_table> WHERE <run_filter>;
    SELECT COUNT(*) FROM <catalog>.<schema>.<gold_grain_table> WHERE <run_filter>;

Only expect counts to match where row grain/filter definitions are the same. Silver may reject/quarantine invalid records and Gold may aggregate to machine/day, so also document expected rejects and aggregate reconciliation. Record the real execution IDs, counts, and exceptions before calling it a PASS.

## Status at implementation time

| Check | Status | Evidence still required |
|---|---|---|
| Hadoop mapper/reducer local contract tests | Added; local run pending | Unit test output from the repo checkout |
| Hadoop HDFS/YARN MapReduce job | Not run | Successful real cluster job and output |
| MySQL/Sqoop imports | Not run | Three import logs plus source/HDFS count reconciliation |
| Three-broker Kafka failure/zero-loss drill | Added; not run | Docker runtime output from the isolated three-broker stack |
| Spark checkpoint crash/replay | Added; not run | Local Kafka/Spark/HBase run output and log |
| Databricks Bronze/Silver/Gold reconciliation | Not verified | Actual workspace run ID/table names/count comparisons |
