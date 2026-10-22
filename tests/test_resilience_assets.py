from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]

class ResilienceAssetContractTests(unittest.TestCase):
    def test_kafka_stack_has_three_distinct_nodes_and_durable_write_settings(self):
        compose = (ROOT / "kafka/resilience/docker-compose.yml").read_text(encoding="utf-8")
        for node_id in (1, 2, 3):
            self.assertIn(f"KAFKA_NODE_ID: {node_id}", compose)
        self.assertEqual(compose.count("container_name: sm-kafka-resilience-"), 3)
        self.assertIn("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3", compose)
        self.assertIn("KAFKA_DEFAULT_REPLICATION_FACTOR: 3", compose)
        self.assertIn("KAFKA_MIN_INSYNC_REPLICAS: 2", compose)
        self.assertIn("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2", compose)

    def test_broker_drill_compares_expected_and_observed_message_ids(self):
        script = (ROOT / "kafka/resilience/run_broker_failure_drill.sh").read_text(encoding="utf-8")
        self.assertIn("compose stop kafka-1", script)
        self.assertIn("compose start kafka-1", script)
        self.assertIn("acks=all", script)
        self.assertIn("diff -u", script)
        self.assertIn("min.insync.replicas=2", script)

    def test_sqoop_runner_imports_all_three_reference_tables(self):
        script = (ROOT / "hadoop/sqoop/import_reference_tables.sh").read_text(encoding="utf-8")
        for table in ("machine_master", "maintenance_history", "work_orders"):
            self.assertIn(table, script)
        self.assertIn("--password-file", script)
        self.assertIn("refusing to overwrite", script)

    def test_streaming_recovery_replays_same_batch_with_isolated_checkpoint(self):
        source = (ROOT / "spark_streaming/sensor_stream_part3_test.py").read_text(encoding="utf-8")
        runner = (ROOT / "spark_streaming/resilience/run_checkpoint_recovery_drill.sh").read_text(encoding="utf-8")
        self.assertIn("SPARK_CHECKPOINT_LOCATION", source)
        self.assertIn('StructField("event_id", StringType(), True)', source)
        self.assertIn("sha2(", source)
        self.assertIn("collect_set(", source)
        self.assertIn("calculate_window_metrics", source)
        self.assertIn("RESILIENCE_PAUSE_AFTER_HBASE_SECONDS", source)
        self.assertIn("RESILIENCE_BATCH_ID=", source)
        self.assertIn('outputMode("update")', source)
        self.assertNotIn("dropDuplicatesWithinWatermark", source)
        self.assertIn("kill -KILL --", runner)
        self.assertIn("RESILIENCE_BATCH_RESULT=$FIRST_BATCH_ID:PASS", runner)
        self.assertIn("event_count=1", runner)
        self.assertIn("sensor_raw_resilience_test", runner)
        self.assertIn("sensor_anomalies_resilience_test", runner)
        self.assertIn("SPARK_CHECKPOINT_LOCATION", runner)
        self.assertIn('wait_for_log "STREAMING_QUERY_ACTIVE" 180', runner)


    def test_duplicate_event_drill_is_isolated_and_checks_aggregate_counts(self):
        runner = (ROOT / "spark_streaming/resilience/run_duplicate_event_drill.sh").read_text(encoding="utf-8")
        publisher = (ROOT / "spark_streaming/resilience/publish_event.py").read_text(encoding="utf-8")
        stream = (ROOT / "spark_streaming/sensor_stream_part3_test.py").read_text(encoding="utf-8")
        self.assertIn("hashlib.sha256", publisher)
        self.assertIn('event["event_id"]', publisher)
        self.assertIn("Conflicting sensor payloads reuse event_id", stream)
        for resource in ("sensor_raw_resilience_test", "sensor_anomalies_resilience_test"):
            self.assertIn(resource, runner)
        self.assertIn('--copies 2', runner)
        self.assertIn("EXPECTED_END_OFFSET", runner)
        self.assertIn("find_batch_for_offset", runner)
        self.assertIn('wait_for_log "STREAMING_QUERY_ACTIVE" 120', runner)
        self.assertIn("print(\"STREAMING_QUERY_ACTIVE\", flush=True)", stream)
        self.assertIn("while query.lastProgress is None:", stream)
        self.assertIn("first completed microbatch", stream)
        self.assertIn("DUPLICATE_EVENT_RESULT=DOUBLE_COUNT_CONFIRMED", runner)
        self.assertIn('[[ "$HBASE_TABLE" != "sensor_anomalies" ]]', runner)
        self.assertIn('[[ "$KAFKA_TOPIC" != "sensor_raw" ]]', runner)
        self.assertIn('r.get("event_count") == 2', runner)

    def test_databricks_reconciliation_checks_machine_grain_and_saves_report(self):
        script = (ROOT / "scripts/databricks_reconcile_counts.py").read_text(encoding="utf-8")
        self.assertIn("COUNT(DISTINCT machine_id)", script)
        self.assertIn("run_filter_for_tables", script)
        self.assertIn("absent from {table}", script)
        self.assertIn('"machine_health_matches_silver_machine_grain"', script)
        self.assertIn('"downtime_kpis_matches_silver_machine_grain"', script)
        self.assertIn('"reconciliation_passed": passed', script)
        self.assertIn("databricks-reconciliation-", script)
        self.assertIn('"current_snapshot"', script)

    def test_latency_benchmark_waits_for_active_query(self):
        runner = (ROOT / "spark_streaming/resilience/run_latency_benchmark.sh").read_text(encoding="utf-8")
        stream = (ROOT / "spark_streaming/sensor_stream_part3_test.py").read_text(encoding="utf-8")
        self.assertIn('wait_for_log "Smart Manufacturing Streaming Started" 300', runner)
        self.assertIn('wait_for_log "STREAMING_QUERY_ACTIVE" 180', runner)
        self.assertIn('"target_p95_ms": 10000', runner)
        self.assertIn('"target_max_ms": 10000', runner)
        self.assertIn("max(ordered) < 10000", runner)
        self.assertIn('LATENCY_MODE="${RESILIENCE_LATENCY_MODE:-sequential}"', runner)
        self.assertIn('[[ "$LATENCY_MODE" == "sequential" || "$LATENCY_MODE" == "burst" ]]', runner)
        self.assertIn('if [[ "$LATENCY_MODE" == "sequential" ]]; then', runner)
        self.assertIn("Publishing burst event", runner)
        self.assertIn('"measurement_mode": os.getenv("RESILIENCE_LATENCY_MODE", "sequential")', runner)
        self.assertIn('RESILIENCE_LATENCY_MODE=burst', (ROOT / "PART3_STREAMING_VALIDATION.md").read_text(encoding="utf-8"))
        self.assertIn("while query.lastProgress is None:", stream)
        self.assertIn('print("STREAMING_QUERY_ACTIVE", flush=True)', stream)

if __name__ == "__main__":
    unittest.main()
