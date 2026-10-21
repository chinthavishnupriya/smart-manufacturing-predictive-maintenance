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

if __name__ == "__main__":
    unittest.main()
