#!/usr/bin/env python3
"""Create isolated Kafka/HBase resources for the Spark resilience drill."""
import argparse
import sys

def ensure_topic(bootstrap: str, topic: str) -> None:
    try:
        from kafka import KafkaConsumer
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import TopicAlreadyExistsError
    except ImportError as exc:
        raise RuntimeError("kafka-python is required; install the project streaming dependencies") from exc

    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap,
        request_timeout_ms=30000,
        api_version_auto_timeout_ms=10000,
    )
    try:
        existing_topics = consumer.topics()
    finally:
        consumer.close()
    if topic in existing_topics:
        print(f"Kafka topic already exists: {topic}")
        return

    admin = KafkaAdminClient(
        bootstrap_servers=bootstrap,
        client_id="smart-manufacturing-resilience-setup",
        request_timeout_ms=30000,
        api_version_auto_timeout_ms=10000,
    )
    try:
        # kafka-python versions differ: create_topics may return a response
        # object (older versions) or a mapping of futures (newer versions).
        response = admin.create_topics([
            NewTopic(name=topic, num_partitions=1, replication_factor=1)
        ], validate_only=False)
        if hasattr(response, "topic_errors"):
            errors = [
                item for item in response.topic_errors
                if getattr(item, "error_code", 0) not in (0, 36)
            ]
            if errors:
                raise RuntimeError(f"Kafka topic creation failed: {errors}")
        elif hasattr(response, "get"):
            future = response.get(topic)
            if future is not None:
                try:
                    future.result(timeout=30)
                except TopicAlreadyExistsError:
                    pass
        else:
            raise RuntimeError(
                f"Unsupported kafka-python create_topics response type: {type(response).__name__}"
            )
        print(f"Kafka test topic ready: {topic}")
    finally:
        admin.close()

def ensure_hbase_table(host: str, port: int, table_name: str) -> None:
    try:
        import happybase
    except ImportError as exc:
        raise RuntimeError("happybase is required; install the project streaming dependencies") from exc

    connection = happybase.Connection(host=host, port=port, timeout=10000)
    try:
        connection.open()
        existing = {item.decode("utf-8") if isinstance(item, bytes) else str(item)
                    for item in connection.tables()}
        if table_name in existing:
            print(f"HBase test table already exists: {table_name}")
            return
        connection.create_table(table_name, {
            "metrics": dict(),
            "anomalies": dict(),
        })
        print(f"HBase test table created: {table_name}")
    finally:
        connection.close()

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="sensor_raw_resilience_test")
    parser.add_argument("--hbase-host", default="localhost")
    parser.add_argument("--hbase-port", type=int, default=9090)
    parser.add_argument("--hbase-table", default="sensor_anomalies_resilience_test")
    args = parser.parse_args()
    if args.topic == "sensor_raw" or args.hbase_table == "sensor_anomalies":
        parser.error("Refusing to use the production topic/table; choose isolated resilience resources.")
    try:
        ensure_topic(args.bootstrap, args.topic)
        ensure_hbase_table(args.hbase_host, args.hbase_port, args.hbase_table)
    except Exception as exc:
        print(f"ERROR preparing isolated resilience resources: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    print("Isolated Kafka topic and HBase table are ready.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
