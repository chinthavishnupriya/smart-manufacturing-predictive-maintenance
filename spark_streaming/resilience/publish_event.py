#!/usr/bin/env python3
"""Publish an event JSON file to Kafka one or more times."""
import argparse
import json
import os
from pathlib import Path
import hashlib
from kafka import KafkaProducer

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-file", required=True)
    parser.add_argument("--copies", type=int, default=1)
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "sensor_raw"))
    args = parser.parse_args()
    if args.copies < 1:
        parser.error("--copies must be >= 1")
    event = json.loads(Path(args.event_file).read_text(encoding="utf-8"))
    required = {"machine_id", "timestamp", "temperature", "vibration", "rpm", "pressure", "operating_hours", "failure"}
    if not required.issubset(event):
        parser.error(f"event JSON missing fields: {sorted(required - set(event))}")

    # Preserve an upstream event ID when supplied. For legacy payloads, use a
    # stable fingerprint so replaying the same payload preserves its identity.
    if not str(event.get("event_id", "")).strip():
        canonical_payload = json.dumps(
            {key: value for key, value in event.items() if key != "event_id"},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        event["event_id"] = hashlib.sha256(canonical_payload).hexdigest()
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
        acks="all",
        retries=5,
    )
    try:
        futures = [
            producer.send(args.topic, key=event["machine_id"], value=event)
            for _ in range(args.copies)
        ]
        metadata = [future.get(timeout=30) for future in futures]
        for item in metadata:
            print(f"ACK topic={item.topic} partition={item.partition} offset={item.offset} machine_id={event['machine_id']}")
    finally:
        producer.flush(timeout=30)
        producer.close(timeout=30)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
