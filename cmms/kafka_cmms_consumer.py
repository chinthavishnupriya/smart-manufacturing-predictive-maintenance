"""Consume fresh sensor events, predict failure risk, and create CMMS work orders."""
import json
import os
import sys
from pathlib import Path

from kafka import KafkaConsumer

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from models.predict import predict
from cmms.work_order_service import create_work_order

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = "sensor_raw"
THRESHOLD = 0.80


def process_event(event, db_path=None):
    required = (
        "machine_id", "timestamp", "temperature", "vibration",
        "rpm", "pressure", "operating_hours",
    )
    missing = [key for key in required if key not in event]
    if missing:
        raise ValueError(f"Sensor event missing fields: {missing}")

    prediction = predict(event)
    probability = float(prediction["failure_probability"])

    result = {
        "machine_id": str(event["machine_id"]),
        "timestamp": str(event["timestamp"]),
        "failure_probability": probability,
    }

    if probability <= THRESHOLD:
        return {"created": False, "reason": "BELOW_THRESHOLD", **result}

    if db_path is not None:
        return create_work_order(
            result, db_path=db_path, threshold=THRESHOLD
        )

    db_path = os.getenv("CMMS_DB_PATH")
    if db_path:
        return create_work_order(
            result, db_path=Path(db_path), threshold=THRESHOLD
        )

    return create_work_order(result, threshold=THRESHOLD)


def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="smart-manufacturing-cmms-v1",
        enable_auto_commit=False,
        auto_offset_reset="latest",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    print(f"CMMS consumer listening on {KAFKA_BOOTSTRAP}/{KAFKA_TOPIC}")
    try:
        for message in consumer:
            try:
                outcome = process_event(message.value)
                print(json.dumps(outcome, default=str))
                consumer.commit()
            except Exception as exc:
                print(f"Event processing failed; offset not committed: {exc}",
                      file=sys.stderr)
                raise
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
