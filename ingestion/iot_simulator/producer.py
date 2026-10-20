import argparse
import csv
import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer


# ============================================================
# Configuration
# ============================================================

DEFAULT_KAFKA_URL = "localhost:9092"
DEFAULT_TOPIC = "sensor_raw"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CSV = PROJECT_ROOT / "data" / "raw" / "sensor_data_100k.csv"


# ============================================================
# Kafka Producer
# ============================================================

def create_producer(kafka_url: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=kafka_url,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


# ============================================================
# Current Timestamp
# ============================================================

def current_timestamp() -> str:
    """
    Generate the current UTC timestamp.

    Spark will convert this string into its event_time column.
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# ============================================================
# Convert CSV Row to Sensor Event
# ============================================================

def create_sensor_event(row: dict) -> dict:
    """
    Convert a CSV row into the Kafka sensor event format.

    IMPORTANT:
    The timestamp is intentionally replaced with the current
    UTC time so the project behaves like a real-time system.
    """

    return {
        "machine_id": str(row["machine_id"]),
        "timestamp": current_timestamp(),

        "temperature": float(row["temperature"]),
        "vibration": float(row["vibration"]),
        "rpm": float(row["rpm"]),
        "pressure": float(row["pressure"]),
        "operating_hours": float(row["operating_hours"]),
        "failure": int(row["failure"]),
    }


# ============================================================
# Read CSV
# ============================================================

def read_sensor_data(csv_file: str):
    """
    Read sensor records from the CSV file.
    """

    csv_path = Path(csv_file)

    if not csv_path.exists():
        raise FileNotFoundError(
            f"CSV file not found: {csv_path}"
        )

    with open(csv_path, "r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        required_columns = {
            "machine_id",
            "timestamp",
            "temperature",
            "vibration",
            "rpm",
            "pressure",
            "operating_hours",
            "failure",
        }

        missing_columns = required_columns - set(reader.fieldnames or [])

        if missing_columns:
            raise ValueError(
                f"Missing CSV columns: {sorted(missing_columns)}"
            )

        for row in reader:
            yield row


# ============================================================
# Send One Event
# ============================================================

def send_event(
    producer: KafkaProducer,
    topic: str,
    event: dict,
):
    """
    Send one sensor event to Kafka.
    """

    future = producer.send(topic, value=event)

    # Wait for Kafka acknowledgement.
    future.get(timeout=10)

    print(
        f"[SENT] "
        f"machine={event['machine_id']} | "
        f"time={event['timestamp']} | "
        f"temp={event['temperature']:.2f} | "
        f"vibration={event['vibration']:.2f} | "
        f"rpm={event['rpm']:.2f} | "
        f"pressure={event['pressure']:.2f} | "
        f"failure={event['failure']}"
    )


# ============================================================
# Main Producer Logic
# ============================================================

def run_producer(
    csv_file: str,
    kafka_url: str,
    topic: str,
    delay: float,
    limit: int | None,
    continuous: bool,
):
    producer = create_producer(kafka_url)

    print("=" * 60)
    print("Smart Manufacturing IoT Simulator")
    print("=" * 60)
    print(f"Kafka Server : {kafka_url}")
    print(f"Kafka Topic  : {topic}")
    print(f"CSV File     : {csv_file}")
    print(f"Delay        : {delay} seconds")
    print(f"Continuous   : {continuous}")
    print("=" * 60)

    sent_count = 0

    try:
        while True:

            for row in read_sensor_data(csv_file):

                event = create_sensor_event(row)

                send_event(
                    producer=producer,
                    topic=topic,
                    event=event,
                )

                sent_count += 1

                if delay > 0:
                    time.sleep(delay)

                # Stop after requested number of records.
                if limit is not None and sent_count >= limit:
                    print()
                    print("=" * 60)
                    print(f"Finished sending {sent_count} events.")
                    print("=" * 60)
                    return

            # If continuous mode is enabled, start reading
            # the CSV again and continue producing events.
            if not continuous:
                break

            print()
            print("[INFO] CSV reached end. Restarting simulation...")
            print()

    except KeyboardInterrupt:
        print()
        print("[INFO] Producer stopped by user.")

    finally:
        producer.flush()
        producer.close()

        print()
        print(f"[INFO] Total events sent: {sent_count}")


# ============================================================
# Command-Line Arguments
# ============================================================

def parse_arguments():

    parser = argparse.ArgumentParser(
        description="Smart Manufacturing Kafka IoT Simulator"
    )

    parser.add_argument(
        "--csv",
        default=str(DEFAULT_CSV),
        help="Path to sensor CSV file",
    )

    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Delay between events in seconds",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of events to send",
    )

    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Continuously replay the CSV",
    )

    parser.add_argument(
        "--kafka-url",
        default=DEFAULT_KAFKA_URL,
        help="Kafka bootstrap server",
    )

    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC,
        help="Kafka topic",
    )

    return parser.parse_args()


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":

    args = parse_arguments()

    run_producer(
        csv_file=args.csv,
        kafka_url=args.kafka_url,
        topic=args.topic,
        delay=args.delay,
        limit=args.limit,
        continuous=args.continuous,
    )