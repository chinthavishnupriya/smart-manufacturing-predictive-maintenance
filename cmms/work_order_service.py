"""Idempotent CMMS work-order creation for predictive maintenance."""

import hashlib
import json
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

DB_PATH = Path(__file__).resolve().parent / "work_orders.db"
THRESHOLD = 0.80


def connect(db_path=DB_PATH):
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path, timeout=30)
    connection.execute("""
        CREATE TABLE IF NOT EXISTS work_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_key TEXT NOT NULL UNIQUE,
            machine_id TEXT NOT NULL,
            event_timestamp TEXT NOT NULL,
            failure_probability REAL NOT NULL,
            status TEXT NOT NULL DEFAULT 'OPEN',
            description TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)
    connection.commit()
    return connection


def create_work_order(event, db_path=DB_PATH, threshold=THRESHOLD):
    """Create one work order per machine and prediction timestamp."""
    machine_id = str(event["machine_id"]).strip()
    timestamp = str(event["timestamp"]).strip()
    probability = float(event["failure_probability"])

    if not machine_id or not timestamp:
        raise ValueError("machine_id and timestamp are required")
    if not 0 <= probability <= 1:
        raise ValueError("failure_probability must be between 0 and 1")

    if probability <= threshold:
        return {"created": False, "reason": "BELOW_THRESHOLD"}

    identity = f"{machine_id}|{timestamp}"
    event_key = hashlib.sha256(identity.encode("utf-8")).hexdigest()

    with connect(db_path) as conn:
        cursor = conn.execute("""
            INSERT OR IGNORE INTO work_orders (
                event_key, machine_id, event_timestamp,
                failure_probability, status, description, created_at
            ) VALUES (?, ?, ?, ?, 'OPEN', ?, ?)
        """, (
            event_key,
            machine_id,
            timestamp,
            probability,
            f"Inspect machine {machine_id}: predicted failure risk "
            f"{probability:.1%} exceeds the {threshold:.0%} threshold.",
            datetime.now(timezone.utc).isoformat()
        ))
        created = cursor.rowcount == 1

        row = conn.execute(
            "SELECT id, status FROM work_orders WHERE event_key = ?",
            (event_key,)
        ).fetchone()

    return {
        "created": created,
        "reason": "CREATED" if created else "DUPLICATE",
        "work_order_id": row[0],
        "status": row[1],
        "machine_id": machine_id,
        "failure_probability": probability
    }


def list_work_orders(db_path=DB_PATH):
    with connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute(
            "SELECT * FROM work_orders ORDER BY id DESC"
        ).fetchall()]


def process_json_file(path, db_path=DB_PATH):
    """Process one prediction JSON file without requiring an API."""
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    sensor = payload.get("sensor_data", {})
    prediction = payload.get("prediction", {})

    event = {
        "machine_id": prediction.get("machine_id", sensor.get("machine_id")),
        "timestamp": sensor.get("timestamp"),
        "failure_probability": prediction.get("failure_probability")
    }
    if None in event.values():
        raise ValueError("Prediction JSON is missing required fields")

    return create_work_order(event, db_path=db_path)


if __name__ == "__main__":
    prediction_path = Path(__file__).resolve().parents[1] / "outputs/live_prediction.json"
    print(json.dumps(process_json_file(prediction_path), indent=2))
