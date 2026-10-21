"""FastAPI service for predictive-maintenance inference and HBase monitoring."""

from pathlib import Path
import os

import happybase
import joblib
import pandas as pd

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from data_processing.feature_engineering import build_features


# ============================================================
# PATHS AND CONFIGURATION
# ============================================================

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_MODEL_FILE = ROOT / "models" / "predictive_maintenance_model.joblib"
MODEL_FILE = Path(
    os.environ.get("PREDICT_MODEL_PATH", str(DEFAULT_MODEL_FILE))
)
if not MODEL_FILE.is_absolute():
    MODEL_FILE = ROOT / MODEL_FILE

HBASE_HOST = "localhost"
HBASE_PORT = 9090
HBASE_TABLE = "sensor_anomalies"


# ============================================================
# FASTAPI APPLICATION
# ============================================================

app = FastAPI(
    title="Smart Manufacturing Predictive Maintenance API",
    version="2.0.0",
)


# ============================================================
# MACHINE LEARNING MODEL
# ============================================================

_model_bundle = None


class SensorReading(BaseModel):
    machine_id: str
    timestamp: str

    temperature: float = Field(
        ...,
        description="Temperature"
    )

    vibration: float = Field(
        ...,
        ge=0
    )

    rpm: float = Field(
        ...,
        ge=0
    )

    pressure: float

    operating_hours: float = Field(
        ...,
        ge=0
    )


def get_bundle():
    """Load the predictive-maintenance model."""

    global _model_bundle

    if _model_bundle is None:

        if not MODEL_FILE.exists():
            raise HTTPException(
                status_code=503,
                detail=(
                    "Model not found. "
                    "Run: python models/train_model.py"
                ),
            )

        _model_bundle = joblib.load(MODEL_FILE)

    return _model_bundle


# ============================================================
# HBASE CONNECTION
# ============================================================

def get_hbase_connection():
    """Create and open a connection to HBase Thrift."""

    try:

        connection = happybase.Connection(
            host=HBASE_HOST,
            port=HBASE_PORT,
            timeout=5000,
        )

        connection.open()

        return connection

    except Exception as e:

        raise HTTPException(
            status_code=503,
            detail=f"HBase connection failed: {str(e)}",
        )


# ============================================================
# HBASE DATA CONVERSION
# ============================================================

def decode(value):
    """Convert HBase bytes to a normal Python string."""

    if isinstance(value, bytes):
        return value.decode("utf-8")

    return value


def row_to_dict(row_key, data):
    """
    Convert an HBase row into a normal dictionary.

    Example:

    b'metrics:temperature'
        becomes
    temperature
    """

    result = {
        "row_key": decode(row_key)
    }

    for key, value in data.items():

        key = decode(key)
        value = decode(value)

        if ":" in key:

            family, column = key.split(":", 1)

            result[column] = value

        else:

            result[key] = value

    return result


# ============================================================
# BASIC API ENDPOINTS
# ============================================================

@app.get("/")
def root():

    return {
        "service": "Smart Manufacturing Predictive Maintenance",
        "status": "running",
    }


@app.get("/health")
def health():
    """Check API, ML model and HBase status."""

    hbase_status = False

    try:

        connection = happybase.Connection(
            host=HBASE_HOST,
            port=HBASE_PORT,
            timeout=3000,
        )

        connection.open()

        hbase_status = True

        connection.close()

    except Exception:

        hbase_status = False

    return {
        "status": "healthy",
        "model_available": MODEL_FILE.exists(),
        "hbase_available": hbase_status,
    }


# ============================================================
# ML PREDICTION ENDPOINT
# ============================================================


@app.post("/predict")
def predict(reading: SensorReading):
    """Predict machine failure risk and detect sensor anomalies."""

    bundle = get_bundle()

    # Build the same features used during model training
    df = pd.DataFrame([reading.model_dump()])
    df = build_features(df)
    x = df[bundle["features"]]

    # Predict machine failure risk
    probability = float(
        bundle["model"].predict_proba(x)[0, 1]
    )

    failure = int(probability >= bundle["threshold"])

    # Detect sensor anomalies using the streaming thresholds
    temperature_anomaly = (
        reading.temperature < 40 or reading.temperature > 90
    )
    vibration_anomaly = (
        reading.vibration < 0 or reading.vibration > 5
    )
    rpm_anomaly = (
        reading.rpm < 500 or reading.rpm > 2500
    )
    pressure_anomaly = (
        reading.pressure < 2 or reading.pressure > 8
    )

    anomaly_flags = {
        "temperature": temperature_anomaly,
        "vibration": vibration_anomaly,
        "rpm": rpm_anomaly,
        "pressure": pressure_anomaly,
    }

    detected_anomalies = [
        sensor
        for sensor, is_anomaly in anomaly_flags.items()
        if is_anomaly
    ]

    return {
        "machine_id": reading.machine_id,
        "failure_probability": round(probability, 4),
        "failure_prediction": failure,
        "failure_status": (
            "FAILURE RISK" if failure else "NORMAL"
        ),
        "failure_threshold": bundle["threshold"],
        "anomaly_status": (
            "ANOMALY" if detected_anomalies else "NORMAL"
        ),
        "detected_anomalies": detected_anomalies,
    }


# ============================================================
# GET ALL MACHINES
# ============================================================

@app.get("/machines")
def get_machines():
    """
    Return the latest HBase record for every machine.
    """

    connection = get_hbase_connection()

    try:

        table = connection.table(
            HBASE_TABLE
        )

        machines = {}

        # Scan complete HBase table
        for row_key, data in table.scan():

            record = row_to_dict(
                row_key,
                data
            )

            key = record["row_key"]
            # Skip legacy rows that do not contain a complete time window.
            if key == "M001_20261007_0620":
                continue
            # Row key format starts with machine ID.
            #
            # Example:
            # M001_20261008_0933_0943
            #
            # Remove the trailing date and window fields.
            # Supports machine IDs containing underscores.
            # Remove the timestamp/window suffix:
            # YYYYMMDD_HHMM_HHMM
            # Extract machine ID from the row key.
            parts = key.split("_")

            if (
                len(parts) >= 4
                and len(parts[-3]) == 8
                and parts[-3].isdigit()
                and parts[-2].isdigit()
                and parts[-1].isdigit()
            ):
                machine_id = "_".join(parts[:-3])
            else:
                machine_id = key

            # Keep only the latest window for each machine.
            if (
                machine_id not in machines
                or key > machines[machine_id]["row_key"]
            ):
                machines[machine_id] = record
        return {
            "count": len(machines),
            "machines": list(
                machines.values()
            ),
        }

    finally:

        connection.close()


# ============================================================
# GET LATEST RECORD FOR ONE MACHINE
# ============================================================

@app.get("/machine/{machine_id}/latest")
def get_latest_machine(machine_id: str):
    """
    Return the latest HBase record for one machine.
    """

    connection = get_hbase_connection()

    try:

        table = connection.table(
            HBASE_TABLE
        )

        latest = None

        # HappyBase uses row_prefix, NOT prefix.
        for row_key, data in table.scan(
            row_prefix=machine_id.encode("utf-8")
        ):

            record = row_to_dict(
                row_key,
                data
            )

            # Row keys contain timestamp/window information,
            # so lexicographical comparison gives the latest
            # row when the timestamp fields are zero-padded.
            if (
                latest is None
                or record["row_key"] > latest["row_key"]
            ):

                latest = record

        if latest is None:

            raise HTTPException(
                status_code=404,
                detail=(
                    f"No HBase data found "
                    f"for machine {machine_id}"
                ),
            )

        return latest

    finally:

        connection.close()


# ============================================================
# GET LATEST ANOMALIES
# ============================================================

@app.get("/anomalies/latest")
def get_latest_anomalies():
    """
    Return the latest record for each machine
    where the machine is currently anomalous.
    """

    connection = get_hbase_connection()

    try:

        table = connection.table(
            HBASE_TABLE
        )

        latest_records = {}

        # Scan HBase
        for row_key, data in table.scan():

            record = row_to_dict(
                row_key,
                data
            )

            machine_id = (
                record["row_key"]
                .split("_")[0]
            )

            # Keep latest record for each machine
            if (
                machine_id not in latest_records
                or record["row_key"]
                > latest_records[machine_id]["row_key"]
            ):

                latest_records[machine_id] = record

        # Select only ANOMALY records
        anomalies = [
            record
            for record in latest_records.values()
            if record.get("status") == "ANOMALY"
        ]

        return {
            "count": len(anomalies),
            "anomalies": anomalies,
        }

    finally:

        connection.close()