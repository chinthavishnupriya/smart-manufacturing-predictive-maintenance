import os
import time
# ============================================================
# Smart Manufacturing IoT
# Spark Structured Streaming + Kafka + HBase + RUL Prediction
# ============================================================

import sys
from pathlib import Path

import joblib
import pandas as pd
import happybase

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    collect_set,
    count,
    from_json,
    length,
    lit,
    max as spark_max,
    sha2,
    struct,
    sum as spark_sum,
    to_json,
    to_timestamp,
    trim,
    var_pop,
    variance,
    when,
    window,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_DIR = Path(__file__).resolve().parent.parent

RUL_MODEL_PATH = (
    PROJECT_DIR
    / "models"
    / "rul_model.joblib"
)


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "sensor_raw"

HBASE_HOST = "localhost"
HBASE_PORT = 9090
HBASE_TABLE = "sensor_anomalies"

# IMPORTANT:
# This is a NEW checkpoint for the RUL-enabled aggregation.
CHECKPOINT_LOCATION = (
    "file:///home/vishnupriya/"
    "smart-manufacturing-checkpoints/"
    "sensor_stream_hbase_rul_latency_v3"
)

KAFKA_PACKAGE = (
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
)


# ============================================================
# LOAD RUL MODEL
# ============================================================

print()
print("Loading RUL model...")
print(f"RUL Model Path   : {RUL_MODEL_PATH}")

if not RUL_MODEL_PATH.exists():
    print("ERROR: RUL model file not found.")
    sys.exit(1)

rul_model = joblib.load(RUL_MODEL_PATH)

print("RUL Model        : Loaded successfully")


# ============================================================
# CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("SmartManufacturingStreaming")
    .master("local[2]")
    .config("spark.driver.memory", "768m")
    .config("spark.executor.memory", "768m")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.default.parallelism", "2")
    .config("spark.sql.adaptive.enabled", "true")
    .config(
        "spark.jars.packages",
        KAFKA_PACKAGE
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# STARTUP INFORMATION
# ============================================================

print()
print("==============================================")
print("Smart Manufacturing Streaming Started")
print("==============================================")
print(f"Kafka Topic       : {KAFKA_TOPIC}")
print(f"Kafka Server      : {KAFKA_SERVER}")
print("Kafka Connector   : Spark SQL Kafka 4.2.0")
print("Window            : 10 minutes")
print("Sliding Interval  : 1 minute")
print("Watermark         : 10 minutes")
print("Anomaly Detection : Enabled")
print("Anomaly Rate      : Event-based percentage")
print("RUL Prediction    : Enabled")
print("HBase             : Enabled")
print(f"HBase Server      : {HBASE_HOST}:{HBASE_PORT}")
print(f"HBase Table       : {HBASE_TABLE}")
print("Checkpoint        : WSL Linux filesystem")
print("Spark Master      : local[2]")
print("Shuffle Partitions: 2")
print("==============================================")
print()


# ============================================================
# KAFKA SENSOR SCHEMA
# ============================================================

sensor_schema = StructType([
    StructField("machine_id", StringType(), True),
    StructField("event_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("vibration", DoubleType(), True),
    StructField("rpm", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("operating_hours", DoubleType(), True),
    StructField("failure", IntegerType(), True),
])


# ============================================================
# READ FROM KAFKA
# ============================================================

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)


# ============================================================
# PARSE KAFKA JSON
# ============================================================

sensor_df = (
    kafka_df
    .selectExpr(
        "CAST(value AS STRING) AS json_value",
        "timestamp AS kafka_ingest_time"
    )
    .select(
        from_json(
            col("json_value"),
            sensor_schema
        ).alias("data"),
        col("kafka_ingest_time")
    )
    .select("data.*", "kafka_ingest_time")
    .withColumn(
        "event_time",
        to_timestamp(col("timestamp"))
    )
    .filter(
        col("machine_id").isNotNull()
    )
    .filter(
        col("event_time").isNotNull()
    )
    .withColumn(
        "event_id",
        when(
            col("event_id").isNotNull() & (length(trim(col("event_id"))) > 0),
            trim(col("event_id")),
        ).otherwise(
            sha2(
                to_json(
                    struct(
                        col("machine_id"),
                        col("timestamp"),
                        col("temperature"),
                        col("vibration"),
                        col("rpm"),
                        col("pressure"),
                        col("operating_hours"),
                        col("failure"),
                    )
                ),
                256,
            )
        )
    )
)


# ============================================================
# ANOMALY DETECTION
# ============================================================

sensor_with_anomalies = (
    sensor_df

    # Temperature anomaly
    .withColumn(
        "temperature_anomaly",
        when(
            (col("temperature") < 40)
            | (col("temperature") > 90),
            1
        ).otherwise(0)
    )

    # Vibration anomaly
    .withColumn(
        "vibration_anomaly",
        when(
            (col("vibration") < 0)
            | (col("vibration") > 5),
            1
        ).otherwise(0)
    )

    # RPM anomaly
    .withColumn(
        "rpm_anomaly",
        when(
            (col("rpm") < 500)
            | (col("rpm") > 2500),
            1
        ).otherwise(0)
    )

    # Pressure anomaly
    .withColumn(
        "pressure_anomaly",
        when(
            (col("pressure") < 2)
            | (col("pressure") > 8),
            1
        ).otherwise(0)
    )
)


# ============================================================
# OVERALL ANOMALY FLAG
# ============================================================

sensor_with_anomalies = (
    sensor_with_anomalies
    .withColumn(
        "has_anomaly",
        when(
            (col("temperature_anomaly") == 1)
            | (col("vibration_anomaly") == 1)
            | (col("rpm_anomaly") == 1)
            | (col("pressure_anomaly") == 1),
            1
        ).otherwise(0)
    )
)


# ============================================================
# WATERMARK + WINDOW AGGREGATION
# ============================================================

aggregated_df = (
    sensor_with_anomalies
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        window(col("event_time"), "10 minutes", "1 minute"),
        col("machine_id"),
    )
    .agg(
        # Keep the distinct event payloads inside the window's single streaming
        # aggregate. This avoids chaining separate stateful deduplication and
        # aggregation operators in UPDATE output mode. The HBase callback
        # deduplicates by event_id again before computing every metric.
        collect_set(
            struct(
                col("event_id").alias("event_id"),
                col("temperature").alias("temperature"),
                col("vibration").alias("vibration"),
                col("rpm").alias("rpm"),
                col("pressure").alias("pressure"),
                col("operating_hours").alias("operating_hours"),
            )
        ).alias("event_records"),
        spark_max("kafka_ingest_time").alias("latest_kafka_ingest_time"),
    )
)


# ============================================================
# HBASE WRITE FUNCTION
# ============================================================
# ============================================================

def calculate_window_metrics(window_row):
    """Calculate metrics from one window's distinct logical sensor events.

    Explicit producer event IDs are preferred. Legacy events receive a stable
    payload fingerprint upstream in sensor_df. A single stateful window
    aggregation is used, so this logic avoids a separate streaming
    dropDuplicates operator followed by another stateful aggregation.
    """
    unique_events = {}
    for record in (window_row["event_records"] or []):
        event = record.asDict(recursive=True)
        event_id = str(event["event_id"])
        # If a producer accidentally reuses an ID with conflicting values,
        # retain the first value encountered for that ID in this window.
        unique_events.setdefault(event_id, event)

    if not unique_events:
        return None

    frame = pd.DataFrame(list(unique_events.values()))

    def numeric(name):
        return pd.to_numeric(frame[name], errors="coerce")

    temperature = numeric("temperature")
    vibration = numeric("vibration")
    rpm = numeric("rpm")
    pressure = numeric("pressure")
    operating_hours = numeric("operating_hours")

    temperature_anomaly = ((temperature < 40) | (temperature > 90)).fillna(False)
    vibration_anomaly = ((vibration < 0) | (vibration > 5)).fillna(False)
    rpm_anomaly = ((rpm < 500) | (rpm > 2500)).fillna(False)
    pressure_anomaly = ((pressure < 2) | (pressure > 8)).fillna(False)
    anomalous_event = (
        temperature_anomaly
        | vibration_anomaly
        | rpm_anomaly
        | pressure_anomaly
    )

    temperature_count = int(temperature_anomaly.sum())
    vibration_count = int(vibration_anomaly.sum())
    rpm_count = int(rpm_anomaly.sum())
    pressure_count = int(pressure_anomaly.sum())
    event_count = len(unique_events)
    anomalous_event_count = int(anomalous_event.sum())

    return {
        "avg_temperature": temperature.mean(),
        "avg_vibration": vibration.mean(),
        "avg_rpm": rpm.mean(),
        "avg_pressure": pressure.mean(),
        "avg_operating_hours": operating_hours.mean(),
        "temperature_variance": temperature.var(ddof=0),
        "vibration_variance": vibration.var(ddof=0),
        "event_count": event_count,
        "temperature_anomaly_count": temperature_count,
        "vibration_anomaly_count": vibration_count,
        "rpm_anomaly_count": rpm_count,
        "pressure_anomaly_count": pressure_count,
        "anomalous_event_count": anomalous_event_count,
        "total_anomaly_count": (
            temperature_count + vibration_count + rpm_count + pressure_count
        ),
        "anomaly_rate": (anomalous_event_count / event_count) * 100.0,
        "anomaly_status": "ANOMALY" if anomalous_event_count else "NORMAL",
    }


def write_to_hbase(batch_df, batch_id):
    batch_start_time = time.perf_counter()

    print()
    print("----------------------------------------------")
    print(f"HBase Batch ID: {batch_id}")
    print("----------------------------------------------")

    hbase_connection = None

    try:
        # Each Spark window row contains the unique event payloads currently
        # known for that window. Compute all metrics from event IDs, not raw
        # Kafka record count, so duplicate deliveries do not inflate values.
        rows = batch_df.collect()
        if not rows:
            print("No records in this batch.")
            return

        metric_rows = [calculate_window_metrics(row) for row in rows]
        valid_pairs = [
            (row, metrics)
            for row, metrics in zip(rows, metric_rows)
            if metrics is not None
        ]
        rows = [pair[0] for pair in valid_pairs]
        metric_rows = [pair[1] for pair in valid_pairs]
        if not rows:
            print("No distinct events in this batch.")
            return

        # Prepare all model inputs using the trained feature names.
        rul_inputs = pd.DataFrame([{
            "temperature": metrics["avg_temperature"],
            "vibration": metrics["avg_vibration"],
            "rpm": metrics["avg_rpm"],
            "pressure": metrics["avg_pressure"],
            "operating_hours": metrics["avg_operating_hours"],
        } for metrics in metric_rows])

        prediction_start = time.perf_counter()
        predicted_values = rul_model.predict(rul_inputs)
        print(
            f"TIMING RUL batch prediction: "
            f"{time.perf_counter() - prediction_start:.4f}s "
            f"for {len(rows)} rows"
        )

        connection_start = time.perf_counter()
        hbase_connection = happybase.Connection(
            host=HBASE_HOST,
            port=HBASE_PORT,
            timeout=30000,
            autoconnect=True
        )
        hbase_connection.open()
        print(
            f"TIMING HBase connection: "
            f"{time.perf_counter() - connection_start:.3f}s"
        )

        table = hbase_connection.table(HBASE_TABLE)
        record_count = 0
        latency_records = []
        write_start = time.perf_counter()

        # One HBase batch context for this microbatch.
        with table.batch(batch_size=100) as hbase_batch:
            for row, metrics, prediction in zip(rows, metric_rows, predicted_values):
                machine_id = row["machine_id"]
                window_start = row["window"]["start"]
                window_end = row["window"]["end"]
                ingest_time = row["latest_kafka_ingest_time"]

                start_string = window_start.strftime("%Y%m%d_%H%M")
                end_string = window_end.strftime("%H%M")
                row_key = f"{machine_id}_{start_string}_{end_string}"

                predicted_rul_hours = max(0.0, float(prediction))
                predicted_rul_minutes = predicted_rul_hours * 60

                def hbase_value(value):
                    return "None" if value is None else str(value)

                kafka_to_hbase_start_ms = (
                    (pd.Timestamp.now().to_pydatetime() - ingest_time)
                    .total_seconds() * 1000
                )

                hbase_data = {
                    b"metrics:avg_temperature":
                        hbase_value(metrics["avg_temperature"]).encode(),
                    b"metrics:avg_vibration":
                        hbase_value(metrics["avg_vibration"]).encode(),
                    b"metrics:avg_rpm":
                        hbase_value(metrics["avg_rpm"]).encode(),
                    b"metrics:avg_pressure":
                        hbase_value(metrics["avg_pressure"]).encode(),
                    b"metrics:avg_operating_hours":
                        hbase_value(metrics["avg_operating_hours"]).encode(),
                    b"metrics:temperature_variance":
                        hbase_value(metrics["temperature_variance"]).encode(),
                    b"metrics:vibration_variance":
                        hbase_value(metrics["vibration_variance"]).encode(),
                    b"metrics:event_count":
                        hbase_value(metrics["event_count"]).encode(),
                    b"metrics:latest_kafka_ingest_time":
                        hbase_value(ingest_time).encode(),
                    b"metrics:kafka_to_hbase_start_ms":
                        hbase_value(round(kafka_to_hbase_start_ms, 3)).encode(),
                    b"metrics:predicted_rul_hours":
                        hbase_value(predicted_rul_hours).encode(),
                    b"metrics:predicted_rul_minutes":
                        hbase_value(predicted_rul_minutes).encode(),
                    b"anomalies:temperature_count":
                        hbase_value(metrics["temperature_anomaly_count"]).encode(),
                    b"anomalies:vibration_count":
                        hbase_value(metrics["vibration_anomaly_count"]).encode(),
                    b"anomalies:rpm_count":
                        hbase_value(metrics["rpm_anomaly_count"]).encode(),
                    b"anomalies:pressure_count":
                        hbase_value(metrics["pressure_anomaly_count"]).encode(),
                    b"anomalies:anomalous_event_count":
                        hbase_value(metrics["anomalous_event_count"]).encode(),
                    b"anomalies:total_anomaly_count":
                        hbase_value(metrics["total_anomaly_count"]).encode(),
                    b"anomalies:anomaly_rate":
                        hbase_value(metrics["anomaly_rate"]).encode(),
                    b"anomalies:status":
                        hbase_value(metrics["anomaly_status"]).encode(),
                }

                hbase_batch.put(row_key.encode(), hbase_data)
                latency_records.append((machine_id, ingest_time))
                record_count += 1

        print(
            f"TIMING HBase batch write: "
            f"{time.perf_counter() - write_start:.4f}s"
        )

        for machine_id, ingest_time in latency_records:
            elapsed_ms = (
                (pd.Timestamp.now().to_pydatetime() - ingest_time)
                .total_seconds() * 1000
            )
            print(
                f"LATENCY machine={machine_id} "
                f"kafka_to_hbase_complete_ms={elapsed_ms:.3f}"
            )

        print(
            "HBase batch write completed successfully. "
            f"Records written: {record_count}"
        )
        print(f"RUL predictions generated: {len(predicted_values)}")

    except Exception as error:
        print("ERROR writing to HBase:")
        print(f"{type(error).__name__}: {error}")
        raise

    finally:
        if hbase_connection is not None:
            try:
                hbase_connection.close()
                print("HBase connection closed.")
            except Exception:
                pass

        print(
            f"TIMING total HBase function: "
            f"{time.perf_counter() - batch_start_time:.3f}s"
        )


# ============================================================
# START STREAMING QUERY
# ============================================================

query = (
    aggregated_df
    .writeStream
    .outputMode("update")
    .foreachBatch(write_to_hbase)
    .option(
        "checkpointLocation",
        CHECKPOINT_LOCATION
    )
    .trigger(
        processingTime=os.getenv("SPARK_TRIGGER_INTERVAL", "1 second")
    )
    .start()
)


# ============================================================
# WAIT FOR STREAMING QUERY
# ============================================================

try:

    query.awaitTermination()

except KeyboardInterrupt:

    print()
    print("Stopping Smart Manufacturing Streaming...")

    query.stop()
    spark.stop()

    print("Streaming stopped.")

except Exception as error:

    print()
    print(
        "Streaming query failed:"
    )
    print(
        f"{type(error).__name__}: {error}"
    )

    try:
        query.stop()
    except Exception:
        pass

    spark.stop()
    raise
