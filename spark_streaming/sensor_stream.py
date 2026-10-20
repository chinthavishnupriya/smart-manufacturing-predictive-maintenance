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
    count,
    from_json,
    lit,
    max as spark_max,
    sum as spark_sum,
    to_timestamp,
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

    .withWatermark(
        "event_time",
        "10 minutes"
    )

    .groupBy(
        window(
            col("event_time"),
            "10 minutes",
            "1 minute"
        ),
        col("machine_id")
    )

    .agg(
        # Sensor averages
        avg("temperature").alias(
            "avg_temperature"
        ),

        avg("vibration").alias(
            "avg_vibration"
        ),

        avg("rpm").alias(
            "avg_rpm"
        ),

        avg("pressure").alias(
            "avg_pressure"
        ),

        # IMPORTANT:
        # Required for RUL prediction.
        avg("operating_hours").alias(
            "avg_operating_hours"
        ),

        # Variance
        var_pop("temperature").alias(
            "temperature_variance"
        ),

        var_pop("vibration").alias(
            "vibration_variance"
        ),

        # Latest Kafka record timestamp in this machine/window.
        spark_max("kafka_ingest_time").alias(
            "latest_kafka_ingest_time"
        ),

        # Event count
        count("*").alias(
            "event_count"
        ),

        # Individual anomaly counts
        spark_sum(
            "temperature_anomaly"
        ).alias(
            "temperature_anomaly_count"
        ),

        spark_sum(
            "vibration_anomaly"
        ).alias(
            "vibration_anomaly_count"
        ),

        spark_sum(
            "rpm_anomaly"
        ).alias(
            "rpm_anomaly_count"
        ),

        spark_sum(
            "pressure_anomaly"
        ).alias(
            "pressure_anomaly_count"
        ),

        # Total anomalous events
        spark_sum(
            "has_anomaly"
        ).alias(
            "anomalous_event_count"
        ),
    )
)


# ============================================================
# CALCULATE ANOMALY METRICS
# ============================================================

aggregated_df = (
    aggregated_df

    .withColumn(
        "total_anomaly_count",
        col("temperature_anomaly_count")
        + col("vibration_anomaly_count")
        + col("rpm_anomaly_count")
        + col("pressure_anomaly_count")
    )

    .withColumn(
        "anomaly_rate",
        when(
            col("event_count") > 0,
            (
                col("anomalous_event_count")
                / col("event_count")
                * 100
            )
        ).otherwise(0.0)
    )

    .withColumn(
        "anomaly_status",
        when(
            col("anomalous_event_count") > 0,
            lit("ANOMALY")
        ).otherwise(
            lit("NORMAL")
        )
    )
)


# ============================================================
# HBASE WRITE FUNCTION
# ============================================================

def write_to_hbase(batch_df, batch_id):
    batch_start_time = time.perf_counter()

    print()
    print("----------------------------------------------")
    print(f"HBase Batch ID: {batch_id}")
    print("----------------------------------------------")

    hbase_connection = None

    try:
        # Collect this microbatch once, avoiding a separate emptiness action.
        rows = list(batch_df.toLocalIterator())
        if not rows:
            print("No records in this batch.")
            return

        # Prepare all model inputs using the trained feature names.
        rul_inputs = pd.DataFrame([{
            "temperature": row["avg_temperature"],
            "vibration": row["avg_vibration"],
            "rpm": row["avg_rpm"],
            "pressure": row["avg_pressure"],
            "operating_hours": row["avg_operating_hours"],
        } for row in rows])

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
            for row, prediction in zip(rows, predicted_values):
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
                        hbase_value(row["avg_temperature"]).encode(),
                    b"metrics:avg_vibration":
                        hbase_value(row["avg_vibration"]).encode(),
                    b"metrics:avg_rpm":
                        hbase_value(row["avg_rpm"]).encode(),
                    b"metrics:avg_pressure":
                        hbase_value(row["avg_pressure"]).encode(),
                    b"metrics:avg_operating_hours":
                        hbase_value(row["avg_operating_hours"]).encode(),
                    b"metrics:temperature_variance":
                        hbase_value(row["temperature_variance"]).encode(),
                    b"metrics:vibration_variance":
                        hbase_value(row["vibration_variance"]).encode(),
                    b"metrics:event_count":
                        hbase_value(row["event_count"]).encode(),
                    b"metrics:latest_kafka_ingest_time":
                        hbase_value(ingest_time).encode(),
                    b"metrics:kafka_to_hbase_start_ms":
                        hbase_value(round(kafka_to_hbase_start_ms, 3)).encode(),
                    b"metrics:predicted_rul_hours":
                        hbase_value(predicted_rul_hours).encode(),
                    b"metrics:predicted_rul_minutes":
                        hbase_value(predicted_rul_minutes).encode(),
                    b"anomalies:temperature_count":
                        hbase_value(row["temperature_anomaly_count"]).encode(),
                    b"anomalies:vibration_count":
                        hbase_value(row["vibration_anomaly_count"]).encode(),
                    b"anomalies:rpm_count":
                        hbase_value(row["rpm_anomaly_count"]).encode(),
                    b"anomalies:pressure_count":
                        hbase_value(row["pressure_anomaly_count"]).encode(),
                    b"anomalies:anomalous_event_count":
                        hbase_value(row["anomalous_event_count"]).encode(),
                    b"anomalies:total_anomaly_count":
                        hbase_value(row["total_anomaly_count"]).encode(),
                    b"anomalies:anomaly_rate":
                        hbase_value(row["anomaly_rate"]).encode(),
                    b"anomalies:status":
                        hbase_value(row["anomaly_status"]).encode(),
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
        processingTime="5 seconds"
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
