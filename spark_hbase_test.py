from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)

import happybase


# --------------------------------------------------
# 1. Spark session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("SparkHBaseTest")
    .master("local[1]")
    .config("spark.driver.memory", "512m")
    .config("spark.executor.memory", "512m")
    .config("spark.hadoop.fs.defaultFS", "file:///")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 2. Create test anomaly data
# --------------------------------------------------

schema = StructType([
    StructField("machine_id", StringType(), False),
    StructField("temperature", DoubleType(), False),
    StructField("vibration", DoubleType(), False),
    StructField("rpm", IntegerType(), False),
    StructField("pressure", DoubleType(), False),
    StructField("anomaly_status", StringType(), False),
])

data = [
    (
        "M002",
        105.0,
        7.5,
        3000,
        10.0,
        "ANOMALY",
    )
]

df = spark.createDataFrame(data, schema)

print("\nSpark test data:")
df.show(truncate=False)


# --------------------------------------------------
# 3. Connect to HBase through Thrift
# --------------------------------------------------

print("Connecting to HBase...")

connection = happybase.Connection(
    host="localhost",
    port=9090,
    timeout=10000,
)

connection.open()

print("Connected to HBase!")


# --------------------------------------------------
# 4. Get HBase table
# --------------------------------------------------

table = connection.table("sensor_anomalies")


# --------------------------------------------------
# 5. Write Spark row to HBase
# --------------------------------------------------

for row in df.collect():

    row_key = f"{row.machine_id}_spark_test"

    table.put(
        row_key.encode(),
        {
            b"metrics:temperature": str(row.temperature).encode(),
            b"metrics:vibration": str(row.vibration).encode(),
            b"metrics:rpm": str(row.rpm).encode(),
            b"metrics:pressure": str(row.pressure).encode(),
            b"anomalies:status": row.anomaly_status.encode(),
        }
    )

    print(f"Written to HBase: {row_key}")


# --------------------------------------------------
# 6. Read the record back
# --------------------------------------------------

row_key = b"M002_spark_test"

result = table.row(row_key)

print("\nData retrieved from HBase:")

for key, value in result.items():
    print(f"{key.decode()} = {value.decode()}")


# --------------------------------------------------
# 7. Close connections
# --------------------------------------------------

connection.close()
spark.stop()

print("\nSpark → HBase test completed successfully!")
