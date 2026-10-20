from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, sum, when, round


spark = (
    SparkSession.builder
    .appName("SmartManufacturingHiveAnalytics")
    .master("local[2]")
    .config("spark.driver.memory", "768m")
    .config("spark.executor.memory", "768m")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n" + "=" * 60)
print("SMART MANUFACTURING - HIVE ANALYTICS")
print("=" * 60)

# Read historical ORC data from Hive
df = spark.table("smart_manufacturing_db.sensor_history")

print(f"\nTotal historical records: {df.count()}")

print("\nSchema:")
df.printSchema()

# --------------------------------------------------
# 1. Failure distribution
# --------------------------------------------------

print("\n--- Failure Distribution ---")

failure_analysis = (
    df.groupBy("failure")
      .agg(
          count("*").alias("record_count")
      )
      .orderBy("failure")
)

failure_analysis.show()

# --------------------------------------------------
# 2. Normal vs Failure sensor averages
# --------------------------------------------------

print("\n--- Normal vs Failure Sensor Averages ---")

sensor_analysis = (
    df.groupBy("failure")
      .agg(
          round(avg("temperature"), 2).alias("avg_temperature"),
          round(avg("vibration"), 2).alias("avg_vibration"),
          round(avg("rpm"), 2).alias("avg_rpm"),
          round(avg("pressure"), 2).alias("avg_pressure")
      )
      .orderBy("failure")
)

sensor_analysis.show()

# --------------------------------------------------
# 3. Machine-wise failure count
# --------------------------------------------------

print("\n--- Top Machines by Failure Count ---")

machine_analysis = (
    df.groupBy("machine_id")
      .agg(
          sum(
              when(df.failure == 1, 1).otherwise(0)
          ).alias("failure_count"),
          count("*").alias("total_records")
      )
      .orderBy("failure_count", ascending=False)
)

machine_analysis.show(10)

print("\n" + "=" * 60)
print("Hive historical analytics completed successfully.")
print("=" * 60 + "\n")

spark.stop()