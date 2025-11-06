import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# === Environment variables ===
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "events")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/opt/spark-output/parquet")

# Derive CSV output path (same parent folder as Parquet)
CSV_PATH = OUTPUT_PATH.replace("parquet", "csv")

# === Define schema ===
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("ts", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("source", StringType(), True),
])

# === Create SparkSession ===
spark = (
    SparkSession.builder
    .appName("KafkaSparkExample")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("✅ Spark session started!")
print(f"Kafka: {KAFKA_BOOTSTRAP} | topic: {TOPIC}")
print(f"Writing Parquet to: {OUTPUT_PATH}")
print(f"Writing CSV to: {CSV_PATH}")

# === Read from Kafka ===
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# === Parse JSON ===
parsed = (
    raw.select(
        col("key").cast("string").alias("key"),
        col("value").cast("string").alias("json"),
        col("timestamp").alias("kafka_ts"),
    )
    .withColumn("data", from_json(col("json"), schema))
    .select(
        col("data.event_id").alias("event_id"),
        to_timestamp(col("data.ts")).alias("event_ts"),
        col("data.value").alias("value"),
        col("data.source").alias("source"),
        col("kafka_ts")
    )
)

# === Console sink (for debugging) ===
console_q = (
    parsed.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", False)
    .start()
)

# Add partition column for time-based folder structure
out_df = parsed.withColumn("dt", date_format(col("event_ts"), "yyyy-MM-dd"))

# === Parquet sink ===
parquet_q = (
    out_df.writeStream
    .format("parquet")
    .option("checkpointLocation", os.path.join(OUTPUT_PATH, "_chk"))
    .option("path", OUTPUT_PATH)
    .partitionBy("dt")
    .outputMode("append")
    .start()
)

# === CSV sink ===
csv_q = (
    out_df.writeStream
    .format("csv")
    .option("header", "true")
    .option("checkpointLocation", os.path.join(CSV_PATH, "_chk"))
    .option("path", CSV_PATH)
    .partitionBy("dt")
    .outputMode("append")
    .start()
)

# === Keep running ===
spark.streams.awaitAnyTermination()
