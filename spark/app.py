import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# === Env knobs ===
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "events")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/opt/spark-output/parquet")
CSV_PATH = OUTPUT_PATH.replace("parquet", "csv")

# Limiters (tune these)
TRIGGER_SECONDS = int(os.getenv("TRIGGER_SECONDS", "30"))         # batch interval
MAX_RECORDS_PER_FILE = int(os.getenv("MAX_RECORDS_PER_FILE", "5000"))
FILES_PER_PARTITION = int(os.getenv("FILES_PER_PARTITION", "1"))  # 1 => ~one file per dt per batch

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("ts", StringType(), True),   # ISO8601 string
    StructField("value", DoubleType(), True),
    StructField("source", StringType(), True),
])

spark = (
    SparkSession.builder
    .appName("KafkaSparkExample")
    # keep shuffle low so we don't create many tasks/files
    .config("spark.sql.shuffle.partitions", str(max(1, FILES_PER_PARTITION)))
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("✅ Spark session started!")
print(f"Kafka: {KAFKA_BOOTSTRAP} | topic: {TOPIC}")
print(f"Parquet -> {OUTPUT_PATH} | CSV -> {CSV_PATH}")
print(f"Trigger: {TRIGGER_SECONDS}s | Max records/file: {MAX_RECORDS_PER_FILE} | Files/partition: {FILES_PER_PARTITION}")

# --- Read from Kafka ---
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# --- Parse JSON payload ---
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

# Partition column for day
with_dt = parsed.withColumn("dt", date_format(col("event_ts"), "yyyy-MM-dd"))

# *** Key trick: control number of output files per dt partition ***
# Repartition by dt with a fixed number of partitions (FILES_PER_PARTITION).
# With FILES_PER_PARTITION=1 you'll get ~1 file per dt per micro-batch per sink.
target_partitions = max(1, FILES_PER_PARTITION)
out_df = with_dt.repartition(target_partitions, col("dt"))

# --- Console sink (debug) ---
console_q = (
    with_dt.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", False)
    .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
    .start()
)

# --- Parquet sink ---
parquet_q = (
    out_df.writeStream
    .format("parquet")
    .option("checkpointLocation", os.path.join(OUTPUT_PATH, "_chk"))
    .option("path", OUTPUT_PATH)
    .option("maxRecordsPerFile", str(MAX_RECORDS_PER_FILE))
    .partitionBy("dt")
    .outputMode("append")
    .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
    .start()
)

# --- CSV sink ---
csv_q = (
    out_df.writeStream
    .format("csv")
    .option("header", "true")
    .option("checkpointLocation", os.path.join(CSV_PATH, "_chk"))
    .option("path", CSV_PATH)
    .option("maxRecordsPerFile", str(MAX_RECORDS_PER_FILE))
    .partitionBy("dt")
    .outputMode("append")
    .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
    .start()
)

spark.streams.awaitAnyTermination()
