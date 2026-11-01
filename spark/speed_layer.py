"""
Speed Layer — Spark Structured Streaming
Reads raw click events from Kafka, detects fraud signals in near-real-time,
and writes alerts back to the `fraud-alerts` Kafka topic.

Fraud signals detected:
  1. IP click velocity     — >20 clicks/min from a single IP (bot hammering)
  2. User event velocity   — >30 events/min from a single user_id (scripted session)
  3. Headless fingerprint  — screen_width=0 AND screen_height=0 (headless browser)
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, TimestampType
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
KAFKA_BROKER       = os.getenv("KAFKA_BROKER", "kafka:9092")
INPUT_TOPIC        = "clickstream-raw"
OUTPUT_TOPIC       = "fraud-alerts"
CHECKPOINT_DIR     = "/tmp/spark-checkpoints/speed-layer"

# Tumbling window size for velocity checks
WINDOW_DURATION    = "1 minute"

# Thresholds
IP_CLICK_THRESHOLD   = 20   # clicks per window per IP
USER_EVENT_THRESHOLD = 30   # events per window per user_id

# ---------------------------------------------------------------------------
# Schema — mirrors EventPayload in ingestion-server/main.py
# ---------------------------------------------------------------------------
EVENT_SCHEMA = StructType([
    StructField("event_id",          StringType(),    True),
    StructField("event_type",        StringType(),    True),
    StructField("url",               StringType(),    True),
    StructField("referrer",          StringType(),    True),
    StructField("timestamp",         StringType(),    True),   # client ISO string
    StructField("screen_width",      IntegerType(),   True),
    StructField("screen_height",     IntegerType(),   True),
    StructField("element_tag",       StringType(),    True),
    StructField("element_id",        StringType(),    True),
    StructField("element_classes",   StringType(),    True),
    StructField("element_text",      StringType(),    True),
    StructField("target_href",       StringType(),    True),
    StructField("user_id",           StringType(),    True),
    StructField("server_received_at", StringType(),   True),   # server-stamped ISO string
    StructField("client_ip",         StringType(),    True),
])


def build_alert(fraud_type: str, key_col: str, count_col: str, threshold: int):
    """Return a column expression that serialises an alert to JSON for Kafka."""
    return F.to_json(F.struct(
        F.lit(fraud_type).alias("fraud_type"),
        F.col(key_col).alias("key"),
        F.col(count_col).alias("event_count"),
        F.lit(threshold).alias("threshold"),
        F.col("window.start").cast("string").alias("window_start"),
        F.col("window.end").cast("string").alias("window_end"),
    ))


def main():
    spark = (
        SparkSession.builder
        .appName("clickstream-speed-layer")
        # Pull Kafka connector jars at runtime — avoids baking them into the image
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ------------------------------------------------------------------
    # 1. Read raw stream from Kafka
    # ------------------------------------------------------------------
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Deserialise the Kafka value (bytes → JSON → typed columns)
    events = (
        raw_stream
        .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
        # Use server_received_at as the event time — client timestamps are untrusted
        .withColumn("event_time", F.to_timestamp("server_received_at"))
        .withWatermark("event_time", "30 seconds")
    )

    # ------------------------------------------------------------------
    # 2. Signal A — IP click velocity
    # ------------------------------------------------------------------
    ip_velocity = (
        events
        .filter(F.col("client_ip").isNotNull())
        .groupBy(
            F.window("event_time", WINDOW_DURATION),
            F.col("client_ip")
        )
        .agg(F.count("*").alias("click_count"))
        .filter(F.col("click_count") > IP_CLICK_THRESHOLD)
        .select(
            build_alert("ip_velocity", "client_ip", "click_count", IP_CLICK_THRESHOLD)
            .alias("value")
        )
    )

    # ------------------------------------------------------------------
    # 3. Signal B — User event velocity
    # ------------------------------------------------------------------
    user_velocity = (
        events
        .filter(F.col("user_id").isNotNull())
        .groupBy(
            F.window("event_time", WINDOW_DURATION),
            F.col("user_id")
        )
        .agg(F.count("*").alias("event_count"))
        .filter(F.col("event_count") > USER_EVENT_THRESHOLD)
        .select(
            build_alert("user_velocity", "user_id", "event_count", USER_EVENT_THRESHOLD)
            .alias("value")
        )
    )

    # ------------------------------------------------------------------
    # 4. Signal C — Headless browser fingerprint (one-shot per event, no window needed)
    # ------------------------------------------------------------------
    headless = (
        events
        .filter(
            (F.col("screen_width")  == 0) &
            (F.col("screen_height") == 0)
        )
        .select(
            F.to_json(F.struct(
                F.lit("headless_browser").alias("fraud_type"),
                F.col("client_ip").alias("key"),
                F.col("event_id").alias("event_id"),
                F.col("url").alias("url"),
                F.col("event_time").cast("string").alias("event_time"),
            )).alias("value")
        )
    )

    # ------------------------------------------------------------------
    # 5. Union all signals and sink to fraud-alerts Kafka topic
    # ------------------------------------------------------------------
    alerts = ip_velocity.union(user_velocity).union(headless)

    query = (
        alerts.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("topic", OUTPUT_TOPIC)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .outputMode("append")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
