"""
OmniRoute — Bronze Streaming Layer
====================================
Ingests raw vehicle telemetry from Kafka, parses the JSON payload,
and writes structured Parquet to S3 for downstream Silver consumption.

    Kafka  →  Bronze (raw Parquet archive on S3)

This is the first stage of the decoupled streaming pipeline.
Silver reads from the bronze S3 output as a streaming source.

Usage:
    spark-submit \
      --master yarn --deploy-mode cluster \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
      --conf spark.streaming.stopGracefullyOnShutdown=true \
      bronze_stream.py \
        --kafka-bootstrap <broker_ip>:9092 \
        --bronze-path s3://.../.../ingested/telemetry_raw/
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, hour, to_date
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


# ──────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────
KAFKA_TOPIC = "omniroute.telemetry.raw"
TRIGGER_INTERVAL = "30 seconds"

TELEMETRY_SCHEMA = StructType([
    StructField("vin", StringType()),
    StructField("driver_id", StringType()),
    StructField("speed", IntegerType()),
    StructField("lat", FloatType()),
    StructField("long", FloatType()),
])


# ──────────────────────────────────────────────────────────────
# Bronze Stream Builder
# ──────────────────────────────────────────────────────────────

def build_bronze_stream(spark: SparkSession, kafka_bootstrap: str):
    """
    Bronze Layer — Kafka ingestion and JSON parsing.

    Reads raw JSON messages from the Kafka telemetry topic, parses
    them into a structured DataFrame, and adds partition columns
    (dt, hour) derived from the Kafka message timestamp.

    No filtering or business logic is applied at this stage.

    Returns:
        Streaming DataFrame with columns:
        vin, driver_id, speed, lat, long, event_timestamp, dt, hour
    """
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 10000)
        .load()
    )

    parsed = (
        raw_stream
        .select(
            from_json(col("value").cast("string"), TELEMETRY_SCHEMA).alias("data"),
            col("timestamp").alias("event_timestamp"),
        )
        .select("data.*", "event_timestamp")
        .withColumn("dt", to_date("event_timestamp"))
        .withColumn("hour", hour("event_timestamp"))
    )

    return parsed


# ──────────────────────────────────────────────────────────────
# Sink — Write Bronze to S3
# ──────────────────────────────────────────────────────────────

def start_bronze_query(spark: SparkSession, args: argparse.Namespace):
    """
    Starts the Bronze streaming query that writes parsed Kafka
    telemetry to S3 as Parquet, partitioned by dt and hour.
    """
    bronze_df = build_bronze_stream(spark, args.kafka_bootstrap)

    q = (
        bronze_df.writeStream
        .queryName("bronze_telemetry_raw")
        .format("parquet")
        .partitionBy("dt", "hour")
        .option("path", args.bronze_path)
        .option("checkpointLocation", f"{args.bronze_path}/../checkpoints/telemetry_raw/")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    print("=" * 60)
    print("  OmniRoute Bronze Stream — Query started")
    print(f"  Query : {q.name}")
    print(f"  Sink  : {args.bronze_path}")
    print(f"  Trigger: every {TRIGGER_INTERVAL}")
    print("=" * 60)

    spark.streams.awaitAnyTermination()


# ──────────────────────────────────────────────────────────────
# Entry Point
# ──────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="OmniRoute Bronze Streaming Layer"
    )
    parser.add_argument("--kafka-bootstrap", required=True,
                        help="Kafka bootstrap servers (e.g. 10.0.1.5:9092)")
    parser.add_argument("--bronze-path", required=True,
                        help="S3 path for Bronze telemetry_raw output")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("OmniRoute_Bronze_Stream")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()
    )

    print("=" * 60)
    print("  OmniRoute Bronze Streaming Layer")
    print(f"  Kafka  : {args.kafka_bootstrap}")
    print(f"  Bronze : {args.bronze_path}")
    print("=" * 60)

    try:
        start_bronze_query(spark, args)
    except Exception as e:
        print(f"✗ Bronze stream failed: {e}")
        raise
    finally:
        spark.stop()
