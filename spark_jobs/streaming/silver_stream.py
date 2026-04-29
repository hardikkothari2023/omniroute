"""
OmniRoute — Silver Streaming Layer
====================================
Reads Bronze-layer Parquet from S3 as a streaming source, applies
validation and violation flagging, and writes enriched output to S3.

    S3 (Bronze Parquet)  ->  Silver (validated + flagged -> S3)

Usage:
    spark-submit \
      --master yarn --deploy-mode cluster \
      --conf spark.streaming.stopGracefullyOnShutdown=true \
      silver_stream.py \
        --bronze-path s3://.../.../ingested/telemetry_raw/ \
        --silver-path s3://.../.../silver.telemetry/ \
        --restricted-zones-path s3://.../.../ingested/restricted_zones/
"""

import argparse
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import broadcast, col
from pyspark.sql.types import (
    FloatType, IntegerType, StringType,
    StructField, StructType, TimestampType,
)

SPEED_THRESHOLD = 110
TRIGGER_INTERVAL = "30 seconds"

BRONZE_SCHEMA = StructType([
    StructField("vin", StringType()),
    StructField("driver_id", StringType()),
    StructField("speed", IntegerType()),
    StructField("lat", FloatType()),
    StructField("long", FloatType()),
    StructField("event_timestamp", TimestampType()),
    StructField("dt", StringType()),
    StructField("hour", IntegerType()),
])


def load_restricted_zones(spark, path):
    """Load the restricted zones reference table (small, broadcast-ready)."""
    return spark.read.parquet(path)


def read_bronze_from_s3(spark, bronze_path):
    """Read Bronze Parquet from S3 as a streaming source."""
    return (
        spark.readStream
        .format("parquet")
        .schema(BRONZE_SCHEMA)
        .option("maxFilesPerTrigger", 100)
        .load(bronze_path)
    )


def build_silver_stream(bronze_df, zones_df):
    """
    Silver Layer — Validation and violation flagging.

    1. Drops records with NULL vin or out-of-range coordinates.
    2. Flags speeding events (speed > 110 km/h).
    3. Broadcast-joins with restricted zones for geofence detection.
    4. Derives a combined is_violation flag.
    """
    validated = bronze_df.filter(
        col("vin").isNotNull()
        & col("lat").between(-90, 90)
        & col("long").between(-180, 180)
    )
    validated = validated.withColumn(
        "is_speeding", col("speed") > SPEED_THRESHOLD
    )
    validated = (
        validated.join(
            broadcast(zones_df),
            (col("lat").between(col("min_lat"), col("max_lat")))
            & (col("long").between(col("min_long"), col("max_long"))),
            "left",
        )
        .withColumn("is_in_restricted_zone", col("zone_name").isNotNull())
    )
    validated = validated.withColumn(
        "is_violation", col("is_speeding") | col("is_in_restricted_zone")
    )
    return validated


def start_silver_query(spark, args):
    """Start the Silver streaming query."""
    zones_df = load_restricted_zones(spark, args.restricted_zones_path)
    bronze_df = read_bronze_from_s3(spark, args.bronze_path)
    silver_df = build_silver_stream(bronze_df, zones_df)

    output_df = silver_df.select(
        "vin", "driver_id", "speed", "lat", "long",
        "event_timestamp", "is_speeding", "is_in_restricted_zone",
        "is_violation", "zone_name",
    )

    q = (
        output_df.writeStream
        .queryName("silver_telemetry_validated")
        .format("parquet")
        .option("path", args.silver_path)
        .option("checkpointLocation",
                f"{args.silver_path}/../checkpoints/telemetry_validated/")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    print("=" * 60)
    print("  OmniRoute Silver Stream — Query started")
    print(f"  Query  : {q.name}")
    print(f"  Source : {args.bronze_path}")
    print(f"  Sink   : {args.silver_path}")
    print(f"  Trigger: every {TRIGGER_INTERVAL}")
    print("=" * 60)

    spark.streams.awaitAnyTermination()


def parse_args():
    p = argparse.ArgumentParser(description="OmniRoute Silver Stream")
    p.add_argument("--bronze-path", required=True)
    p.add_argument("--silver-path", required=True)
    p.add_argument("--restricted-zones-path", required=True)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = (
        SparkSession.builder
        .appName("OmniRoute_Silver_Stream")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()
    )
    print("=" * 60)
    print("  OmniRoute Silver Streaming Layer")
    print(f"  Bronze (source): {args.bronze_path}")
    print(f"  Silver (sink)  : {args.silver_path}")
    print(f"  Zones          : {args.restricted_zones_path}")
    print("=" * 60)
    try:
        start_silver_query(spark, args)
    except Exception as e:
        print(f"x Silver stream failed: {e}")
        raise
    finally:
        spark.stop()
