"""
OmniRoute — Gold Streaming Layer
==================================
Reads Silver-layer Parquet from S3 as a streaming source, produces:
  1. safety_violations  — append-only violation event log
  2. driver_safety_status — rolling strike counts + penalized rates

    S3 (Silver Parquet)  ->  Gold (violations + driver safety -> S3)

Usage:
    spark-submit \
      --master yarn --deploy-mode cluster \
      --conf spark.streaming.stopGracefullyOnShutdown=true \
      gold_stream.py \
        --silver-path s3://.../.../silver.telemetry/ \
        --gold-violations-path s3://.../.../gold.safety_violations/ \
        --gold-driver-safety-path s3://.../.../gold.driver_safety_status/ \
        --scd2-path s3://.../.../gold.asset_history_scd2/
"""

import argparse
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    broadcast, coalesce, col, count, current_timestamp,
    date_format, expr, lit, max as spark_max, when,
)
from pyspark.sql.types import (
    FloatType, IntegerType, StringType,
    StructField, StructType, TimestampType, BooleanType,
)

STRIKE_PENALTY_PCT = 0.05
SUSPENSION_THRESHOLD = 10
TRIGGER_INTERVAL = "30 seconds"

SILVER_SCHEMA = StructType([
    StructField("vin", StringType()),
    StructField("driver_id", StringType()),
    StructField("speed", IntegerType()),
    StructField("lat", FloatType()),
    StructField("long", FloatType()),
    StructField("event_timestamp", TimestampType()),
    StructField("is_speeding", BooleanType()),
    StructField("is_in_restricted_zone", BooleanType()),
    StructField("is_violation", BooleanType()),
    StructField("zone_name", StringType()),
])

DRIVER_SAFETY_SCHEMA = StructType([
    StructField("driver_id", StringType()),
    StructField("base_rate", FloatType()),
    StructField("strike_count", IntegerType()),
    StructField("current_adjusted_rate", FloatType()),
    StructField("status", StringType()),
    StructField("month", StringType()),
    StructField("last_updated", TimestampType()),
])


# ──────────────────────────────────────────────────────────────
# Reference Data Loader
# ──────────────────────────────────────────────────────────────

def load_scd2_current(spark, path):
    """
    Load current vehicle-driver assignments from SCD2 table.
    Pre-filters to _is_current = TRUE and renames driver_id to
    avoid collision with the Silver-provided driver_id.
    """
    return (
        spark.read.parquet(path)
        .filter(col("_is_current") == True)
        .select(
            "vin",
            col("driver_id").alias("scd2_driver_id"),
            "daily_rate",
        )
    )


# ──────────────────────────────────────────────────────────────
# Silver S3 Reader
# ──────────────────────────────────────────────────────────────

def read_silver_from_s3(spark, silver_path):
    """Read Silver Parquet from S3 as a streaming source."""
    return (
        spark.readStream
        .format("parquet")
        .schema(SILVER_SCHEMA)
        .option("maxFilesPerTrigger", 100)
        .load(silver_path)
    )


# ──────────────────────────────────────────────────────────────
# Gold Violations Builder
# ──────────────────────────────────────────────────────────────

def build_gold_violations_stream(silver_df, scd2_df):
    """
    Gold Layer — Safety violation detection and classification.

    1. Filters to violation events only (is_violation = TRUE).
    2. Joins with SCD2 to resolve current driver_id for each vehicle.
    3. Classifies: SPEEDING, ZONE_BREACH, or BOTH.
    4. Generates a UUID per violation event.
    """
    violations = silver_df.filter(col("is_violation") == True)

    violations = violations.join(
        broadcast(scd2_df), on="vin", how="left",
    )

    violations = (
        violations
        .withColumn(
            "violation_type",
            when(col("is_speeding") & col("is_in_restricted_zone"), "BOTH")
            .when(col("is_speeding"), "SPEEDING")
            .otherwise("ZONE_BREACH"),
        )
        .withColumn("violation_id", expr("uuid()"))
        .withColumn(
            "driver_id",
            coalesce(col("scd2_driver_id"), col("driver_id")),
        )
    )

    violations = violations.select(
        "violation_id", "vin", "driver_id", "speed", "lat", "long",
        "event_timestamp", "violation_type", "zone_name",
    )
    return violations


# ──────────────────────────────────────────────────────────────
# Gold Driver Safety — foreachBatch
# ──────────────────────────────────────────────────────────────

def build_update_driver_safety_fn(spark, paths):
    """
    Returns a foreachBatch function that maintains the
    driver_safety_status table by incrementing strike counts
    and computing penalized rates.
    """

    def update_driver_safety(batch_df: DataFrame, batch_id: int):
        if batch_df.isEmpty():
            return

        current_month = batch_df.select(
            date_format("event_timestamp", "yyyy-MM").alias("month")
        ).first()["month"]

        new_strikes = (
            batch_df.groupBy("driver_id")
            .agg(
                count("*").alias("new_strike_count"),
                spark_max("event_timestamp").alias("last_violation_ts"),
            )
        )

        try:
            existing = (
                spark.read.parquet(paths["gold_driver_safety"])
                .filter(col("month") == current_month)
            )
        except Exception:
            existing = spark.createDataFrame([], schema=DRIVER_SAFETY_SCHEMA)

        base_rates = (
            spark.read.parquet(paths["scd2"])
            .filter(col("_is_current") == True)
            .select("driver_id", col("daily_rate").alias("base_rate"))
        )

        merged = (
            new_strikes
            .join(existing, on="driver_id", how="full_outer")
            .join(base_rates, on="driver_id", how="left")
            .withColumn(
                "strike_count",
                coalesce(col("strike_count"), lit(0))
                + coalesce(col("new_strike_count"), lit(0)),
            )
            .withColumn(
                "current_adjusted_rate",
                col("base_rate")
                * (1 - STRIKE_PENALTY_PCT * col("strike_count")),
            )
            .withColumn(
                "status",
                when(
                    col("strike_count") >= SUSPENSION_THRESHOLD,
                    "SUSPENDED",
                ).otherwise("ACTIVE"),
            )
            .withColumn("month", lit(current_month))
            .withColumn("last_updated", current_timestamp())
            .select(
                "driver_id", "base_rate", "strike_count",
                "current_adjusted_rate", "status", "month",
                "last_updated",
            )
        )

        (
            merged.write
            .mode("overwrite")
            .partitionBy("month")
            .parquet(paths["gold_driver_safety"])
        )

    return update_driver_safety


# ──────────────────────────────────────────────────────────────
# Query Orchestrator
# ──────────────────────────────────────────────────────────────

def start_gold_queries(spark, args):
    """
    Start two Gold streaming queries:
      q1: safety_violations (append-only log)
      q2: driver_safety_status (foreachBatch read-modify-write)
    """
    scd2_df = load_scd2_current(spark, args.scd2_path)
    silver_df = read_silver_from_s3(spark, args.silver_path)
    violations_df = build_gold_violations_stream(silver_df, scd2_df)

    q1 = (
        violations_df.writeStream
        .queryName("gold_safety_violations")
        .format("parquet")
        .option("path", args.gold_violations_path)
        .option(
            "checkpointLocation",
            f"{args.gold_violations_path}/../checkpoints/safety_violations/",
        )
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    update_fn = build_update_driver_safety_fn(spark, {
        "gold_driver_safety": args.gold_driver_safety_path,
        "scd2": args.scd2_path,
    })

    q2 = (
        violations_df.writeStream
        .queryName("gold_driver_safety_status")
        .foreachBatch(update_fn)
        .option(
            "checkpointLocation",
            f"{args.gold_driver_safety_path}/../checkpoints/driver_safety_status/",
        )
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    print("=" * 60)
    print("  OmniRoute Gold Stream — Queries started")
    print(f"  Queries: {[q.name for q in spark.streams.active]}")
    print(f"  Source : {args.silver_path}")
    print(f"  Trigger: every {TRIGGER_INTERVAL}")
    print("=" * 60)

    spark.streams.awaitAnyTermination()


# ──────────────────────────────────────────────────────────────
# Entry Point
# ──────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="OmniRoute Gold Stream")
    p.add_argument("--silver-path", required=True)
    p.add_argument("--gold-violations-path", required=True)
    p.add_argument("--gold-driver-safety-path", required=True)
    p.add_argument("--scd2-path", required=True)
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = (
        SparkSession.builder
        .appName("OmniRoute_Gold_Stream")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()
    )
    print("=" * 60)
    print("  OmniRoute Gold Streaming Layer")
    print(f"  Silver (source)  : {args.silver_path}")
    print(f"  Violations (sink): {args.gold_violations_path}")
    print(f"  Driver Safety    : {args.gold_driver_safety_path}")
    print(f"  SCD2             : {args.scd2_path}")
    print("=" * 60)
    try:
        start_gold_queries(spark, args)
    except Exception as e:
        print(f"x Gold stream failed: {e}")
        raise
    finally:
        spark.stop()
