"""
OmniRoute — Streaming Telemetry Pipeline (LOCAL TESTING VERSION)
================================================================
Single Spark Structured Streaming application that processes real-time
vehicle telemetry from Kafka through all medallion layers.

Modified to run locally, writing Parquet data to the local file system.

Usage:
    python local_streaming_pipeline.py \
        --kafka-bootstrap <broker_ip>:9092
"""

import argparse
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    broadcast,
    coalesce,
    col,
    count,
    current_timestamp,
    date_format,
    expr,
    from_json,
    hour,
    lit,
    max as spark_max,
    to_date,
    when,
)
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ──────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────
KAFKA_TOPIC = "omniroute.telemetry.raw"
SPEED_THRESHOLD = 110          # km/h — above this is a violation
STRIKE_PENALTY_PCT = 0.05      # 5% deduction per strike
SUSPENSION_THRESHOLD = 10      # strikes to trigger SUSPENDED status
TRIGGER_INTERVAL = "10 seconds" # Sped up for local testing

TELEMETRY_SCHEMA = StructType([
    StructField("vin", StringType()),
    StructField("driver_id", StringType()),
    StructField("speed", IntegerType()),
    StructField("lat", FloatType()),
    StructField("long", FloatType()),
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
# Layer Builders
# ──────────────────────────────────────────────────────────────

def build_bronze_stream(spark: SparkSession, kafka_bootstrap: str) -> DataFrame:
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
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


def build_silver_stream(bronze_df: DataFrame, zones_df: DataFrame) -> DataFrame:
    validated = bronze_df.filter(
        col("vin").isNotNull()
        & col("lat").between(-90, 90)
        & col("long").between(-180, 180)
    )

    validated = validated.withColumn(
        "is_speeding", col("speed") > SPEED_THRESHOLD
    )

    validated = (
        validated
        .join(
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


def build_gold_violations_stream(
    silver_df: DataFrame, scd2_df: DataFrame
) -> DataFrame:
    violations = silver_df.filter(col("is_violation") == True)

    violations = violations.join(
        broadcast(scd2_df),
        on="vin",
        how="left",
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


def build_update_driver_safety_fn(spark: SparkSession, paths: dict):
    def update_driver_safety(batch_df: DataFrame, batch_id: int):
        if batch_df.isEmpty():
            return

        current_month = batch_df.select(
            date_format("event_timestamp", "yyyy-MM").alias("month")
        ).first()["month"]

        new_strikes = (
            batch_df
            .groupBy("driver_id")
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

        try:
             base_rates = (
                spark.read.parquet(paths["scd2"])
                .filter(col("_is_current") == True)
                .select("driver_id", col("daily_rate").alias("base_rate"))
            )
        except Exception:
            # Fallback if local SCD2 mock data doesn't exist yet
            print("⚠️ SCD2 reference data not found locally. Using default base_rate.")
            base_rates = spark.createDataFrame(
                [("default", 100.0)], 
                schema=StructType([StructField("driver_id", StringType()), StructField("base_rate", FloatType())])
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
            .withColumn("base_rate", coalesce(col("base_rate"), lit(100.0))) # Default rate if missing
            .withColumn(
                "current_adjusted_rate",
                col("base_rate") * (1 - STRIKE_PENALTY_PCT * col("strike_count")),
            )
            .withColumn(
                "status",
                when(col("strike_count") >= SUSPENSION_THRESHOLD, "SUSPENDED")
                .otherwise("ACTIVE"),
            )
            .withColumn("month", lit(current_month))
            .withColumn("last_updated", current_timestamp())
            .select(
                "driver_id", "base_rate", "strike_count",
                "current_adjusted_rate", "status", "month", "last_updated",
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
# Reference Data Loaders
# ──────────────────────────────────────────────────────────────

def load_restricted_zones(spark: SparkSession, path: str) -> DataFrame:
    try:
        return spark.read.parquet(path)
    except Exception:
        print(f"⚠️ Restricted zones data not found at {path}. Returning empty DataFrame.")
        schema = StructType([
            StructField("zone_name", StringType()), StructField("min_lat", FloatType()),
            StructField("max_lat", FloatType()), StructField("min_long", FloatType()),
            StructField("max_long", FloatType())
        ])
        return spark.createDataFrame([], schema=schema)


def load_scd2_current(spark: SparkSession, path: str) -> DataFrame:
    try:
        return (
            spark.read.parquet(path)
            .filter(col("_is_current") == True)
            .select("vin", col("driver_id").alias("scd2_driver_id"), "daily_rate")
        )
    except Exception:
        print(f"⚠️ SCD2 data not found at {path}. Returning empty DataFrame.")
        schema = StructType([
            StructField("vin", StringType()), StructField("scd2_driver_id", StringType()),
            StructField("daily_rate", FloatType())
        ])
        return spark.createDataFrame([], schema=schema)


# ──────────────────────────────────────────────────────────────
# Query Orchestrator
# ──────────────────────────────────────────────────────────────

def start_all_queries(spark: SparkSession, args: argparse.Namespace):
    zones_df = load_restricted_zones(spark, args.restricted_zones_path)
    scd2_df = load_scd2_current(spark, args.scd2_path)

    bronze_df = build_bronze_stream(spark, args.kafka_bootstrap)
    silver_df = build_silver_stream(bronze_df, zones_df)
    violations_df = build_gold_violations_stream(silver_df, scd2_df)

    q1 = (
        bronze_df.writeStream
        .queryName("bronze_telemetry_raw")
        .format("parquet")
        .partitionBy("dt", "hour")
        .option("path", args.bronze_path)
        .option("checkpointLocation", f"{args.checkpoint_dir}/bronze/")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    q2 = (
        silver_df
        .select(
            "vin", "driver_id", "speed", "lat", "long",
            "event_timestamp", "is_speeding", "is_in_restricted_zone",
            "is_violation", "zone_name",
        )
        .writeStream
        .queryName("silver_telemetry_validated")
        .format("parquet")
        .option("path", args.silver_path)
        .option("checkpointLocation", f"{args.checkpoint_dir}/silver/")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    q3 = (
        violations_df.writeStream
        .queryName("gold_safety_violations")
        .format("parquet")
        .option("path", args.gold_violations_path)
        .option("checkpointLocation", f"{args.checkpoint_dir}/gold_violations/")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    update_fn = build_update_driver_safety_fn(spark, {
        "gold_driver_safety": args.gold_driver_safety_path,
        "scd2": args.scd2_path,
    })

    q4 = (
        violations_df.writeStream
        .queryName("gold_driver_safety_status")
        .foreachBatch(update_fn)
        .option("checkpointLocation", f"{args.checkpoint_dir}/gold_driver_safety/")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    print("=" * 60)
    print("  OmniRoute Pipeline (LOCAL) — Queries Started")
    print("=" * 60)

    spark.streams.awaitAnyTermination()


# ──────────────────────────────────────────────────────────────
# Entry Point
# ──────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description="OmniRoute Streaming (Local Testing)")
    
    # Kafka source remains required
    parser.add_argument("--kafka-bootstrap", required=True,
                        help="Kafka bootstrap servers (e.g. localhost:9092)")
    
    # Defaults changed to local paths
    parser.add_argument("--bronze-path", default="./local_lake/bronze_raw/")
    parser.add_argument("--silver-path", default="./local_lake/silver_validated/")
    parser.add_argument("--gold-violations-path", default="./local_lake/gold_violations/")
    parser.add_argument("--gold-driver-safety-path", default="./local_lake/gold_driver_safety/")
    parser.add_argument("--scd2-path", default="./local_lake/reference/scd2/")
    parser.add_argument("--restricted-zones-path", default="./local_lake/reference/restricted_zones/")
    parser.add_argument("--checkpoint-dir", default="./local_lake/checkpoints/")
    
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # Added .master("local[*]") for local execution
    spark = (
        SparkSession.builder
        .appName("OmniRoute_Streaming_Local")
        .master("local[*]") 
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.schemaInference", "true")
        # Ensure you have the Kafka package locally
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .getOrCreate()
    )

    # Suppress verbose Spark logging for easier local console debugging
    spark.sparkContext.setLogLevel("WARN")

    try:
        start_all_queries(spark, args)
    except KeyboardInterrupt:
        print("\nPipeline stopped by user.")
    except Exception as e:
        print(f"✗ Pipeline failed: {e}")
        raise
    finally:
        spark.stop()