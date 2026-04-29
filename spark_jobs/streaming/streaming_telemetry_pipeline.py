"""
OmniRoute — Streaming Telemetry Pipeline
==========================================
Single Spark Structured Streaming application that processes real-time
vehicle telemetry from Kafka through all medallion layers:

    Kafka  →  Bronze (raw archive)
           →  Silver (validated + flagged)
           →  Gold   (safety_violations + driver_safety_status)

Designed to run as a long-running EMR step via spark-submit.
Data flows in-memory between layers; each layer also persists to S3
for auditability and downstream consumption.

Usage:
    spark-submit \
      --master yarn --deploy-mode cluster \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
      --conf spark.streaming.stopGracefullyOnShutdown=true \
      streaming_telemetry_pipeline.py \
        --kafka-bootstrap <broker_ip>:9092 \
        --bronze-path s3://.../.../ingested/telemetry_raw/ \
        --silver-path s3://.../.../silver.telemetry/ \
        --gold-violations-path s3://.../.../gold.safety_violations/ \
        --gold-driver-safety-path s3://.../.../gold.driver_safety_status/ \
        --scd2-path s3://.../.../gold.asset_history_scd2/ \
        --restricted-zones-path s3://.../.../ingested/restricted_zones/
"""

import argparse
import uuid

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
TRIGGER_INTERVAL = "30 seconds"

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


def build_silver_stream(bronze_df: DataFrame, zones_df: DataFrame) -> DataFrame:
    """
    Silver Layer — Validation and violation flagging.

    1. Drops records with NULL vin or out-of-range coordinates.
    2. Flags speeding events (speed > 110 km/h).
    3. Broadcast-joins with restricted zones for geofence detection.
    4. Derives a combined is_violation flag.

    Args:
        bronze_df: Streaming DataFrame from build_bronze_stream()
        zones_df:  Static DataFrame of restricted zones (broadcast)

    Returns:
        Streaming DataFrame with added columns:
        is_speeding, is_in_restricted_zone, is_violation, zone_name
    """
    # Step 1: Filter invalid records
    validated = bronze_df.filter(
        col("vin").isNotNull()
        & col("lat").between(-90, 90)
        & col("long").between(-180, 180)
    )

    # Step 2: Flag speeding
    validated = validated.withColumn(
        "is_speeding", col("speed") > SPEED_THRESHOLD
    )

    # Step 3: Geofence check via broadcast join with restricted zones
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

    # Step 4: Combined violation flag
    validated = validated.withColumn(
        "is_violation", col("is_speeding") | col("is_in_restricted_zone")
    )

    return validated


def build_gold_violations_stream(
    silver_df: DataFrame, scd2_df: DataFrame
) -> DataFrame:
    """
    Gold Layer — Safety violation detection and classification.

    1. Filters to violation events only (is_violation = TRUE).
    2. Joins with the SCD2 asset history table to resolve the current
       driver_id for each vehicle.
    3. Classifies violation type: SPEEDING, ZONE_BREACH, or BOTH.
    4. Generates a UUID per violation event.

    A single event triggering both speed + zone = ONE strike (type=BOTH).

    Args:
        silver_df: Streaming DataFrame from build_silver_stream()
        scd2_df:   Static DataFrame of gold.asset_history_scd2
                   (pre-filtered to _is_current = TRUE)

    Returns:
        Streaming DataFrame with columns:
        violation_id, vin, driver_id, speed, lat, long,
        event_timestamp, violation_type, zone_name
    """
    # Step 1: Filter to violations only
    violations = silver_df.filter(col("is_violation") == True)

    # Step 2: Join with SCD2 to resolve current driver
    #   scd2_df should already be filtered to _is_current = TRUE
    #   and broadcast (it's a small lookup table)
    violations = violations.join(
        broadcast(scd2_df),
        on="vin",
        how="left",
    )

    # Step 3: Classify violation type
    violations = (
        violations
        .withColumn(
            "violation_type",
            when(col("is_speeding") & col("is_in_restricted_zone"), "BOTH")
            .when(col("is_speeding"), "SPEEDING")
            .otherwise("ZONE_BREACH"),
        )
        .withColumn("violation_id", expr("uuid()"))
        # Prefer SCD2-resolved driver; fall back to Kafka-provided driver_id
        .withColumn(
            "driver_id",
            coalesce(col("scd2_driver_id"), col("driver_id")),
        )
    )

    # Step 4: Select final schema
    violations = violations.select(
        "violation_id", "vin", "driver_id", "speed", "lat", "long",
        "event_timestamp", "violation_type", "zone_name",
    )

    return violations


def build_update_driver_safety_fn(spark: SparkSession, paths: dict):
    """
    Returns a foreachBatch function that maintains the driver_safety_status
    table by incrementing strike counts and computing penalized rates.

    The state is stored in an S3-backed Parquet table, partitioned by month.
    ── SWAP POINT: To use DynamoDB or Redis instead, replace the
       read/write calls inside the returned function. ──

    Args:
        spark: Active SparkSession
        paths: Dict with 'gold_driver_safety' and 'scd2' S3 paths
    """

    def update_driver_safety(batch_df: DataFrame, batch_id: int):
        """
        Called once per micro-batch by the foreachBatch sink.
        Aggregates new violations, merges with the existing
        driver_safety_status table, and writes back.
        """
        if batch_df.isEmpty():
            return

        # Determine current month from the batch
        current_month = batch_df.select(
            date_format("event_timestamp", "yyyy-MM").alias("month")
        ).first()["month"]

        # ── Aggregate new strikes per driver in this micro-batch ──
        new_strikes = (
            batch_df
            .groupBy("driver_id")
            .agg(
                count("*").alias("new_strike_count"),
                spark_max("event_timestamp").alias("last_violation_ts"),
            )
        )

        # ── Read existing driver safety status (S3-backed state) ──
        # ── SWAP POINT: Replace with DynamoDB/Redis read ──
        try:
            existing = (
                spark.read.parquet(paths["gold_driver_safety"])
                .filter(col("month") == current_month)
            )
        except Exception:
            # First run — no existing data yet
            existing = spark.createDataFrame([], schema=DRIVER_SAFETY_SCHEMA)

        # ── Read base rates from SCD2 (current assignments) ──
        base_rates = (
            spark.read.parquet(paths["scd2"])
            .filter(col("_is_current") == True)
            .select("driver_id", col("daily_rate").alias("base_rate"))
        )

        # ── Merge: new strikes + existing status + base rates ──
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

        # ── Write back (overwrite current month partition) ──
        # ── SWAP POINT: Replace with DynamoDB/Redis put_item calls ──
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
    """Load the restricted zones reference table (small, broadcast-ready)."""
    return spark.read.parquet(path)


def load_scd2_current(spark: SparkSession, path: str) -> DataFrame:
    """
    Load current vehicle-driver assignments from the SCD2 table.
    Pre-filters to _is_current = TRUE and renames driver_id to avoid
    collision with the Kafka-provided driver_id.
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
# Query Orchestrator
# ──────────────────────────────────────────────────────────────

def start_all_queries(spark: SparkSession, args: argparse.Namespace):
    """
    Wire all layers together and start 4 concurrent streaming queries:
      q1: Bronze sink   (Kafka → S3 Parquet archive)
      q2: Silver sink   (validated + flagged → S3)
      q3: Gold sink     (safety violations → S3)
      q4: Gold sink     (driver safety status → S3 via foreachBatch)

    All queries share the same in-memory DataFrame lineage —
    no S3 read-back between layers.
    """
    # ── Load reference data (static, broadcast) ──
    zones_df = load_restricted_zones(spark, args.restricted_zones_path)
    scd2_df = load_scd2_current(spark, args.scd2_path)

    # ── Build layer DataFrames (in-memory pipeline) ──
    bronze_df = build_bronze_stream(spark, args.kafka_bootstrap)
    silver_df = build_silver_stream(bronze_df, zones_df)
    violations_df = build_gold_violations_stream(silver_df, scd2_df)

    # ── Q1: Bronze sink — raw archive to S3 ──
    q1 = (
        bronze_df.writeStream
        .queryName("bronze_telemetry_raw")
        .format("parquet")
        .partitionBy("dt", "hour")
        .option("path", args.bronze_path)
        .option("checkpointLocation", f"{args.bronze_path}/../checkpoints/telemetry_raw/")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    # ── Q2: Silver sink — validated telemetry to S3 ──
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
        .option("checkpointLocation", f"{args.silver_path}/../checkpoints/telemetry_validated/")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    # ── Q3: Gold violations sink — append-only log ──
    q3 = (
        violations_df.writeStream
        .queryName("gold_safety_violations")
        .format("parquet")
        .option("path", args.gold_violations_path)
        .option("checkpointLocation", f"{args.gold_violations_path}/../checkpoints/safety_violations/")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    # ── Q4: Gold driver safety status — foreachBatch (read-modify-write) ──
    update_fn = build_update_driver_safety_fn(spark, {
        "gold_driver_safety": args.gold_driver_safety_path,
        "scd2": args.scd2_path,
    })

    q4 = (
        violations_df.writeStream
        .queryName("gold_driver_safety_status")
        .foreachBatch(update_fn)
        .option("checkpointLocation", f"{args.gold_driver_safety_path}/../checkpoints/driver_safety_status/")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    print("=" * 60)
    print("  OmniRoute Streaming Pipeline — All queries started")
    print(f"  Queries: {[q.name for q in spark.streams.active]}")
    print(f"  Trigger: every {TRIGGER_INTERVAL}")
    print("=" * 60)

    # Block until any query terminates (crash or graceful shutdown)
    spark.streams.awaitAnyTermination()


# ──────────────────────────────────────────────────────────────
# Entry Point
# ──────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="OmniRoute Streaming Telemetry Pipeline"
    )
    parser.add_argument("--kafka-bootstrap", required=True,
                        help="Kafka bootstrap servers (e.g. 10.0.1.5:9092)")
    parser.add_argument("--bronze-path", required=True,
                        help="S3 path for Bronze telemetry_raw output")
    parser.add_argument("--silver-path", required=True,
                        help="S3 path for Silver telemetry_validated output")
    parser.add_argument("--gold-violations-path", required=True,
                        help="S3 path for Gold safety_violations output")
    parser.add_argument("--gold-driver-safety-path", required=True,
                        help="S3 path for Gold driver_safety_status output")
    parser.add_argument("--scd2-path", required=True,
                        help="S3 path to gold.asset_history_scd2 (read-only)")
    parser.add_argument("--restricted-zones-path", required=True,
                        help="S3 path to ingested/restricted_zones (read-only)")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("OmniRoute_Streaming_Telemetry")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()
    )

    print("=" * 60)
    print("  OmniRoute Streaming Telemetry Pipeline")
    print(f"  Kafka        : {args.kafka_bootstrap}")
    print(f"  Bronze       : {args.bronze_path}")
    print(f"  Silver       : {args.silver_path}")
    print(f"  Violations   : {args.gold_violations_path}")
    print(f"  Driver Safety: {args.gold_driver_safety_path}")
    print(f"  SCD2         : {args.scd2_path}")
    print(f"  Zones        : {args.restricted_zones_path}")
    print("=" * 60)

    try:
        start_all_queries(spark, args)
    except Exception as e:
        print(f"✗ Streaming pipeline failed: {e}")
        raise
    finally:
        spark.stop()
