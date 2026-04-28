"""
Silver Transform — Fuel Transactions
=======================================
Reads Bronze ingested/fuel_transactions, applies enrichment and cleansing,
writes to Silver fuel_transactions_enriched.

Logic:
  1. CAST timestamp string → TIMESTAMP, derive txn_date
  2. Compute day_of_week, is_weekend
  3. LEFT JOIN with silver.maintenance_schedules → flag is_maintenance_day
  4. LAG(odometer_reading) OVER (PARTITION BY vin ORDER BY timestamp) → prev_odometer
  5. Compute distance_km, km_per_liter

Usage:
    spark-submit spark_jobs/batch/daily_transform_fuel_transactions.py --run-date 2026-04-23
"""

import os
import argparse
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, to_date, dayofweek, when, lag, lit,
)
from pyspark.sql.window import Window


# ──────────────────────────────────────────────
# S3 paths — loaded from environment variables
# ──────────────────────────────────────────────
INGESTED_PATH = os.environ.get("INGESTED_PATH", "s3a://omniroute-bronze/ingested/") + "fuel_transactions"
SILVER_MAINTENANCE_PATH = os.environ.get("SILVER_MAINTENANCE_SCHEDULES", "s3a://omniroute-data-lake/silver/maintenance_schedules/")
SILVER_PATH = os.environ.get("SILVER_FUEL_TRANSACTIONS", "s3a://omniroute-data-lake/silver/fuel_transactions_enriched/")


def run(spark: SparkSession, run_date: str):
    """
    Read Bronze fuel_transactions, enrich, and write to Silver.
    """
    print(f"[silver.fuel_transactions_enriched] Reading from: {INGESTED_PATH}")

    # ── Read Bronze data ──
    df = spark.read.parquet(INGESTED_PATH)

    # ── 1. Cast timestamp string → TIMESTAMP, derive txn_date ──
    df = (df
          .withColumn("timestamp", to_timestamp(col("timestamp")))
          .withColumn("txn_date", to_date(col("timestamp"))))

    # ── 2. Compute day_of_week and is_weekend ──
    # Spark dayofweek: 1=Sun, 7=Sat
    df = (df
          .withColumn("day_of_week", dayofweek(col("txn_date")))
          .withColumn("is_weekend", col("day_of_week").isin(1, 7)))

    # ── 3. LEFT JOIN with maintenance_schedules → is_maintenance_day ──
    try:
        maint_df = spark.read.parquet(SILVER_MAINTENANCE_PATH)
        maint_df = maint_df.select(
            col("vin").alias("m_vin"),
            col("service_date").alias("m_service_date"),
        )
        df = df.join(
            maint_df,
            (df.vin == maint_df.m_vin) & (df.txn_date == maint_df.m_service_date),
            "left",
        )
        df = (df
              .withColumn("is_maintenance_day", col("m_vin").isNotNull())
              .drop("m_vin", "m_service_date"))
    except Exception:
        # maintenance_schedules may not exist yet (yearly ingest)
        print("[silver.fuel_transactions_enriched] ⚠ maintenance_schedules not found — defaulting is_maintenance_day=False")
        df = df.withColumn("is_maintenance_day", lit(False))

    # ── 4. LAG window → prev_odometer, distance_km ──
    window = Window.partitionBy("vin").orderBy("timestamp")
    df = (df
          .withColumn("prev_odometer", lag("odometer_reading").over(window))
          .withColumn("distance_km", col("odometer_reading") - col("prev_odometer")))

    # ── 5. Compute km_per_liter ──
    df = df.withColumn(
        "km_per_liter",
        when((col("fuel_liters") > 0) & col("distance_km").isNotNull(),
             col("distance_km") / col("fuel_liters"))
    )

    # ── Select final columns ──
    df = df.select(
        "transaction_id", "vin", "fuel_liters", "odometer_reading",
        "timestamp", "txn_date", "day_of_week", "is_weekend",
        "is_maintenance_day", "prev_odometer", "distance_km", "km_per_liter",
    )

    row_count = df.count()

    # ── Write to Silver ──
    df.write.mode("overwrite").parquet(SILVER_PATH)
    print(f"[silver.fuel_transactions_enriched] ✓ Wrote {row_count} rows → {SILVER_PATH}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", default=str(date.today()),
                        help="Execution date (YYYY-MM-DD)")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("OmniRoute_transform_fuel_transactions")
        .getOrCreate()
    )

    try:
        run(spark, args.run_date)
    finally:
        spark.stop()
