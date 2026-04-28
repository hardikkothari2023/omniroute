"""
Silver Transform — Vehicle Assignment
========================================
Reads Bronze ingested/vehicle_assignment, applies cleansing and dedup,
writes to Silver vehicle_assignment_clean.

Logic:
  1. Convert start_timestamp / end_timestamp (Unix epoch) → start_date / end_date
  2. TRIM + UPPER on region
  3. Dedup: ROW_NUMBER() OVER (PARTITION BY vin, start_date ORDER BY daily_rate DESC) → keep rn = 1
  4. Drop rows with NULL vin or NULL driver_id

Usage:
    spark-submit spark_jobs/batch/daily_transform_vehicle_assignment.py --run-date 2026-04-23
"""

import os
import argparse
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_unixtime, to_date, trim, upper, row_number,
)
from pyspark.sql.window import Window


# ──────────────────────────────────────────────
# S3 paths — loaded from environment variables
# ──────────────────────────────────────────────
INGESTED_PATH = os.environ.get("INGESTED_PATH", "s3a://omniroute-bronze/ingested/") + "vehicle_assignment"
SILVER_PATH = os.environ.get("SILVER_VEHICLE_ASSIGNMENT", "s3a://omniroute-data-lake/silver/vehicle_assignment_clean/")


def run(spark: SparkSession, run_date: str):
    """
    Read Bronze vehicle_assignment, transform, and write to Silver.
    """
    print(f"[silver.vehicle_assignment_clean] Reading from: {INGESTED_PATH}")

    # ── Read Bronze data ──
    df = spark.read.parquet(INGESTED_PATH)

    # ── 1. Convert Unix epoch → DATE ──
    df = (df
          .withColumn("start_date", to_date(from_unixtime(col("start_timestamp"))))
          .withColumn("end_date", to_date(from_unixtime(col("end_timestamp"))))
          .drop("start_timestamp", "end_timestamp"))

    # ── 2. TRIM + UPPER on region ──
    df = df.withColumn("region", upper(trim(col("region"))))

    # ── 3. Drop NULLs ──
    df = df.filter(col("vin").isNotNull() & col("driver_id").isNotNull())

    # ── 4. Dedup: keep highest daily_rate per (vin, start_date) ──
    window = Window.partitionBy("vin", "start_date").orderBy(col("daily_rate").desc())
    df = (df
          .withColumn("rn", row_number().over(window))
          .filter(col("rn") == 1)
          .drop("rn"))

    # ── Select final columns ──
    df = df.select("vin", "driver_id", "start_date", "end_date", "daily_rate", "region")

    row_count = df.count()

    # ── Write to Silver ──
    df.write.mode("overwrite").parquet(SILVER_PATH)
    print(f"[silver.vehicle_assignment_clean] ✓ Wrote {row_count} rows → {SILVER_PATH}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", default=str(date.today()),
                        help="Execution date (YYYY-MM-DD)")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("OmniRoute_transform_vehicle_assignment")
        .getOrCreate()
    )

    try:
        run(spark, args.run_date)
    finally:
        spark.stop()
