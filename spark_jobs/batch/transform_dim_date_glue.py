"""
AWS Glue Job — Silver Transformation: Date Dimension (dim_date)
================================================================
Generates a full year of dates as a Delta table in the Silver layer.

ER Table: dim_date
  PK: date_id (YYYYMMDD format, INT)

Columns:
  date_id       INT       — YYYYMMDD (e.g. 20260101)
  full_date     DATE      — The actual date (unique)
  day           INT       — Day of month (1-31)
  month         INT       — Month (1-12)
  quarter       INT       — Quarter (1-4)
  year          INT       — Year (e.g. 2026)
  day_of_week   STRING    — Monday, Tuesday, etc.
  is_weekend    BOOLEAN   — TRUE if Saturday/Sunday

Logic:
  - Accepts --year parameter (e.g. 2026)
  - Generates all 365/366 dates for that year
  - Uses Delta MERGE to avoid duplicates if re-run

Trigger:
  - Run on Jan 1st each year (or manually for historical years)
  - Scheduled via Airflow or run once manually

Glue Job Parameters:
  --year                : Year to generate dates for (e.g. 2026)
  --silver_output_path  : S3 path for Silver dim_date Delta table
"""

import sys
from datetime import date, timedelta
import calendar

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col, lit, dayofweek, date_format, quarter, month, dayofmonth, year,
    when,
)
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DateType, StringType, BooleanType,
)
from delta.tables import DeltaTable


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────
def silver_table_exists(spark, path):
    """Check whether the Silver Delta table already exists."""
    try:
        DeltaTable.forPath(spark, path)
        return True
    except Exception:
        return False


def generate_dates_for_year(target_year):
    """
    Generate a list of all dates for the given year.
    Returns a list of tuples: (date_id, full_date, day, month, quarter, year, day_name, is_weekend)
    """
    start = date(target_year, 1, 1)
    end = date(target_year, 12, 31)

    dates = []
    current = start
    while current <= end:
        date_id = int(current.strftime("%Y%m%d"))
        day_name = calendar.day_name[current.weekday()].upper()  # MONDAY, TUESDAY, etc.
        is_wknd = current.weekday() >= 5  # Saturday=5, Sunday=6
        q = (current.month - 1) // 3 + 1

        dates.append((
            date_id,
            current,
            current.day,
            current.month,
            q,
            current.year,
            day_name,
            is_wknd,
        ))
        current += timedelta(days=1)

    return dates


# ──────────────────────────────────────────────────────────────
# Schema
# ──────────────────────────────────────────────────────────────
DIM_DATE_SCHEMA = StructType([
    StructField("date_id", IntegerType(), False),
    StructField("full_date", DateType(), False),
    StructField("day", IntegerType(), False),
    StructField("month", IntegerType(), False),
    StructField("quarter", IntegerType(), False),
    StructField("year", IntegerType(), False),
    StructField("day_of_week", StringType(), False),
    StructField("is_weekend", BooleanType(), False),
])


# ──────────────────────────────────────────────────────────────
# Core Logic
# ──────────────────────────────────────────────────────────────
def run(spark, target_year, silver_path):
    """
    Generate dim_date for the given year and MERGE into Silver Delta table.
    """
    print(f"[dim_date] Generating dates for year: {target_year}")

    # ── Generate all dates as Python objects ──
    date_rows = generate_dates_for_year(target_year)
    print(f"[dim_date] Generated {len(date_rows)} date records")

    # ── Create Spark DataFrame ──
    df = spark.createDataFrame(date_rows, schema=DIM_DATE_SCHEMA)

    if silver_table_exists(spark, silver_path):
        # ── MERGE: Insert only new dates (idempotent for re-runs) ──
        print("[dim_date] Silver exists → Delta MERGE (insert-only)")
        silver_table = DeltaTable.forPath(spark, silver_path)
        (
            silver_table.alias("existing")
            .merge(df.alias("incoming"), "existing.date_id = incoming.date_id")
            .whenNotMatchedInsertAll()
            .execute()
        )
        final_count = spark.read.format("delta").load(silver_path).count()
        print(f"[dim_date] ✓ MERGE complete. Total Silver rows: {final_count}")

        # ── VACUUM: Clean up old Parquet files ──
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        DeltaTable.forPath(spark, silver_path).vacuum(0)
        print("[dim_date] ✓ VACUUM complete — old Parquet files deleted.")

    else:
        # ── FIRST RUN (Bootstrap) ──
        print("[dim_date] Silver does NOT exist → Bootstrap write")
        (
            df.write.format("delta")
            .mode("overwrite").option("overwriteSchema", "true")
            .save(silver_path)
        )
        print(f"[dim_date] ✓ Bootstrap: {len(date_rows)} rows → {silver_path}")


# ──────────────────────────────────────────────────────────────
# Glue Job Entry Point
# ──────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "year",
    "silver_output_path",
])

# ── Delta Lake requires extensions set BEFORE SparkSession creation ──
conf = SparkConf()
conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

sc = SparkContext(conf=conf)
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

target_year = int(args["year"])
silver_path = args["silver_output_path"].rstrip("/")

print("=" * 60)
print(f"  dim_date Silver Transformation — Glue Job")
print(f"  Year        : {target_year}")
print(f"  Silver Path : {silver_path}")
print("=" * 60)

try:
    run(spark, target_year, silver_path)
    print("✓ dim_date generation completed successfully.")
except Exception as e:
    print(f"✗ dim_date generation failed: {e}")
    raise
finally:
    job.commit()
