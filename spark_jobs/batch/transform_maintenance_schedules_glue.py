"""
AWS Glue Job — Silver Transformation: Maintenance Schedules (dim_maintenance)
===============================================================================
SCD Type 1 via Delta Lake MERGE (no soft-delete per ER diagram).

ER Table: dim_maintenance
Reads ONLY today's partition from Bronze (partition pruning on load_date),
cleans it, validates service_type against known list, and MERGEs into
the Silver Delta table.

Null Handling (Silver enforcement):
  - vin NULL/empty      → DROP ROW (FK)
  - service_date NULL   → DROP ROW (can't match to fuel dates)
  - service_type NULL   → Default 'UNKNOWN'

Service Type Fraud Validation:
  - service_type is validated against VALID_SERVICE_TYPES whitelist.
  - If not in list → flagged as 'UNVERIFIED' (row kept, but marked).

Bronze data is NOT moved — stays in ingested/ for auditability.

MERGE Logic:
  Match key: (vin, service_date)
  WHEN MATCHED AND service_type changed → UPDATE
  WHEN NOT MATCHED                      → INSERT

Glue Job Parameters:
  --run_date              : Partition date (YYYY-MM-DD)
  --bronze_ingested_path  : Base S3 path for Bronze ingested data
  --silver_output_path    : S3 path for Silver Delta table output
"""

import sys
from datetime import date

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql.functions import (
    col, trim, upper, to_date, row_number, current_timestamp, lit, when,
    sha2, concat_ws, date_format,
)
from delta.tables import DeltaTable


# ──────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────
TABLE_NAME = "maintenance_schedules"

# Whitelist of valid service types for fraud validation.
# Any service_type NOT in this list is flagged as 'UNVERIFIED'.
VALID_SERVICE_TYPES = {
    "OIL_CHANGE", "TIRE_ROTATION", "BRAKE_INSPECTION", "BRAKE_REPLACEMENT",
    "ENGINE_TUNE_UP", "TRANSMISSION_SERVICE", "BATTERY_REPLACEMENT",
    "COOLANT_FLUSH", "AIR_FILTER", "FUEL_FILTER", "WHEEL_ALIGNMENT",
    "SUSPENSION_CHECK", "EXHAUST_REPAIR", "AC_SERVICE", "GENERAL_INSPECTION",
    "UNKNOWN",
}


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────
def normalize_path(path):
    """Ensure S3 path ends with a trailing slash."""
    return path if path.endswith("/") else path + "/"


def silver_table_exists(spark, path):
    """Check whether the Silver Delta table already exists."""
    try:
        DeltaTable.forPath(spark, path)
        return True
    except Exception:
        return False


# ──────────────────────────────────────────────────────────────
# Data Cleaning
# ──────────────────────────────────────────────────────────────
def clean_snapshot(bronze_df):
    """
    Clean incoming Bronze maintenance schedules data.
    Enforces null handling, dedup, service type validation, and ER schema.
    """
    # ── Parse service_date string → Date type ──
    df = bronze_df.withColumn(
        "service_date", to_date(col("service_date"), "yyyy-MM-dd")
    )

    # ── Drop NULL/unparseable service_date ──
    before_date = bronze_df.count()
    df = df.filter(col("service_date").isNotNull())
    dropped_date = before_date - df.count()
    if dropped_date > 0:
        print(f"[dim_maintenance] Dropped {dropped_date} rows with NULL/invalid service_date")

    # ── Drop NULL/empty/INVALID VINs ──
    before_vin = df.count()
    df = df.filter(
        col("vin").isNotNull()
        & (col("vin") != "")
    )
    # ── Normalize VIN → UPPERCASE ──
    df = df.withColumn("vin", trim(upper(col("vin"))))
    # ── Filter out INVALID_ prefixed VINs (already uppercased) ──
    df = df.filter(~col("vin").startswith("INVALID_"))
    dropped_vin = before_vin - df.count()
    if dropped_vin > 0:
        print(f"[dim_maintenance] Dropped {dropped_vin} rows with NULL/INVALID VIN")

    # ── Dedup by (vin, service_date) ──
    dedup_window = Window.partitionBy("vin", "service_date").orderBy(col("service_type"))
    df = (
        df.withColumn("_rn", row_number().over(dedup_window))
        .filter(col("_rn") == 1).drop("_rn")
    )

    # ── Default NULL service_type → 'UNKNOWN', normalize ──
    df = df.withColumn("service_type", trim(upper(col("service_type"))))
    df = df.withColumn(
        "service_type",
        when(col("service_type").isNull() | (col("service_type") == ""), lit("UNKNOWN"))
        .otherwise(col("service_type"))
    )

    # ── Service type fraud validation ──
    # If service_type is not in the known whitelist, flag as 'UNVERIFIED'.
    # Row is KEPT but marked, allowing downstream analysis to filter or audit.
    before_validation = df.count()
    unverified_count = df.filter(~col("service_type").isin(list(VALID_SERVICE_TYPES))).count()
    if unverified_count > 0:
        print(f"[dim_maintenance] ⚠ {unverified_count} rows have unrecognized service_type → flagged as UNVERIFIED")
    df = df.withColumn(
        "service_type",
        when(col("service_type").isin(list(VALID_SERVICE_TYPES)), col("service_type"))
        .otherwise(lit("UNVERIFIED"))
    )

    # ── Build final Silver schema (no audit_run_id) ──
    df = df.select(
        sha2(concat_ws("|", col("vin"), col("service_date").cast("string")), 256)
            .alias("maintenance_sk"),
        sha2(col("vin"), 256).alias("vehicle_sk"),
        date_format(col("service_date"), "yyyyMMdd").cast("int").alias("date_id"),
        col("vin"),
        col("service_date"),
        col("service_type"),
        lit(None).cast("string").alias("description"),
        current_timestamp().alias("created_at"),
    )

    return df


# ──────────────────────────────────────────────────────────────
# Core Transformation Logic
# ──────────────────────────────────────────────────────────────
def run(spark, run_date, bronze_base, silver_path):
    """
    Silver transformation for maintenance_schedules → dim_maintenance.
    Reads only today's Bronze partition. Bronze data stays in place.
    """
    bronze_partition_path = f"{bronze_base}{TABLE_NAME}/load_date={run_date}"
    print(f"[dim_maintenance] run_date={run_date}")
    print(f"[dim_maintenance] Reading Bronze from: {bronze_partition_path}")

    try:
        bronze_df = spark.read.parquet(bronze_partition_path)
    except Exception as e:
        print(f"[dim_maintenance] ⚠ No Bronze partition for {run_date}: {e}")
        return

    total = bronze_df.count()
    print(f"[dim_maintenance] Bronze rows read: {total}")
    if total == 0:
        print("[dim_maintenance] ⚠ No data. Skipping.")
        return

    incoming_df = clean_snapshot(bronze_df)
    count = incoming_df.count()
    print(f"[dim_maintenance] After cleaning: {count} rows")
    if count == 0:
        print("[dim_maintenance] ⚠ All rows filtered out. Skipping.")
        return

    if silver_table_exists(spark, silver_path):
        # ── SCD TYPE 1 MERGE ──
        print("[dim_maintenance] Silver exists → Delta MERGE (SCD1)")
        silver_table = DeltaTable.forPath(spark, silver_path)

        (
            silver_table.alias("existing")
            .merge(
                incoming_df.alias("incoming"),
                "existing.vin = incoming.vin AND existing.service_date = incoming.service_date"
            )
            .whenMatchedUpdate(
                condition="existing.service_type != incoming.service_type",
                set={
                    "service_type": "incoming.service_type",
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        final_count = spark.read.format("delta").load(silver_path).count()
        print(f"[dim_maintenance] ✓ MERGE complete. Total Silver rows: {final_count}")

        # ── VACUUM: Delete old Parquet files no longer in Delta log ──
        # After SCD1 MERGE, old Parquet files are no longer needed.
        # VACUUM(0) keeps only the latest version to save S3 storage.
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        DeltaTable.forPath(spark, silver_path).vacuum(0)
        print("[dim_maintenance] ✓ VACUUM complete — old Parquet files deleted.")

    else:
        # ── FIRST RUN (Bootstrap) ──
        print("[dim_maintenance] Silver does NOT exist → Bootstrap write")
        (
            incoming_df.write.format("delta")
            .mode("overwrite").option("overwriteSchema", "true")
            .save(silver_path)
        )
        print(f"[dim_maintenance] ✓ Bootstrap: {count} rows → {silver_path}")


# ──────────────────────────────────────────────────────────────
# Glue Job Entry Point
# ──────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "run_date",
    "bronze_ingested_path",
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

bronze_base = normalize_path(args["bronze_ingested_path"])
silver_path = args["silver_output_path"].rstrip("/")
run_date = args.get("run_date", str(date.today()))

print("=" * 60)
print(f"  dim_maintenance Silver Transformation — Glue Job")
print(f"  Run Date     : {run_date}")
print(f"  Bronze Base  : {bronze_base}")
print(f"  Silver Path  : {silver_path}")
print("=" * 60)

try:
    run(spark, run_date, bronze_base, silver_path)
    print("✓ dim_maintenance transformation completed successfully.")
except Exception as e:
    print(f"✗ dim_maintenance transformation failed: {e}")
    raise
finally:
    job.commit()
