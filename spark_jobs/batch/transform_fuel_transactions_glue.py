"""
AWS Glue Job — Silver Transformation: Fuel Transactions (fact_fuel)
====================================================================
Incremental insert-only MERGE via Delta Lake.

ER Table: fact_fuel (Granularity: Fuel Transaction)
Reads ONLY today's partition from Bronze (partition pruning on load_date),
cleans it, adds exclusion flags, looks up driver_sk, and MERGEs.

Null Handling (Silver enforcement):
  - transaction_id NULL → DROP ROW (PK)
  - vin NULL/empty      → DROP ROW (FK, can't compute efficiency)
  - fuel_liters NULL/≤0 → DROP ROW (core metric)
  - odometer NULL       → DROP ROW (core metric)
  - timestamp NULL      → DROP ROW (can't derive date)

Date columns (day_of_week, is_weekend) are NOT stored here — they live
in dim_date, joined via date_id FK.

Bronze data is NOT moved — stays in ingested/ for auditability.

Glue Job Parameters:
  --run_date                   : Partition date (YYYY-MM-DD)
  --bronze_ingested_path       : Base S3 path for Bronze ingested data
  --silver_output_path         : S3 path for Silver Delta table output
  --silver_maintenance_path    : S3 path to Silver maintenance Delta table
  --silver_assignment_path     : S3 path to Silver assignment Delta table
  --silver_vehicle_path        : S3 path to Silver vehicle registry (for active VIN filter)
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
    col, to_date, to_timestamp, row_number, current_timestamp,
    when, lit, sha2, date_format, trim, upper,
)
from pyspark.sql.types import FloatType
from delta.tables import DeltaTable


# ──────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────
TABLE_NAME = "fuel_transactions"


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
# Enrichment Helpers
# ──────────────────────────────────────────────────────────────
def add_maintenance_flag(spark, df, silver_maintenance_path):
    """LEFT JOIN with Silver maintenance to flag maintenance days."""
    try:
        maintenance_df = (
            spark.read.format("delta").load(silver_maintenance_path)
            .select(
                col("vin").alias("maint_vin"),
                col("service_date").alias("maint_date"),
            )
            .distinct()
        )
        df = (
            df.join(
                maintenance_df,
                (col("vin") == col("maint_vin")) & (col("txn_date") == col("maint_date")),
                how="left"
            )
            .withColumn(
                "is_maintenance_day",
                when(col("maint_vin").isNotNull(), lit(True)).otherwise(lit(False))
            )
            .drop("maint_vin", "maint_date")
        )
        print("[fact_fuel] ✓ Joined with maintenance schedules")
        return df
    except Exception as e:
        print(f"[fact_fuel] ⚠ Could not read maintenance Silver: {e}")
        return df.withColumn("is_maintenance_day", lit(False))


def add_driver_sk(spark, df, silver_assignment_path):
    """LEFT JOIN with Silver assignment to get driver_sk for each VIN."""
    try:
        assignment_df = (
            spark.read.format("delta").load(silver_assignment_path)
            .filter(col("is_current") == True)   # noqa: E712
            .select(col("vin").alias("asgn_vin"), col("driver_sk").alias("asgn_driver_sk"))
        )
        # Dedup in case multiple current assignments exist
        dedup_window = Window.partitionBy("asgn_vin").orderBy(col("asgn_driver_sk"))
        assignment_df = (
            assignment_df.withColumn("_rn", row_number().over(dedup_window))
            .filter(col("_rn") == 1).drop("_rn")
        )
        df = (
            df.join(assignment_df, col("vin") == col("asgn_vin"), how="left")
            .withColumn(
                "driver_sk",
                when(col("asgn_driver_sk").isNotNull(), col("asgn_driver_sk"))
                .otherwise(lit(None).cast("string"))
            )
            .drop("asgn_vin", "asgn_driver_sk")
        )
        print("[fact_fuel] ✓ Joined with assignment for driver_sk")
        return df
    except Exception as e:
        print(f"[fact_fuel] ⚠ Could not read assignment Silver: {e}")
        return df.withColumn("driver_sk", lit(None).cast("string"))


# ──────────────────────────────────────────────────────────────
# Core Transformation Logic
# ──────────────────────────────────────────────────────────────
def run(spark, run_date, bronze_base, silver_path,
        silver_maintenance_path, silver_assignment_path, silver_vehicle_path):
    """
    INCREMENTAL Silver transformation for fuel_transactions.
    Reads only today's Bronze partition. Bronze data stays in place.
    """
    bronze_partition_path = f"{bronze_base}{TABLE_NAME}/load_date={run_date}"
    print(f"[fact_fuel] run_date={run_date}")
    print(f"[fact_fuel] Reading: {bronze_partition_path}")

    try:
        bronze_df = spark.read.parquet(bronze_partition_path)
    except Exception as e:
        print(f"[fact_fuel] ⚠ No Bronze partition for {run_date}: {e}")
        return

    total = bronze_df.count()
    print(f"[fact_fuel] Bronze rows read: {total}")
    if total == 0:
        print("[fact_fuel] ⚠ Empty partition. Skipping.")
        return

    # ── Drop rows with NULL essential keys ──
    df = bronze_df.filter(
        col("transaction_id").isNotNull() & (col("transaction_id") != "")
        & col("vin").isNotNull() & (col("vin") != "")
    )
    dropped_keys = total - df.count()
    if dropped_keys > 0:
        print(f"[fact_fuel] Dropped {dropped_keys} rows with NULL transaction_id/vin")

    # ── Normalize VIN + transaction_id → UPPERCASE ──
    df = df.withColumn("vin", trim(upper(col("vin"))))
    df = df.withColumn("transaction_id", trim(upper(col("transaction_id"))))

    # ── Parse timestamp → TimestampType ──
    df = df.withColumn(
        "transaction_timestamp",
        to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
    )
    before_ts = df.count()
    df = df.filter(col("transaction_timestamp").isNotNull())
    dropped_ts = before_ts - df.count()
    if dropped_ts > 0:
        print(f"[fact_fuel] Dropped {dropped_ts} rows with unparseable timestamp")

    # ── Drop NULL/invalid fuel_liters and odometer ──
    df = df.filter(
        col("fuel_liters").cast(FloatType()).isNotNull()
        & (col("fuel_liters").cast(FloatType()) > 0)
        & col("odometer_reading").cast(FloatType()).isNotNull()
    )

    # ── Dedup by transaction_id ──
    dedup_window = Window.partitionBy("transaction_id").orderBy(col("transaction_timestamp"))
    df = (
        df.withColumn("_rn", row_number().over(dedup_window))
        .filter(col("_rn") == 1).drop("_rn")
    )
    print(f"[fact_fuel] After dedup + null filter: {df.count()} rows")

    # ── Derive txn_date ──
    df = df.withColumn("txn_date", to_date(col("transaction_timestamp")))

    # ── Filter: Only keep transactions from the PREVIOUS day ──
    # In Airflow 3.x, {{ ds }} = data_interval_start = today (e.g., 2026-05-03).
    # But fuel CSVs arriving at 07:00 UTC contain YESTERDAY's transactions.
    # So we filter for txn_date = run_date - 1 day.
    from datetime import timedelta as td
    target_date = str(date.fromisoformat(run_date) - td(days=1))
    print(f"[fact_fuel] run_date={run_date}, target transaction date={target_date}")

    before_date_filter = df.count()
    df = df.filter(col("txn_date") == lit(target_date))
    dropped_date = before_date_filter - df.count()
    if dropped_date > 0:
        print(f"[fact_fuel] Dropped {dropped_date} rows — txn_date != {target_date}")
    print(f"[fact_fuel] After date filter (txn_date = {target_date}): {df.count()} rows")

    if df.count() == 0:
        print(f"[fact_fuel] ⚠ No transactions found for {target_date}. Skipping.")
        return

    # ── Add flags and lookups (no weekend flags — those live in dim_date) ──
    df = add_maintenance_flag(spark, df, silver_maintenance_path)
    df = add_driver_sk(spark, df, silver_assignment_path)

    # ── Filter: Only keep VINs that are ACTIVE in Silver Vehicle Registry ──
    # For fuel transactions, pre-MERGE filter is correct:
    # We only INSERT new transactions — old transactions stay as-is
    # (they were valid when the vehicle was active).
    try:
        unique_vins_before = df.select("vin").distinct().count()
        print(f"[fact_fuel] Unique VINs in incoming data (before registry check): {unique_vins_before}")

        registry_df = (
            spark.read.format("delta").load(silver_vehicle_path)
            .filter(col("is_active") == True)  # noqa: E712
            .select(col("vin").alias("reg_vin"))
        )
        active_vin_count = registry_df.count()
        print(f"[fact_fuel] Active VINs in vehicle registry: {active_vin_count}")

        before_filter = df.count()
        df = (
            df.join(registry_df, col("vin") == col("reg_vin"), "inner")
            .drop("reg_vin")
        )
        after_filter = df.count()
        unique_vins_after = df.select("vin").distinct().count()
        dropped_inactive = before_filter - after_filter

        if dropped_inactive > 0:
            print(f"[fact_fuel] Dropped {dropped_inactive} rows — VIN not active in vehicle registry")
        print(f"[fact_fuel] Unique VINs after registry check: {unique_vins_after}")
        print(f"[fact_fuel] After active VIN filter: {after_filter} rows")
    except Exception as e:
        print(f"[fact_fuel] ⚠ Could not read vehicle registry: {e}. Skipping active VIN filter.")

    # ── Select final Silver columns per ER ──
    # day_of_week and is_weekend are NOT included — they live in dim_date.
    df = df.select(
        sha2(col("transaction_id"), 256).alias("fuel_trx_sk"),
        col("transaction_id"),
        sha2(col("vin"), 256).alias("vehicle_sk"),
        col("driver_sk"),
        date_format(col("txn_date"), "yyyyMMdd").cast("int").alias("date_id"),
        col("vin"),
        col("transaction_timestamp"),
        col("fuel_liters").cast(FloatType()).alias("fuel_liters"),
        col("odometer_reading").cast(FloatType()).alias("odometer_reading_km"),
        col("txn_date"),
        col("is_maintenance_day"),
        current_timestamp().alias("created_at"),
    )

    incoming_count = df.count()
    print(f"[fact_fuel] Incoming records: {incoming_count}")
    if incoming_count == 0:
        print("[fact_fuel] ⚠ All rows filtered out. Skipping.")
        return

    if silver_table_exists(spark, silver_path):
        # ── INSERT-ONLY MERGE (idempotent) ──
        print("[fact_fuel] Silver exists → Delta MERGE (insert-only)")

        # Enable schema auto-merge so new columns are added automatically
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

        silver_table = DeltaTable.forPath(spark, silver_path)
        (
            silver_table.alias("existing")
            .merge(df.alias("incoming"), "existing.transaction_id = incoming.transaction_id")
            .whenNotMatchedInsert(values={
                "fuel_trx_sk":              "incoming.fuel_trx_sk",
                "transaction_id":           "incoming.transaction_id",
                "vehicle_sk":               "incoming.vehicle_sk",
                "driver_sk":                "incoming.driver_sk",
                "date_id":                  "incoming.date_id",
                "vin":                      "incoming.vin",
                "transaction_timestamp":    "incoming.transaction_timestamp",
                "fuel_liters":              "incoming.fuel_liters",
                "odometer_reading_km":      "incoming.odometer_reading_km",
                "txn_date":                 "incoming.txn_date",
                "is_maintenance_day":       "incoming.is_maintenance_day",
                "created_at":               "incoming.created_at",
            })
            .execute()
        )
        final_count = spark.read.format("delta").load(silver_path).count()
        print(f"[fact_fuel] ✓ MERGE complete. Total Silver rows: {final_count}")

        # ── VACUUM: Delete old Parquet files no longer in Delta log ──
        # After insert-only MERGE, old Parquet files are no longer needed.
        # VACUUM(0) keeps only the latest version to save S3 storage.
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        DeltaTable.forPath(spark, silver_path).vacuum(0)
        print("[fact_fuel] ✓ VACUUM complete — old Parquet files deleted.")
    else:
        # ── FIRST RUN (Bootstrap) ──
        print("[fact_fuel] Silver does NOT exist → Bootstrap")
        (
            df.write.format("delta")
            .mode("overwrite").option("overwriteSchema", "true")
            .save(silver_path)
        )
        print(f"[fact_fuel] ✓ Bootstrap: {incoming_count} rows → {silver_path}")


# ──────────────────────────────────────────────────────────────
# Glue Job Entry Point
# ──────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "run_date",
    "bronze_ingested_path",
    "silver_output_path",
    "silver_maintenance_path",
    "silver_assignment_path",
    "silver_vehicle_path",
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
silver_maintenance_path = args["silver_maintenance_path"].rstrip("/")
silver_assignment_path = args["silver_assignment_path"].rstrip("/")
silver_vehicle_path = args["silver_vehicle_path"].rstrip("/")

run_date = args.get("run_date", str(date.today()))

print("=" * 60)
print(f"  fact_fuel Silver Transformation — Glue Job")
print(f"  Run Date          : {run_date}")
print(f"  Bronze Base       : {bronze_base}")
print(f"  Silver Output     : {silver_path}")
print(f"  Silver Maintenance: {silver_maintenance_path}")
print(f"  Silver Assignment : {silver_assignment_path}")
print("=" * 60)

try:
    run(spark, run_date, bronze_base, silver_path,
        silver_maintenance_path, silver_assignment_path, silver_vehicle_path)
    print("✓ fact_fuel transformation completed successfully.")
except Exception as e:
    print(f"✗ fact_fuel transformation failed: {e}")
    raise
finally:
    job.commit()
