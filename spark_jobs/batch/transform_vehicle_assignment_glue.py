"""
AWS Glue Job — Silver Transformation: Vehicle Assignment (dim_vehicle_assignment_scd2)
======================================================================================
SCD Type 2 (History/Bridge) via Delta Lake MERGE.

ER Table: dim_vehicle_assignment_scd2
Reads ONLY today's partition from Bronze (partition pruning on load_date),
cleans it, derives status, and MERGEs into the Silver Delta table.

Null Handling (Silver enforcement):
  - vin NULL/empty            → DROP ROW (FK)
  - driver_id NULL/empty      → DROP ROW (FK)
  - start_timestamp NULL      → DROP ROW (can't derive start_date)
  - daily_rate NULL/≤0        → DROP ROW
  - region NULL/empty         → Left as NULL

Bronze data is NOT moved — stays in ingested/ for auditability.

MERGE Logic:
  Match key: (vin, start_date)
  WHEN MATCHED AND data changed → UPDATE
  WHEN NOT MATCHED              → INSERT

Glue Job Parameters:
  --run_date              : Partition date (YYYY-MM-DD)
  --bronze_ingested_path  : Base S3 path for Bronze ingested data
  --silver_output_path    : S3 path for Silver Delta table output
  --silver_vehicle_path   : S3 path to Silver vehicle registry (for active VIN filter)
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
    col, trim, upper, row_number, from_unixtime, to_date,
    current_timestamp, when, lit, sha2, concat_ws, to_timestamp,
)
from pyspark.sql.types import LongType, FloatType, DecimalType
from delta.tables import DeltaTable


# ──────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────
TABLE_NAME = "vehicle_assignment"


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
def clean_incoming_batch(bronze_df):
    """
    Clean a batch of Bronze vehicle assignment records.
    Enforces null handling, dedup, status derivation, and ER schema.
    """
    total_before = bronze_df.count()

    # ── Drop NULL vin or driver_id (both required FKs) ──
    df = bronze_df.filter(
        col("vin").isNotNull() & (col("vin") != "")
        & col("driver_id").isNotNull() & (col("driver_id") != "")
    )
    dropped_keys = total_before - df.count()
    if dropped_keys > 0:
        print(f"[dim_vehicle_assignment_scd2] Dropped {dropped_keys} rows with NULL vin/driver_id")

    # ── Normalize VIN + driver_id → UPPERCASE ──
    df = df.withColumn("vin", trim(upper(col("vin"))))
    df = df.withColumn("driver_id", trim(upper(col("driver_id"))))

    # ── Convert Unix timestamps → Timestamp (preserves time) + Date (for keys) ──
    df = (
        df
        .withColumn(
            "start_datetime",
            from_unixtime(col("start_timestamp").cast(LongType())).cast("timestamp")
        )
        .withColumn(
            "start_date",
            to_date(col("start_datetime"))
        )
        .withColumn(
            "end_datetime",
            when(
                col("end_timestamp").isNotNull(),
                from_unixtime(col("end_timestamp").cast(LongType())).cast("timestamp")
            ).otherwise(lit(None).cast("timestamp"))
        )
        .withColumn(
            "end_date",
            to_date(col("end_datetime"))
        )
    )

    # ── Drop rows where start_timestamp couldn't be parsed ──
    before_start = df.count()
    df = df.filter(col("start_date").isNotNull())
    dropped_start = before_start - df.count()
    if dropped_start > 0:
        print(f"[dim_vehicle_assignment_scd2] Dropped {dropped_start} rows with NULL start_date")

    # ── Drop NULL/invalid daily_rate ──
    before_rate = df.count()
    df = df.filter(
        col("daily_rate").cast(FloatType()).isNotNull()
        & (col("daily_rate").cast(FloatType()) > 0)
    )
    dropped_rate = before_rate - df.count()
    if dropped_rate > 0:
        print(f"[dim_vehicle_assignment_scd2] Dropped {dropped_rate} rows with NULL/invalid daily_rate")

    # ── region: leave NULL as-is, TRIM + UPPER non-null values ──
    df = df.withColumn(
        "region",
        when(
            col("region").isNull() | (trim(col("region")) == ""),
            lit(None).cast("string")
        ).otherwise(trim(upper(col("region"))))
    )

    # ── Dedup WITHIN batch: same VIN + same start_date → keep highest daily_rate ──
    dedup_window = Window.partitionBy("vin").orderBy(col("daily_rate").desc())                    #change here now just start date gyi yaha se 
    df = (
        df.withColumn("_rn", row_number().over(dedup_window))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    # ── Derive status and is_current from end_date ──
    df = (
        df
        .withColumn(
            "status",
            when(col("end_date").isNull(), lit("IN-TRANSIT"))
            .otherwise(lit("ARCHIVED"))
        )
        .withColumn("is_current", col("end_date").isNull())
    )

    # ── Build final Silver schema (no audit_run_id) ──
    df = df.select(
        sha2(concat_ws("|", col("vin"), col("start_date").cast("string")), 256).alias("assignment_sk"),
        sha2(col("vin"), 256).alias("vehicle_sk"),
        sha2(col("driver_id"), 256).alias("driver_sk"),
        col("vin"),
        col("driver_id"),
        col("region"),
        col("daily_rate").cast(DecimalType(10, 2)).alias("daily_rate"),
        col("start_datetime"),
        col("start_date"),
        col("end_datetime"),
        col("end_date"),
        col("is_current"),
        col("status"),
        current_timestamp().alias("created_at"),
        current_timestamp().alias("updated_at"),
    )

    return df


# ──────────────────────────────────────────────────────────────
# Core Transformation Logic
# ──────────────────────────────────────────────────────────────
def run(spark, run_date, bronze_base, silver_path, silver_vehicle_path):
    """
    INCREMENTAL Silver transformation for vehicle_assignment.
    Reads only today's Bronze partition. Bronze data stays in place.
    Only VINs active in the Silver Vehicle Registry are kept.
    """
    bronze_partition_path = f"{bronze_base}{TABLE_NAME}/load_date={run_date}"
    print(f"[dim_vehicle_assignment_scd2] run_date={run_date}")
    print(f"[dim_vehicle_assignment_scd2] Reading: {bronze_partition_path}")

    try:
        bronze_df = spark.read.parquet(bronze_partition_path)
    except Exception as e:
        print(f"[dim_vehicle_assignment_scd2] ⚠ No Bronze partition for {run_date}: {e}")
        return

    total = bronze_df.count()
    print(f"[dim_vehicle_assignment_scd2] Bronze rows read: {total}")
    if total == 0:
        print("[dim_vehicle_assignment_scd2] ⚠ Empty partition. Skipping.")
        return

    incoming_df = clean_incoming_batch(bronze_df)
    count = incoming_df.count()
    print(f"[dim_vehicle_assignment_scd2] After cleaning: {count} rows")
    if count == 0:
        print("[dim_vehicle_assignment_scd2] ⚠ All rows filtered out. Skipping.")
        return

    # ── NO pre-MERGE VIN filter — we MERGE all incoming data first, ──
    # ── then archive assignments for inactive VINs post-MERGE.      ──

    if silver_table_exists(spark, silver_path):
        # ── SCD TYPE 2: TWO-PASS MERGE ──
        # SCD2 requires TWO operations because we need to both:
        #   1. CLOSE the old active record (set end_date, is_current=False)
        #   2. INSERT the new active record
        # A single MERGE cannot do both for the same incoming row.
        print("[dim_vehicle_assignment_scd2] Silver exists → Two-Pass SCD2 MERGE")

        # Enable schema auto-merge for any new columns
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

        # ─── PASS 1: Close old active records ───
        # Match: same VIN + currently active (is_current=True)
        # If incoming has a DIFFERENT start_date → this is a new assignment
        # → close the old one by setting end_date = incoming.start_date
        print("[dim_vehicle_assignment_scd2] Pass 1: Closing old active records...")
        silver_table_p1 = DeltaTable.forPath(spark, silver_path)
        (
            silver_table_p1.alias("existing")
            .merge(
                incoming_df.alias("incoming"),
                "existing.vin = incoming.vin AND existing.is_current = TRUE"
            )
            .whenMatchedUpdate(
                # Only close if this is a genuinely NEW assignment (different start_date)
                condition="existing.start_date != incoming.start_date",
                set={
                    "end_date":       "incoming.start_date",
                    "end_datetime":   "incoming.start_datetime",
                    "is_current":     lit(False),
                    "status":         lit("ARCHIVED"),
                    "updated_at":     current_timestamp(),
                }
            )
            .execute()
        )
        closed_count = spark.read.format("delta").load(silver_path).filter(
            (col("is_current") == False) & (col("status") == "ARCHIVED")  # noqa: E712
        ).count()
        print(f"[dim_vehicle_assignment_scd2] Pass 1 done. Total ARCHIVED records: {closed_count}")

        # ─── PASS 2: Insert new records / Update existing records ───
        # Match: same VIN + same start_date (unique assignment key)
        # - If matched AND data changed → UPDATE (rate correction, driver swap)
        # - If not matched → INSERT (new assignment)
        print("[dim_vehicle_assignment_scd2] Pass 2: Inserting new / updating existing...")
        silver_table_p2 = DeltaTable.forPath(spark, silver_path)
        (
            silver_table_p2.alias("existing")
            .merge(
                incoming_df.alias("incoming"),
                "existing.vin = incoming.vin AND existing.start_date = incoming.start_date"
            )
            .whenMatchedUpdate(
                condition="""
                    incoming.daily_rate != existing.daily_rate
                    OR incoming.driver_id != existing.driver_id
                    OR (incoming.end_date IS NOT NULL AND existing.end_date IS NULL)
                    OR (incoming.end_date IS NULL AND existing.end_date IS NOT NULL)
                    OR (incoming.end_date != existing.end_date)
                """,
                set={
                    "driver_id":      "incoming.driver_id",
                    "driver_sk":      "incoming.driver_sk",
                    "end_datetime":   "incoming.end_datetime",
                    "end_date":       "incoming.end_date",
                    "daily_rate":     "incoming.daily_rate",
                    "region":         "incoming.region",
                    "status":         "incoming.status",
                    "is_current":     "incoming.is_current",
                    "updated_at":     current_timestamp(),
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        final_df = spark.read.format("delta").load(silver_path)
        active_before = final_df.filter(col("is_current") == True).count()   # noqa: E712
        print(f"[dim_vehicle_assignment_scd2] ✓ MERGE complete. IN-TRANSIT before registry check: {active_before}")

        # ── POST-MERGE: Archive assignments whose VIN is no longer active ──
        # This catches BOTH today's incoming + all historical Silver records.
        try:
            unique_vins_before = final_df.filter(col("is_current") == True).select("vin").distinct().count()  # noqa: E712
            print(f"[dim_vehicle_assignment_scd2] Unique VINs with is_current=True (before registry check): {unique_vins_before}")

            active_vins_df = (
                spark.read.format("delta").load(silver_vehicle_path)
                .filter(col("is_active") == True)  # noqa: E712
                .select(col("vin").alias("reg_vin"))
            )
            active_vin_count = active_vins_df.count()
            print(f"[dim_vehicle_assignment_scd2] Active VINs in vehicle registry: {active_vin_count}")

            # Find VINs in Silver assignment that are is_current=True but NOT in active registry
            inactive_assignments = (
                final_df.filter(col("is_current") == True)  # noqa: E712
                .join(active_vins_df, col("vin") == col("reg_vin"), "left_anti")
                .select("vin", "start_date")
            )
            inactive_count = inactive_assignments.count()

            if inactive_count > 0:
                inactive_unique_vins = inactive_assignments.select("vin").distinct().count()
                print(f"[dim_vehicle_assignment_scd2] VINs to archive (not in active registry): {inactive_unique_vins} unique VINs, {inactive_count} assignment records")

                # Use Delta UPDATE to archive these assignments
                silver_table_post = DeltaTable.forPath(spark, silver_path)
                silver_table_post.update(
                    condition=(
                        col("is_current") == True  # noqa: E712
                    ) & (
                        col("vin").isin(
                            [row["vin"] for row in inactive_assignments.collect()]
                        )
                    ),
                    set={
                        "is_current": lit(False),
                        "status":     lit("ARCHIVED"),
                        "updated_at": current_timestamp(),
                    }
                )
                print(f"[dim_vehicle_assignment_scd2] ✓ Archived {inactive_count} assignments — VIN no longer active in registry")
            else:
                print("[dim_vehicle_assignment_scd2] ✓ All current assignments have active VINs")

        except Exception as e:
            print(f"[dim_vehicle_assignment_scd2] ⚠ Could not enforce active VIN check: {e}. Skipping post-MERGE archive.")

        # ── Final counts ──
        final_df2 = spark.read.format("delta").load(silver_path)
        active = final_df2.filter(col("is_current") == True).count()     # noqa: E712
        archived = final_df2.filter(col("is_current") == False).count()  # noqa: E712
        unique_active_vins = final_df2.filter(col("is_current") == True).select("vin").distinct().count()  # noqa: E712
        print(f"[dim_vehicle_assignment_scd2] Final counts:")
        print(f"  IN-TRANSIT: {active} | ARCHIVED: {archived} | Total: {active + archived}")
        print(f"  Unique VINs with active assignment: {unique_active_vins}")

        # ── VACUUM: Delete old Parquet files no longer in Delta log ──
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        DeltaTable.forPath(spark, silver_path).vacuum(0)
        print("[dim_vehicle_assignment_scd2] ✓ VACUUM complete — old Parquet files deleted.")

    else:
        # ── FIRST RUN (Bootstrap) ──
        print("[dim_vehicle_assignment_scd2] Silver does NOT exist → Bootstrap")
        (
            incoming_df.write.format("delta")
            .mode("overwrite").option("overwriteSchema", "true")
            .save(silver_path)
        )
        print(f"[dim_vehicle_assignment_scd2] ✓ Bootstrap: {count} rows → {silver_path}")


# ──────────────────────────────────────────────────────────────
# Glue Job Entry Point
# ──────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "run_date",
    "bronze_ingested_path",
    "silver_output_path",
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
silver_vehicle_path = args["silver_vehicle_path"].rstrip("/")
run_date = args.get("run_date", str(date.today()))

print("=" * 60)
print(f"  dim_vehicle_assignment_scd2 Silver Transformation — Glue Job")
print(f"  Run Date     : {run_date}")
print(f"  Bronze Base  : {bronze_base}")
print(f"  Silver Path  : {silver_path}")
print("=" * 60)

try:
    run(spark, run_date, bronze_base, silver_path, silver_vehicle_path)
    print("✓ dim_vehicle_assignment_scd2 transformation completed successfully.")
except Exception as e:
    print(f"✗ dim_vehicle_assignment_scd2 transformation failed: {e}")
    raise
finally:
    job.commit()
