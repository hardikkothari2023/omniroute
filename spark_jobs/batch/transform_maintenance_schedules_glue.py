"""
AWS Glue Job — Silver Transformation: Maintenance Schedules (dim_maintenance)
===============================================================================
SCD Type 1 via Delta Lake MERGE (no soft-delete per ER diagram).

ER Table: dim_maintenance
Reads ONLY today's partition from Bronze (partition pruning on load_date),
cleans it, validates service_type against known list, and MERGEs into
the Silver Delta table.

Null Handling (Silver enforcement):
  - vin NULL/empty      → QUARANTINE ROW (FK)
  - service_date NULL   → QUARANTINE ROW (can't match to fuel dates)
  - service_type NULL   → Default 'UNKNOWN'

Service Type Fraud Validation:
  - service_type is validated against VALID_SERVICE_TYPES whitelist.
  - If not in list → flagged as 'UNVERIFIED' (row kept, but marked).

Quarantine:
  - Rejected rows appended to bronze quarantine/{table_name}/ as Parquet
  - Bronze metadata (load_date, batch_id, etc.) preserved on quarantined rows
  - batch_id used for idempotency — re-runs skip if batch already quarantined

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


def derive_quarantine_path(bronze_ingested_path):
    """Derive quarantine base from bronze ingested path.
    e.g. s3://bucket/prefix/ingested/ → s3://bucket/prefix/quarantine/
    """
    return bronze_ingested_path.replace("/ingested/", "/quarantine/")


# ──────────────────────────────────────────────────────────────
# Quarantine Helpers
# ──────────────────────────────────────────────────────────────
def _tag_rejected(df, reason):
    """Tag rejected rows with reason. Preserves all columns including
    bronze metadata (load_date, batch_id, ingestion_timestamp, source_file_name).
    """
    return (
        df.select([col(c).cast("string").alias(c) for c in df.columns])
        .withColumn("rejection_reason", lit(reason))
        .withColumn("rejected_at", current_timestamp())
    )


def write_quarantine(spark, quarantine_dfs, quarantine_base, table_name):
    """Union all quarantine DFs and append to quarantine path.
    Uses batch_id from bronze metadata for idempotency.
    """
    if not quarantine_dfs:
        print(f"[{table_name}] ✓ No rows quarantined")
        return

    combined = quarantine_dfs[0]
    for qdf in quarantine_dfs[1:]:
        combined = combined.unionByName(qdf, allowMissingColumns=True)

    count = combined.count()
    if count == 0:
        print(f"[{table_name}] ✓ No rows quarantined")
        return

    output_path = f"{quarantine_base}{table_name}"

    # ── Idempotency: skip if batch_id already in quarantine ──
    if "batch_id" in combined.columns:
        batch_ids = [
            r["batch_id"] for r in
            combined.select("batch_id").distinct().collect()
            if r["batch_id"] is not None
        ]
        if batch_ids:
            try:
                existing = spark.read.parquet(output_path)
                already = existing.filter(
                    col("batch_id").isin(batch_ids)
                ).select("batch_id").distinct().count()
                if already > 0:
                    print(f"[{table_name}] ⚠ batch_id already in quarantine — skipping (idempotent)")
                    return
            except Exception:
                pass  # Quarantine doesn't exist yet

    combined.write.mode("append").parquet(output_path)
    print(f"[{table_name}] ✗ Quarantined {count} rejected rows → {output_path}")


# ──────────────────────────────────────────────────────────────
# Data Cleaning
# ──────────────────────────────────────────────────────────────
def clean_snapshot(bronze_df):
    """
    Clean incoming Bronze maintenance schedules data.
    Returns (clean_df, quarantine_dfs).
    """
    quarantine_dfs = []

    # ── Parse service_date string → Date type ──
    df = bronze_df.withColumn(
        "service_date", to_date(col("service_date"), "yyyy-MM-dd")
    )

    # ── Quarantine NULL/unparseable service_date ──
    rejected_date = df.filter(col("service_date").isNull())
    df = df.filter(col("service_date").isNotNull())
    dropped_date = rejected_date.count()
    if dropped_date > 0:
        print(f"[dim_maintenance] Rejected {dropped_date} rows: NULL/invalid service_date")
        quarantine_dfs.append(_tag_rejected(rejected_date, "NULL_OR_INVALID_SERVICE_DATE"))

    # ── Quarantine NULL/empty VINs ──
    rejected_null_vin = df.filter(col("vin").isNull() | (col("vin") == ""))
    df = df.filter(col("vin").isNotNull() & (col("vin") != ""))
    null_vin_count = rejected_null_vin.count()
    if null_vin_count > 0:
        print(f"[dim_maintenance] Rejected {null_vin_count} rows: NULL/empty VIN")
        quarantine_dfs.append(_tag_rejected(rejected_null_vin, "NULL_OR_EMPTY_VIN"))

    # ── Normalize VIN → UPPERCASE ──
    df = df.withColumn("vin", trim(upper(col("vin"))))

    # ── Quarantine INVALID_ prefixed VINs ──
    rejected_invalid_vin = df.filter(col("vin").startswith("INVALID_"))
    df = df.filter(~col("vin").startswith("INVALID_"))
    invalid_vin_count = rejected_invalid_vin.count()
    if invalid_vin_count > 0:
        print(f"[dim_maintenance] Rejected {invalid_vin_count} rows: INVALID_ VIN prefix")
        quarantine_dfs.append(_tag_rejected(rejected_invalid_vin, "INVALID_VIN_PREFIX"))

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
    unverified_count = df.filter(~col("service_type").isin(list(VALID_SERVICE_TYPES))).count()
    if unverified_count > 0:
        print(f"[dim_maintenance] ⚠ {unverified_count} rows have unrecognized service_type → flagged as UNVERIFIED")
    df = df.withColumn(
        "service_type",
        when(col("service_type").isin(list(VALID_SERVICE_TYPES)), col("service_type"))
        .otherwise(lit("UNVERIFIED"))
    )

    # ── Build final Silver schema ──
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

    return df, quarantine_dfs


# ──────────────────────────────────────────────────────────────
# Core Transformation Logic
# ──────────────────────────────────────────────────────────────
def run(spark, run_date, bronze_base, silver_path):
    """Silver transformation for maintenance_schedules → dim_maintenance."""
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

    incoming_df, quarantine_dfs = clean_snapshot(bronze_df)

    # ── Write quarantined rows to bronze quarantine ──
    quarantine_base = derive_quarantine_path(bronze_base)
    write_quarantine(spark, quarantine_dfs, quarantine_base, TABLE_NAME)

    count = incoming_df.count()
    print(f"[dim_maintenance] After cleaning: {count} rows")
    if count == 0:
        print("[dim_maintenance] ⚠ All rows filtered out. Skipping.")
        return

    if silver_table_exists(spark, silver_path):
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
                set={"service_type": "incoming.service_type"}
            )
            .whenNotMatchedInsert(values={
                "maintenance_sk": "incoming.maintenance_sk",
                "vehicle_sk":     "incoming.vehicle_sk",
                "date_id":        "incoming.date_id",
                "vin":            "incoming.vin",
                "service_date":   "incoming.service_date",
                "service_type":   "incoming.service_type",
                "description":    "incoming.description",
                "created_at":     "incoming.created_at",
            })
            .execute()
        )

        final_count = spark.read.format("delta").load(silver_path).count()
        print(f"[dim_maintenance] ✓ MERGE complete. Total Silver rows: {final_count}")

        DeltaTable.forPath(spark, silver_path).vacuum()
        print("[dim_maintenance] ✓ VACUUM complete — old Parquet files deleted.")

    else:
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
