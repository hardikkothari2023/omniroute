"""
AWS Glue Job — Silver Transformation: Vehicle Registry (dim_vehicle)
=====================================================================
SCD Type 1 + Soft Delete via Delta Lake MERGE.

ER Table: dim_vehicle
Reads ONLY today's partition from Bronze (partition pruning on load_date),
cleans it, and MERGEs into the Silver Delta table.

Null Handling (Silver enforcement):
  - vin NULL/empty         → QUARANTINE ROW (PK)
  - model NULL/empty       → QUARANTINE ROW (needed for baseline derivation)
  - mfg_year NULL/0/out-of-range → QUARANTINE ROW (must be 2000–current year)
  - fuel_type NULL         → Left as NULL (quarantined by valid fuel list filter)
  - baseline_kmpl NULL     → Derived from model avg (past Silver + current batch).
                             If model has no baseline anywhere → QUARANTINE ROW.

Quarantine:
  - Rejected rows appended to bronze quarantine/{table_name}/ as Parquet
  - Bronze metadata (load_date, batch_id, etc.) preserved on quarantined rows
  - batch_id used for idempotency — re-runs skip if batch already quarantined

Bronze data is NOT moved — stays in ingested/ for auditability.

Glue Job Parameters:
  --run_date              : Partition date (YYYY-MM-DD)
  --bronze_ingested_path  : Base S3 path for Bronze ingested data
  --silver_output_path    : S3 path for Silver Delta table output
"""

import sys
from datetime import date, datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql.functions import (
    col, trim, upper, row_number, lit, current_timestamp, sha2, when,
    avg, coalesce,
)
from pyspark.sql.types import IntegerType, FloatType
from delta.tables import DeltaTable

# ──────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────
TABLE_NAME = "vehicle_registry"
VALID_FUEL_TYPES = {"DIESEL", "LNG", "CNG", "ELECTRIC"}
CURRENT_YEAR = datetime.utcnow().year
MIN_MFG_YEAR = 2000
MAX_MFG_YEAR = CURRENT_YEAR


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────
def normalize_path(path):
    """Ensure S3 path ends with a trailing slash."""
    return path if path.endswith("/") else path + "/"


def derive_quarantine_path(bronze_ingested_path):
    """Derive quarantine base from bronze ingested path.
    e.g. s3://bucket/prefix/ingested/ → s3://bucket/prefix/quarantine/
    """
    return bronze_ingested_path.replace("/ingested/", "/quarantine/")


def silver_table_exists(spark, path):
    """Check whether the Silver Delta table already exists."""
    try:
        DeltaTable.forPath(spark, path)
        return True
    except Exception:
        return False


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
def clean_snapshot(spark, bronze_df, silver_path):
    """
    Clean incoming Bronze vehicle registry snapshot.
    Enforces null handling, dedup, validation, baseline derivation,
    and ER schema alignment.

    Returns:
        (clean_df, quarantine_dfs): Tuple of the cleaned DataFrame and a list
        of DataFrames containing rejected rows tagged with rejection reasons.
    """
    quarantine_dfs = []

    # ── Quarantine NULL/empty VIN (primary key) ──
    rejected_vin = bronze_df.filter(col("vin").isNull() | (col("vin") == ""))
    df = bronze_df.filter(col("vin").isNotNull() & (col("vin") != ""))
    dropped = rejected_vin.count()
    if dropped > 0:
        print(f"[dim_vehicle] Rejected {dropped} rows with NULL/empty VIN")
        quarantine_dfs.append(_tag_rejected(rejected_vin, "NULL_OR_EMPTY_VIN"))

    # ── Normalize VIN → UPPERCASE ──
    df = df.withColumn("vin", trim(upper(col("vin"))))

    # ── Quarantine NULL/empty model (needed for baseline derivation) ──
    rejected_model = df.filter(col("model").isNull() | (trim(col("model")) == ""))
    df = df.filter(col("model").isNotNull() & (trim(col("model")) != ""))
    dropped_model = rejected_model.count()
    if dropped_model > 0:
        print(f"[dim_vehicle] Rejected {dropped_model} rows with NULL/empty model")
        quarantine_dfs.append(_tag_rejected(rejected_model, "NULL_OR_EMPTY_MODEL"))

    # ── Normalize model: TRIM + UPPER ──
    df = df.withColumn("model", trim(upper(col("model"))))

    # ── fuel_type: leave NULL as-is, TRIM + UPPER non-null values ──
    # NULL fuel_types will be naturally excluded by the valid list filter below.
    df = df.withColumn(
        "fuel_type",
        when(
            col("fuel_type").isNull() | (trim(col("fuel_type")) == ""),
            lit(None).cast("string")
        ).otherwise(trim(upper(col("fuel_type"))))
    )

    # ── Quarantine invalid fuel_type (not in BRD-allowed values) ──
    # NULLs are excluded since isin() returns NULL for NULL inputs → filtered out.
    rejected_fuel = df.filter(~col("fuel_type").isin(list(VALID_FUEL_TYPES)))
    df = df.filter(col("fuel_type").isin(list(VALID_FUEL_TYPES)))
    dropped_fuel = rejected_fuel.count()
    if dropped_fuel > 0:
        print(f"[dim_vehicle] Rejected {dropped_fuel} rows with invalid/NULL fuel_type")
        quarantine_dfs.append(_tag_rejected(rejected_fuel, "INVALID_OR_NULL_FUEL_TYPE"))

    # ── Cast mfg_year to int (NULL stays NULL), quarantine out-of-range ──
    # Valid range: [2000, current_year]. NULL and 0 are both quarantined.
    df = df.withColumn(
        "mfg_year",
        when(col("mfg_year").cast(IntegerType()).isNull(), lit(0))
        .otherwise(col("mfg_year").cast(IntegerType()))
    )
    valid_year_condition = (
        (col("mfg_year") >= MIN_MFG_YEAR) & (col("mfg_year") <= MAX_MFG_YEAR)
    )
    rejected_year = df.filter(~valid_year_condition)
    df = df.filter(valid_year_condition)
    dropped_year = rejected_year.count()
    if dropped_year > 0:
        print(f"[dim_vehicle] Rejected {dropped_year} rows with NULL/0/out-of-range mfg_year (valid: {MIN_MFG_YEAR}–{MAX_MFG_YEAR})")
        quarantine_dfs.append(_tag_rejected(rejected_year, "MFG_YEAR_OUT_OF_RANGE"))

    # ── Dedup by VIN (within single partition, pick one deterministically) ──
    dedup_window = Window.partitionBy("vin").orderBy(col("vin"))
    df = (
        df.withColumn("_rn", row_number().over(dedup_window))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    # ── Derive baseline_efficiency ──
    # Use baseline_kmpl from source if available, else NULL
    baseline_col = (
        col("baseline_kmpl").cast(FloatType())
        if "baseline_kmpl" in bronze_df.columns
        else lit(None).cast("float")
    )
    df = df.withColumn("baseline_efficiency", baseline_col)

    # ── Fill NULL baseline from model averages ──
    # Step 1: Compute average from current batch
    batch_avg = (
        df.filter(col("baseline_efficiency").isNotNull())
        .groupBy("model")
        .agg(avg("baseline_efficiency").alias("batch_avg"))
    )

    # Step 2: Compute average from existing Silver (if table exists)
    silver_avg = None
    if silver_table_exists(spark, silver_path):
        try:
            existing = spark.read.format("delta").load(silver_path)
            silver_avg = (
                existing
                .filter(col("baseline_efficiency").isNotNull() & (col("is_active") == True))  # noqa: E712
                .groupBy("model")
                .agg(avg("baseline_efficiency").alias("silver_avg"))
            )
        except Exception:
            pass

    # Step 3: Join averages and fill NULLs via coalesce
    df = df.join(batch_avg, on="model", how="left")
    if silver_avg is not None:
        df = df.join(silver_avg, on="model", how="left")
        df = df.withColumn(
            "baseline_efficiency",
            when(col("baseline_efficiency").isNotNull(), col("baseline_efficiency"))
            .otherwise(coalesce(col("batch_avg"), col("silver_avg")))
        )
        df = df.drop("batch_avg", "silver_avg")
    else:
        df = df.withColumn(
            "baseline_efficiency",
            when(col("baseline_efficiency").isNotNull(), col("baseline_efficiency"))
            .otherwise(col("batch_avg"))
        )
        df = df.drop("batch_avg")

    # Step 4: Quarantine rows where baseline is still NULL (model never seen before)
    rejected_baseline = df.filter(col("baseline_efficiency").isNull())
    df = df.filter(col("baseline_efficiency").isNotNull())
    dropped_baseline = rejected_baseline.count()
    if dropped_baseline > 0:
        print(f"[dim_vehicle] Rejected {dropped_baseline} rows — model has no baseline data anywhere")
        quarantine_dfs.append(_tag_rejected(rejected_baseline, "NO_BASELINE_EFFICIENCY"))

    # ── Build final Silver schema (no audit_run_id) ──
    df = df.select(
        sha2(col("vin"), 256).alias("vehicle_sk"),
        col("vin"),
        col("model"),
        col("fuel_type"),
        col("mfg_year"),
        col("baseline_efficiency"),
        current_timestamp().alias("created_at"),
        current_timestamp().alias("updated_at"),
        lit(True).alias("is_active"),
    )

    return df, quarantine_dfs


# ──────────────────────────────────────────────────────────────
# Core Transformation Logic
# ──────────────────────────────────────────────────────────────
def run(spark, run_date, bronze_base, silver_path):
    """
    Silver transformation for vehicle_registry → dim_vehicle.
    Reads only today's Bronze partition. Bronze data stays in place.
    """
    bronze_partition_path = f"{bronze_base}{TABLE_NAME}/load_date={run_date}"
    print(f"[dim_vehicle] run_date={run_date}")
    print(f"[dim_vehicle] Reading Bronze from: {bronze_partition_path}")

    try:
        bronze_df = spark.read.parquet(bronze_partition_path)
    except Exception as e:
        print(f"[dim_vehicle] ⚠ No Bronze partition for {run_date}: {e}")
        return

    total = bronze_df.count()
    print(f"[dim_vehicle] Bronze rows read: {total}")
    if total == 0:
        print("[dim_vehicle] ⚠ No data. Skipping.")
        return

    incoming_df, quarantine_dfs = clean_snapshot(spark, bronze_df, silver_path)

    # ── Write quarantined rows to bronze quarantine ──
    quarantine_base = derive_quarantine_path(bronze_base)
    write_quarantine(spark, quarantine_dfs, quarantine_base, TABLE_NAME)

    count = incoming_df.count()
    print(f"[dim_vehicle] After cleaning: {count} rows")
    if count == 0:
        print("[dim_vehicle] ⚠ All rows filtered out. Skipping.")
        return

    if silver_table_exists(spark, silver_path):
        # ── SCD TYPE 1 MERGE + SOFT-DELETE ──
        print("[dim_vehicle] Silver exists → Delta MERGE (SCD1 + soft-delete)")
        silver_table = DeltaTable.forPath(spark, silver_path)

        (
            silver_table.alias("existing")
            .merge(incoming_df.alias("incoming"), "existing.vin = incoming.vin")
            .whenMatchedUpdate(
                condition="""
                    existing.model != incoming.model
                    OR existing.mfg_year != incoming.mfg_year
                    OR existing.fuel_type != incoming.fuel_type
                    OR existing.is_active = FALSE
                """,
                set={
                    "model":              "incoming.model",
                    "mfg_year":           "incoming.mfg_year",
                    "fuel_type":          "incoming.fuel_type",
                    "baseline_efficiency":"incoming.baseline_efficiency",
                    "is_active":          lit(True),
                    "updated_at":         current_timestamp(),
                }
            )
            .whenNotMatchedInsert(values={
                "vehicle_sk":          "incoming.vehicle_sk",
                "vin":                 "incoming.vin",
                "model":               "incoming.model",
                "fuel_type":           "incoming.fuel_type",
                "mfg_year":            "incoming.mfg_year",
                "baseline_efficiency": "incoming.baseline_efficiency",
                "created_at":          "incoming.created_at",
                "updated_at":          "incoming.updated_at",
                "is_active":           "incoming.is_active",
            })
            .whenNotMatchedBySourceUpdate(
                condition="existing.is_active = TRUE",
                set={
                    "is_active":    lit(False),
                    "updated_at":   current_timestamp(),
                }
            )
            .execute()
        )

        final_df = spark.read.format("delta").load(silver_path)
        active = final_df.filter(col("is_active") == True).count()      # noqa: E712
        inactive = final_df.filter(col("is_active") == False).count()    # noqa: E712
        print(f"[dim_vehicle] ✓ MERGE complete.")
        print(f"  Active: {active} | Inactive: {inactive} | Total: {active + inactive}")

        # ── VACUUM: Delete old Parquet files no longer in Delta log ──
        # After MERGE, Delta keeps old Parquet files for time travel.
        # VACUUM(0) removes ALL old files, keeping only the latest version.
        # This saves S3 storage costs but disables time travel to older versions.
        DeltaTable.forPath(spark, silver_path).vacuum()
        print("[dim_vehicle] ✓ VACUUM complete — old Parquet files deleted.")

    else:
        # ── FIRST RUN (Bootstrap) ──
        print("[dim_vehicle] Silver does NOT exist → Bootstrap write")
        (
            incoming_df.write.format("delta")
            .mode("overwrite").option("overwriteSchema", "true")
            .save(silver_path)
        )
        print(f"[dim_vehicle] ✓ Bootstrap: {count} rows → {silver_path}")


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
print(f"  dim_vehicle Silver Transformation — Glue Job")
print(f"  Run Date     : {run_date}")
print(f"  Bronze Base  : {bronze_base}")
print(f"  Silver Path  : {silver_path}")
print("=" * 60)

try:
    run(spark, run_date, bronze_base, silver_path)
    print("✓ dim_vehicle transformation completed successfully.")
except Exception as e:
    print(f"✗ dim_vehicle transformation failed: {e}")
    raise
finally:
    job.commit()
