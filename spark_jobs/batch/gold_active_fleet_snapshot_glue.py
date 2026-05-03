"""
AWS Glue Job — Gold Layer: Active Fleet Snapshot (active_fleet_snapshot)
========================================================================
Daily fleet snapshot per BRD Section 3.3.2 + 5.3.2.

Purpose:
  Generate a daily snapshot of IN-TRANSIT vehicles grouped by model.
  This is the "Morning Snapshot" used by management.

BRD Example Output:
  | model            | no_of_active_vehicles |
  | Volvo VNL        | 45                    |
  | Freightliner M2  | 122                   |
  | Isuzu N-Series   | 89                    |

Logic:
  1. Read dim_vehicle_assignment_scd2 WHERE is_current = TRUE (IN-TRANSIT)
  2. Get DISTINCT VINs (a vehicle may have overlapping entries)
  3. JOIN dim_vehicle on vin → get model
  4. GROUP BY model → COUNT(*) as active_vehicle_count
  5. Write as Delta with MERGE on (snapshot_date, model)

Glue Job Parameters:
  --run_date                  : Snapshot date (YYYY-MM-DD)
  --silver_assignment_path    : S3 path to Silver dim_vehicle_assignment_scd2
  --silver_vehicle_path       : S3 path to Silver dim_vehicle Delta table
  --gold_output_path          : S3 path for Gold active_fleet_snapshot Delta table
"""

import sys
from datetime import date

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col, lit, current_timestamp, countDistinct,
)
from delta.tables import DeltaTable


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────
def gold_table_exists(spark, path):
    """Check whether the Gold Delta table already exists."""
    try:
        DeltaTable.forPath(spark, path)
        return True
    except Exception:
        return False


# ──────────────────────────────────────────────────────────────
# Core Logic
# ──────────────────────────────────────────────────────────────
def run(spark, run_date, silver_assignment_path, silver_vehicle_path, gold_output_path):
    """
    Generate daily active fleet snapshot: count IN-TRANSIT vehicles by model.
    """
    print(f"[active_fleet_snapshot] run_date={run_date}")

    # ── Read Silver assignment: only current (IN-TRANSIT) assignments ──
    try:
        assignment_df = (
            spark.read.format("delta").load(silver_assignment_path)
            .filter(col("is_current") == True)  # noqa: E712
            .select("vin")
            .distinct()  # One row per VIN
        )
    except Exception as e:
        print(f"[active_fleet_snapshot] ⚠ Could not read assignment: {e}")
        return

    active_count = assignment_df.count()
    print(f"[active_fleet_snapshot] Active (IN-TRANSIT) vehicles: {active_count}")
    if active_count == 0:
        print("[active_fleet_snapshot] ⚠ No active vehicles. Skipping.")
        return

    # ── Read Silver vehicle for model lookup ──
    try:
        vehicle_df = (
            spark.read.format("delta").load(silver_vehicle_path)
            .filter(col("is_active") == True)  # noqa: E712
            .select(
                col("vin").alias("v_vin"),
                col("model"),
            )
        )
    except Exception as e:
        print(f"[active_fleet_snapshot] ⚠ Could not read dim_vehicle: {e}")
        return

    # ── JOIN assignment with vehicle to get model ──
    joined = assignment_df.join(
        vehicle_df,
        col("vin") == col("v_vin"),
        how="inner"
    ).drop("v_vin")

    # ── GROUP BY model → COUNT ──
    snapshot_df = (
        joined
        .groupBy("model")
        .agg(countDistinct("vin").alias("active_vehicle_count"))
        .withColumn("snapshot_date", lit(run_date).cast("date"))
        .withColumn("snapshot_ts", current_timestamp())
        .withColumn("created_at", current_timestamp())
    )

    # ── Select final schema ──
    snapshot_df = snapshot_df.select(
        "snapshot_date",
        "model",
        "active_vehicle_count",
        "snapshot_ts",
        "created_at",
    )

    result_count = snapshot_df.count()
    print(f"[active_fleet_snapshot] Snapshot rows: {result_count}")
    snapshot_df.show(truncate=False)

    # ── Write to Gold Delta table ──
    if gold_table_exists(spark, gold_output_path):
        print("[active_fleet_snapshot] Gold exists → Delta MERGE")
        gold_table = DeltaTable.forPath(spark, gold_output_path)
        (
            gold_table.alias("existing")
            .merge(
                snapshot_df.alias("incoming"),
                "existing.snapshot_date = incoming.snapshot_date AND existing.model = incoming.model"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        total = spark.read.format("delta").load(gold_output_path).count()
        print(f"[active_fleet_snapshot] ✓ MERGE complete. Total Gold rows: {total}")

        # VACUUM
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        DeltaTable.forPath(spark, gold_output_path).vacuum(0)
        print("[active_fleet_snapshot] ✓ VACUUM complete.")
    else:
        print("[active_fleet_snapshot] Gold does NOT exist → Bootstrap")
        (
            snapshot_df.write.format("delta")
            .mode("overwrite").option("overwriteSchema", "true")
            .save(gold_output_path)
        )
        print(f"[active_fleet_snapshot] ✓ Bootstrap: {result_count} rows")


# ──────────────────────────────────────────────────────────────
# Glue Job Entry Point
# ──────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "run_date",
    "silver_assignment_path",
    "silver_vehicle_path",
    "gold_output_path",
])

conf = SparkConf()
conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

sc = SparkContext(conf=conf)
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

run_date = args.get("run_date", str(date.today()))

print("=" * 60)
print(f"  Active Fleet Snapshot — Gold Layer Glue Job")
print(f"  Run Date          : {run_date}")
print(f"  Silver Assignment : {args['silver_assignment_path']}")
print(f"  Silver Vehicle    : {args['silver_vehicle_path']}")
print(f"  Gold Output       : {args['gold_output_path']}")
print("=" * 60)

try:
    run(spark, run_date,
        args["silver_assignment_path"].rstrip("/"),
        args["silver_vehicle_path"].rstrip("/"),
        args["gold_output_path"].rstrip("/"))
    print("✓ Active Fleet Snapshot completed successfully.")
except Exception as e:
    print(f"✗ Active Fleet Snapshot failed: {e}")
    raise
finally:
    job.commit()
