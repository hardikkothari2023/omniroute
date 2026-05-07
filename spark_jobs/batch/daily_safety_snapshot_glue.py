"""
AWS Glue Job — Daily Safety Snapshot
======================================
Archives the current state of report.driver_safety_status from PostgreSQL
to S3 Gold as a daily Delta partition.

Runs daily in the omniroute_midnight_pipeline DAG, after the
gold_active_fleet_snapshot step.

Logic:
  1. Read full report.driver_safety_status table from Postgres via JDBC
  2. Add snapshot_date and archived_at columns
  3. Write to Gold Delta table partitioned by snapshot_date
  4. Use Delta MERGE on (driver_id, snapshot_date) for idempotency

Output Schema (one row per driver per day):
  driver_id, base_rate, strike_count, current_adjusted_rate,
  status, month, snapshot_date (partition), archived_at

Glue Job Parameters:
  --run_date               : Date being snapshotted (YYYY-MM-DD)
  --silver_assignment_path : S3 path to Silver dim_vehicle_assignment_scd2
  --gold_output_path       : S3 path for gold.daily_safety_snapshot/
  --pg_host                : PostgreSQL host
  --pg_port                : PostgreSQL port (default 5432)
  --pg_database            : PostgreSQL database name
  --pg_user                : PostgreSQL username
  --pg_password            : PostgreSQL password

Idempotency:
  Uses Delta MERGE on (driver_id, snapshot_date). Re-running for the
  same date overwrites existing rows rather than duplicating them.
"""

import sys
from datetime import date

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit, current_timestamp, coalesce
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
def run(spark, run_date, silver_assignment_path, gold_output_path, jdbc_url, connection_props):
    """
    Read report.driver_safety_status from Postgres and dim_vehicle_assignment_scd2
    from Silver. Join them to include drivers with 0 strikes, then archive to S3 Gold
    as a daily Delta partition.
    """
    print(f"[daily_safety_snapshot] run_date={run_date}")

    # ── Step 1: Read from Postgres ──
    print("\n[1/3] Reading report.driver_safety_status from PostgreSQL...")
    try:
        pg_df = (
            spark.read.jdbc(
                url=jdbc_url,
                table="report.driver_safety_status",
                properties=connection_props,
            )
        )
    except Exception as e:
        print(f"  ✗ Failed to read from PostgreSQL: {e}")
        raise

    row_count = pg_df.count()
    print(f"  ✓ Read {row_count} driver(s) from Postgres")

    if row_count == 0:
        print("  ⚠ No drivers found in driver_safety_status.")

    # ── Step 2: Read active assignments from Silver ──
    print("\n[2/4] Reading active assignments from Silver...")
    try:
        assignment_df = (
            spark.read.format("delta").load(silver_assignment_path)
            .filter(col("is_current") == True)  # noqa: E712
            .select(
                "driver_id",
                col("daily_rate").alias("assignment_base_rate")
            ).distinct()
        )
    except Exception as e:
        print(f"  ✗ Failed to read silver assignment: {e}")
        raise

    active_count = assignment_df.count()
    print(f"  ✓ Active drivers in assignment (IN TRANSIT): {active_count}")

    # ── Step 3: Join and add snapshot columns ──
    print("\n[3/4] Preparing snapshot DataFrame (RIGHT JOIN from assignment)...")
    
    run_month_str = run_date[:7] + "-01"
    
    snapshot_df = (
        pg_df.join(assignment_df, on="driver_id", how="right")
        .withColumn("base_rate", coalesce(col("base_rate"), col("assignment_base_rate")))
        .withColumn("strike_count", coalesce(col("strike_count"), lit(0)))
        .withColumn("current_adjusted_rate", coalesce(col("current_adjusted_rate"), col("assignment_base_rate")))
        .withColumn("status", coalesce(col("status"), lit("ACTIVE")))
        .withColumn("month", coalesce(col("month"), lit(run_month_str).cast("date")))
        .withColumn("snapshot_date", lit(run_date).cast("date"))
        .withColumn("archived_at", current_timestamp())
    )

    # Select final schema
    snapshot_df = snapshot_df.select(
        "driver_id",
        "base_rate",
        "strike_count",
        "current_adjusted_rate",
        "status",
        "month",
        "snapshot_date",
        "archived_at",
    )

    print(f"  ✓ Snapshot DataFrame ready: {snapshot_df.count()} rows")

    # ── Step 4: Write to Gold Delta ──
    print(f"\n[4/4] Writing to Gold Delta at {gold_output_path}...")

    if gold_table_exists(spark, gold_output_path):
        print("  Gold table exists → Delta MERGE (upsert)")
        gold_table = DeltaTable.forPath(spark, gold_output_path)
        (
            gold_table.alias("existing")
            .merge(
                snapshot_df.alias("incoming"),
                "existing.driver_id = incoming.driver_id "
                "AND existing.snapshot_date = incoming.snapshot_date"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        total = spark.read.format("delta").load(gold_output_path).count()
        print(f"  ✓ MERGE complete. Total Gold rows: {total}")

        # VACUUM old versions
        DeltaTable.forPath(spark, gold_output_path).vacuum()
        print("  ✓ VACUUM complete.")
    else:
        print("  Gold table does NOT exist → Bootstrap write")
        (
            snapshot_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("snapshot_date")
            .save(gold_output_path)
        )
        print(f"  ✓ Bootstrap: {row_count} rows written")

    print("\n[daily_safety_snapshot] ✓ Snapshot archived successfully.")


# ──────────────────────────────────────────────────────────────
# Glue Job Entry Point
# ──────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "run_date",
    "silver_assignment_path",
    "gold_output_path",
    "pg_host",
    "pg_port",
    "pg_database",
    "pg_user",
    "pg_password",
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
silver_assignment_path = args["silver_assignment_path"].rstrip("/")
gold_output_path = args["gold_output_path"].rstrip("/")

# ── Build JDBC URL and connection properties ──
jdbc_url = f"jdbc:postgresql://{args['pg_host']}:{args['pg_port']}/{args['pg_database']}"
connection_props = {
    "user": args["pg_user"],
    "password": args["pg_password"],
    "driver": "org.postgresql.Driver",
    "sslmode": "prefer",
}

print("=" * 60)
print("  Daily Safety Snapshot — Glue Job")
print(f"  Run Date      : {run_date}")
print(f"  Gold Output   : {gold_output_path}")
print(f"  JDBC URL      : {jdbc_url}")
print(f"  PG User       : {args['pg_user']}")
print("=" * 60)

try:
    run(spark, run_date, silver_assignment_path, gold_output_path, jdbc_url, connection_props)
    print("✓ Daily safety snapshot completed successfully.")
except Exception as e:
    print(f"✗ Daily safety snapshot failed: {e}")
    raise
finally:
    job.commit()
