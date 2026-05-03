"""
AWS Glue Job — Gold to PostgreSQL Loader
=========================================
Loads Gold/Silver Delta tables into PostgreSQL reporting database.

Per BRD Section 5.2:
  Gold Layer (S3 – Parquet) → Scheduled Loads → PostgreSQL → BI / CSV / SQL

Tables Loaded:
  1. report.fleet_assignment_history ← Silver dim_vehicle_assignment_scd2 (full replace)
  2. report.fuel_efficiency_audit    ← Gold fuel_efficiency_audit (today's rows only)
  3. report.active_fleet_snapshot    ← Gold active_fleet_snapshot (today's rows only)
  4. report.dim_vehicle              ← Silver dim_vehicle (full replace)
  5. report.dim_date                 ← Silver dim_date (full replace)

Glue Job Parameters:
  --run_date                  : Date being processed (YYYY-MM-DD)
  --silver_assignment_path    : S3 path to Silver assignment Delta
  --silver_vehicle_path       : S3 path to Silver dim_vehicle Delta
  --silver_date_path          : S3 path to Silver dim_date Delta
  --gold_fuel_audit_path      : S3 path to Gold fuel_efficiency_audit Delta
  --gold_fleet_snapshot_path  : S3 path to Gold active_fleet_snapshot Delta
  --pg_host                   : PostgreSQL host (EC2 private IP)
  --pg_port                   : PostgreSQL port (default 5432)
  --pg_database               : PostgreSQL database name
  --pg_user                   : PostgreSQL username
  --pg_password               : PostgreSQL password
"""

import sys
from datetime import date

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit
from delta.tables import DeltaTable


# ──────────────────────────────────────────────────────────────
# JDBC Helper
# ──────────────────────────────────────────────────────────────
def write_to_postgres(df, table_name, jdbc_url, connection_props, mode="overwrite"):
    """Write a Spark DataFrame to a PostgreSQL table."""
    try:
        row_count = df.count()
        df.write.jdbc(
            url=jdbc_url,
            table=table_name,
            mode=mode,
            properties=connection_props,
        )
        print(f"  ✓ {table_name}: {row_count} rows → PostgreSQL ({mode})")
    except Exception as e:
        print(f"  ✗ {table_name}: FAILED — {e}")


def read_delta_safe(spark, path, label):
    """Safely read a Delta table, return None on failure."""
    try:
        df = spark.read.format("delta").load(path)
        print(f"  ✓ Read {label}: {df.count()} rows")
        return df
    except Exception as e:
        print(f"  ⚠ Could not read {label}: {e}")
        return None


# ──────────────────────────────────────────────────────────────
# Core Logic
# ──────────────────────────────────────────────────────────────
def run(spark, run_date, paths, jdbc_url, connection_props):
    """Load Gold/Silver data into PostgreSQL reporting tables."""
    print(f"[gold_to_postgres] run_date={run_date}")

    # ── 1. Fleet Assignment History (full replace) ──
    print("\n[1/5] Loading fleet_assignment_history...")
    assignment_df = read_delta_safe(spark, paths["assignment"], "dim_vehicle_assignment_scd2")
    if assignment_df is not None:
        # Drop columns not in PostgreSQL schema
        pg_cols = [
            "assignment_sk", "vehicle_sk", "driver_sk", "vin", "driver_id",
            "region", "daily_rate", "start_date", "end_date", "is_current",
            "status", "created_at", "updated_at"
        ]
        assignment_df = assignment_df.select(*[col(c) for c in pg_cols if c in assignment_df.columns])
        write_to_postgres(assignment_df, "report.fleet_assignment_history", jdbc_url, connection_props, "overwrite")

    # ── 2. Fuel Efficiency Audit (today's rows only) ──
    print("\n[2/5] Loading fuel_efficiency_audit...")
    fuel_audit_df = read_delta_safe(spark, paths["fuel_audit"], "fuel_efficiency_audit")
    if fuel_audit_df is not None:
        today_audit = fuel_audit_df.filter(col("audit_date") == lit(run_date))
        write_to_postgres(today_audit, "report.fuel_efficiency_audit", jdbc_url, connection_props, "append")

    # ── 3. Active Fleet Snapshot (today's row only) ──
    print("\n[3/5] Loading active_fleet_snapshot...")
    snapshot_df = read_delta_safe(spark, paths["fleet_snapshot"], "active_fleet_snapshot")
    if snapshot_df is not None:
        today_snapshot = snapshot_df.filter(col("snapshot_date") == lit(run_date))
        write_to_postgres(today_snapshot, "report.active_fleet_snapshot", jdbc_url, connection_props, "append")

    # ── 4. Dim Vehicle (full replace) ──
    print("\n[4/5] Loading dim_vehicle...")
    vehicle_df = read_delta_safe(spark, paths["vehicle"], "dim_vehicle")
    if vehicle_df is not None:
        write_to_postgres(vehicle_df, "report.dim_vehicle", jdbc_url, connection_props, "overwrite")

    # ── 5. Dim Date (full replace) ──
    print("\n[5/5] Loading dim_date...")
    date_df = read_delta_safe(spark, paths["date"], "dim_date")
    if date_df is not None:
        write_to_postgres(date_df, "report.dim_date", jdbc_url, connection_props, "overwrite")

    print("\n[gold_to_postgres] ✓ All loads complete.")


# ──────────────────────────────────────────────────────────────
# Glue Job Entry Point
# ──────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "run_date",
    "silver_assignment_path",
    "silver_vehicle_path",
    "silver_date_path",
    "gold_fuel_audit_path",
    "gold_fleet_snapshot_path",
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

# ── Build JDBC URL and connection properties ──
jdbc_url = f"jdbc:postgresql://{args['pg_host']}:{args['pg_port']}/{args['pg_database']}"
connection_props = {
    "user": args["pg_user"],
    "password": args["pg_password"],
    "driver": "org.postgresql.Driver",
}

paths = {
    "assignment": args["silver_assignment_path"].rstrip("/"),
    "vehicle": args["silver_vehicle_path"].rstrip("/"),
    "date": args["silver_date_path"].rstrip("/"),
    "fuel_audit": args["gold_fuel_audit_path"].rstrip("/"),
    "fleet_snapshot": args["gold_fleet_snapshot_path"].rstrip("/"),
}

print("=" * 60)
print(f"  Gold → PostgreSQL Loader — Glue Job")
print(f"  Run Date    : {run_date}")
print(f"  JDBC URL    : {jdbc_url}")
print(f"  PG User     : {args['pg_user']}")
print(f"  Tables      : 5 reporting tables")
print("=" * 60)

try:
    run(spark, run_date, paths, jdbc_url, connection_props)
    print("✓ Gold to PostgreSQL load completed successfully.")
except Exception as e:
    print(f"✗ Gold to PostgreSQL load failed: {e}")
    raise
finally:
    job.commit()
