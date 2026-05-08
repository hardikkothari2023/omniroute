"""
AWS Glue Job — Monthly Rate Deduction Report
===============================================
Reads daily safety snapshots for the previous month from S3 Gold,
computes per-driver payroll deductions, writes the report to the
Gold Delta table, and uploads a plain-text manager summary to S3.

Runs on the 1st of every month as part of the omniroute_monthly_pipeline
DAG (before the safety_strikes_reset job).

Logic:
  For each driver, using all daily snapshots of the target month:
    - total_strikes        = strike_count from the LAST snapshot of the month
    - days_active          = COUNT of days where status = 'ACTIVE'
    - final_payable        = SUM(current_adjusted_rate) for ACTIVE days
                             (SUSPENDED days contribute $0)
    - total_rate_deduction = SUM(base_rate) for ACTIVE days − final_payable
    - final_status         = status from the LAST snapshot of the month

Output:
  1. Gold Delta table  — gold.monthly_rate_deduction/ (no partition; one
                         report per month, idempotent via MERGE)
  2. Manager TXT file  — gold.manager_reports/<YYYY-MM>/
                         monthly_rate_deduction_report_<YYYY-MM>.txt

Glue Job Parameters:
  --run_month          : Month being reported (YYYY-MM)
  --run_date           : Logical date (YYYY-MM-DD)
  --gold_snapshot_path : S3 path to gold.daily_safety_snapshot/ Delta table
  --gold_output_path   : S3 path for gold.monthly_rate_deduction/ Delta table
  --gold_report_path   : (optional) S3 base path for gold.manager_reports/
                         Defaults to same bucket/prefix as gold_output_path
                         with the table name replaced by gold.manager_reports

Idempotency:
  Delta table uses MERGE on (driver_id, report_month).
  TXT file is overwritten on every run (S3 PUT is atomic).
"""

import sys
from datetime import date, timedelta

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql.functions import (
    col, lit, current_timestamp, sum as spark_sum,
    count, max as spark_max, row_number,
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


def get_month_boundaries(run_month):
    """
    Given run_month (YYYY-MM), return the first and last date of the
    PREVIOUS month (the month we are reporting on).

    The monthly DAG runs on the 1st, so run_month = current month.
    We report on the previous month.
    """
    # Parse the 1st of run_month
    year, month = map(int, run_month.split("-"))
    first_of_run_month = date(year, month, 1)

    # Previous month's last day = day before run_month's 1st
    last_day_prev = first_of_run_month - timedelta(days=1)

    # Previous month's first day
    first_day_prev = date(last_day_prev.year, last_day_prev.month, 1)

    return str(first_day_prev), str(last_day_prev)


def _derive_manager_reports_path(gold_output_path):
    """
    Derive the default gold.manager_reports base path from gold_output_path.

    Example:
      gold_output_path = "s3://bucket/prefix/gold.monthly_rate_deduction"
      →  returns        "s3://bucket/prefix/gold.manager_reports"
    """
    # Strip trailing slash then replace the last path component
    base = gold_output_path.rstrip("/")
    parts = base.rsplit("/", 1)
    return parts[0] + "/gold.manager_reports"


def write_txt_manager_report(report_df, report_month_str, gold_report_path):
    """
    Build a plain-text manager summary from report_df and upload it to S3 at:
      <gold_report_path>/<YYYY-MM>/monthly_rate_deduction_report_<YYYY-MM>.txt

    Args:
        report_df       : Spark DataFrame with the final report rows.
        report_month_str: String like "2026-04" (the reported month, NOT run_month).
        gold_report_path: S3 base path, e.g.
                          "s3://bucket/prefix/gold.manager_reports"
    """
    # ── Collect rows (report is small — O(#drivers)) ──
    rows = report_df.orderBy("driver_id").collect()

    # ── Compute fleet-level totals ──
    total_drivers      = len(rows)
    total_active       = sum(1 for r in rows if r["final_status"] == "ACTIVE")
    total_suspended    = total_drivers - total_active
    total_payable      = sum(float(r["final_payable"] or 0.0) for r in rows)
    total_deduction    = sum(float(r["total_rate_deduction"] or 0.0) for r in rows)
    total_strikes_all  = sum(int(r["total_strikes"] or 0) for r in rows)

    # ── Build the TXT content ──
    lines = []
    lines.append("=" * 70)
    lines.append(f"  OmniRoute — Monthly Rate Deduction Manager Report")
    lines.append(f"  Reporting Month : {report_month_str}")
    lines.append("=" * 70)
    lines.append("")
    lines.append("FLEET SUMMARY")
    lines.append("-" * 40)
    lines.append(f"  Total Drivers      : {total_drivers}")
    lines.append(f"  Active Drivers     : {total_active}")
    lines.append(f"  Suspended Drivers  : {total_suspended}")
    lines.append(f"  Total Strikes      : {total_strikes_all}")
    lines.append(f"  Total Payable (INR): {total_payable:,.2f}")
    lines.append(f"  Total Deductions   : {total_deduction:,.2f}")
    lines.append("")
    lines.append("DRIVER BREAKDOWN")
    lines.append("-" * 70)
    header = (
        f"{'Driver ID':<15}"
        f"{'Status':<12}"
        f"{'Strikes':>8}"
        f"{'Days Active':>13}"
        f"{'Payable (INR)':>16}"
        f"{'Deduction (INR)':>17}"
    )
    lines.append(header)
    lines.append("-" * 70)
    for r in rows:
        line = (
            f"{str(r['driver_id']):<15}"
            f"{str(r['final_status']):<12}"
            f"{int(r['total_strikes'] or 0):>8}"
            f"{int(r['days_active'] or 0):>13}"
            f"{float(r['final_payable'] or 0.0):>16,.2f}"
            f"{float(r['total_rate_deduction'] or 0.0):>17,.2f}"
        )
        lines.append(line)
    lines.append("-" * 70)
    lines.append("")
    lines.append("(Generated by monthly_rate_deduction_report_glue.py)")
    lines.append("=" * 70)

    txt_content = "\n".join(lines) + "\n"

    # ── Parse S3 path and upload ──
    # gold_report_path format: s3://<bucket>/<prefix>/gold.manager_reports
    path_stripped = gold_report_path.rstrip("/")
    assert path_stripped.startswith("s3://"), \
        f"gold_report_path must start with s3://, got: {gold_report_path}"

    without_scheme = path_stripped[len("s3://"):]
    bucket, _, prefix_base = without_scheme.partition("/")

    object_key = f"{prefix_base}/monthly_rate_deduction_report_{report_month_str}.txt"

    print(f"  Uploading manager report → s3://{bucket}/{object_key}")
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=bucket,
        Key=object_key,
        Body=txt_content.encode("utf-8"),
        ContentType="text/plain",
    )
    print(f"  ✓ Manager report uploaded successfully.")


# ──────────────────────────────────────────────────────────────
# Core Logic
# ──────────────────────────────────────────────────────────────
def run(spark, run_month, gold_snapshot_path, gold_output_path, gold_report_path):
    """
    Read daily safety snapshots for the previous month, compute
    per-driver payroll deductions, write to Gold Delta table, and
    upload a plain-text manager report to S3.
    """
    month_start, month_end = get_month_boundaries(run_month)
    report_month = f"{month_start}"  # 1st of the reported month

    # Derive YYYY-MM label for the reported month (e.g. "2026-04")
    report_month_label = month_start[:7]   # first 7 chars of "YYYY-MM-DD"

    print(f"[monthly_rate_deduction] run_month={run_month}")
    print(f"[monthly_rate_deduction] Reporting on: {month_start} → {month_end}")
    print(f"[monthly_rate_deduction] Manager report target: {gold_report_path}/{report_month_label}/")

    # ── Step 1: Read daily snapshots for the target month ──
    print(f"\n[1/4] Reading daily snapshots from {gold_snapshot_path}...")
    try:
        all_snapshots = spark.read.format("delta").load(gold_snapshot_path)
    except Exception as e:
        print(f"  ✗ Failed to read daily snapshots: {e}")
        raise

    month_snapshots = all_snapshots.filter(
        (col("snapshot_date") >= lit(month_start))
        & (col("snapshot_date") <= lit(month_end))
    )

    snapshot_count = month_snapshots.count()
    distinct_dates = month_snapshots.select("snapshot_date").distinct().count()
    distinct_drivers = month_snapshots.select("driver_id").distinct().count()

    print(f"  ✓ Found {snapshot_count} snapshot rows")
    print(f"    Distinct dates  : {distinct_dates}")
    print(f"    Distinct drivers: {distinct_drivers}")

    if snapshot_count == 0:
        print("  ⚠ No snapshots found for this month. Skipping report.")
        return

    # ── Step 2: Get the LAST snapshot per driver (for total_strikes + final_status) ──
    print("\n[2/4] Computing last-day metrics per driver...")

    window_last = Window.partitionBy("driver_id").orderBy(col("snapshot_date").desc())
    last_snapshot = (
        month_snapshots
        .withColumn("rn", row_number().over(window_last))
        .filter(col("rn") == 1)
        .select(
            col("driver_id"),
            col("strike_count").alias("total_strikes"),
            col("status").alias("final_status"),
        )
    )

    # ── Step 3: Compute ACTIVE-day aggregates ──
    print("[3/4] Computing active-day aggregates...")

    active_snapshots = month_snapshots.filter(col("status") == "ACTIVE")

    active_agg = (
        active_snapshots
        .groupBy("driver_id")
        .agg(
            count("*").alias("days_active"),
            spark_sum("current_adjusted_rate").alias("final_payable"),
            spark_sum("base_rate").alias("total_base_for_active_days"),
        )
    )

    # ── Step 4: Join and compute final report ──
    print("[4/4] Building final report...")

    report_df = (
        last_snapshot
        .join(active_agg, on="driver_id", how="left")
        # Drivers who were SUSPENDED the entire month will have NULLs
        .fillna({
            "days_active": 0,
            "final_payable": 0.0,
            "total_base_for_active_days": 0.0,
        })
        .withColumn(
            "total_rate_deduction",
            col("total_base_for_active_days") - col("final_payable"),
        )
        .withColumn("report_month", lit(report_month).cast("date"))
        .withColumn("created_at", current_timestamp())
        .select(
            "driver_id",
            "report_month",
            "total_strikes",
            "days_active",
            "total_rate_deduction",
            "final_payable",
            "final_status",
            "created_at",
        )
    )

    result_count = report_df.count()
    print(f"\n  Report rows: {result_count}")
    report_df.show(20, truncate=False)

    # ── Write to Gold Delta ──
    print(f"\n  Writing to {gold_output_path}...")

    if gold_table_exists(spark, gold_output_path):
        print("  Gold table exists → Delta MERGE (upsert)")
        gold_table = DeltaTable.forPath(spark, gold_output_path)
        (
            gold_table.alias("existing")
            .merge(
                report_df.alias("incoming"),
                "existing.driver_id = incoming.driver_id "
                "AND existing.report_month = incoming.report_month"
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
        print("  Gold table does NOT exist → Bootstrap write (no partition — one report per month)")
        (
            report_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(gold_output_path)
        )
        print(f"  ✓ Bootstrap: {result_count} rows written")

    # ── Write plain-text manager report to S3 ──
    print(f"\n  Generating manager TXT report for {report_month_label}...")
    try:
        write_txt_manager_report(report_df, report_month_label, gold_report_path)
    except Exception as e:
        print(f"  ✗ Manager report upload failed (non-fatal): {e}")
        raise

    print("\n[monthly_rate_deduction] ✓ Report generated successfully.")


# ──────────────────────────────────────────────────────────────
# Glue Job Entry Point
# ──────────────────────────────────────────────────────────────
# Required args
required_args = ["JOB_NAME", "run_month", "run_date", "gold_snapshot_path", "gold_output_path"]
# Optional: gold_report_path — if not supplied, derived from gold_output_path
optional_args = ["gold_report_path"]

all_arg_names = required_args + [
    a for a in optional_args
    if any(a in arg for arg in sys.argv)
]
args = getResolvedOptions(sys.argv, all_arg_names)

conf = SparkConf()
conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

sc = SparkContext(conf=conf)
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

run_month          = args["run_month"]                    # e.g. "2026-05"
run_date           = args["run_date"]                     # e.g. "2026-05-01"
gold_snapshot_path = args["gold_snapshot_path"].rstrip("/")
gold_output_path   = args["gold_output_path"].rstrip("/")

# gold_report_path: use explicit arg if provided, else derive from gold_output_path
gold_report_path = (
    args["gold_report_path"].rstrip("/")
    if "gold_report_path" in args
    else _derive_manager_reports_path(gold_output_path)
)

print("=" * 60)
print("  Monthly Rate Deduction Report — Glue Job")
print(f"  Run Month       : {run_month}")
print(f"  Run Date        : {run_date}")
print(f"  Snapshot Input  : {gold_snapshot_path}")
print(f"  Report Output   : {gold_output_path}")
print(f"  Manager Reports : {gold_report_path}")
print("=" * 60)

try:
    run(spark, run_month, gold_snapshot_path, gold_output_path, gold_report_path)
    print("\n✓ Monthly rate deduction report completed successfully.")
except Exception as e:
    print(f"\n✗ Monthly rate deduction report failed: {e}")
    raise
finally:
    job.commit()
