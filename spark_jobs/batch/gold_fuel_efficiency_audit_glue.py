"""
AWS Glue Job — Gold Layer: Fuel Efficiency Audit (fuel_efficiency_audit)
========================================================================
Daily fuel efficiency audit per BRD Section 3.3.2.

Purpose:
  Flag vehicles where fuel consumption exceeds the fleet baseline by 12%.
  Exclude weekends and maintenance days.
  Process only today's fuel transactions.

Logic (per BRD):
  1. distance = current_odometer - previous_odometer (per VIN, by timestamp)
  2. km_per_liter = distance / fuel_liters
  3. Compare vs baseline from dim_vehicle
  4. Exclude weekends (JOIN dim_date WHERE is_weekend = FALSE)
  5. Exclude maintenance days (WHERE is_maintenance_day = FALSE)
  6. Flag if variance > 12% below baseline → status = 'FLAGGED'

BRD Example:
  Baseline (Freightliner M2) = 5.0 km/L
  12% threshold = 5.0 - 12% = 4.4 km/L
  Vehicle does 4.0 km/L → FLAGGED

Glue Job Parameters:
  --run_date              : Date to audit (YYYY-MM-DD)
  --silver_fuel_path      : S3 path to Silver fact_fuel Delta table
  --silver_vehicle_path   : S3 path to Silver dim_vehicle Delta table
  --silver_date_path      : S3 path to Silver dim_date Delta table
  --gold_output_path      : S3 path for Gold fuel_efficiency_audit Delta table
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
    col, lag, lit, current_timestamp, abs as spark_abs,
    date_format, when,
)
from pyspark.sql.types import FloatType
from delta.tables import DeltaTable


# ──────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────
VARIANCE_THRESHOLD_PCT = 12.0  # BRD: 12% below baseline = FLAGGED


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
def run(spark, run_date, silver_fuel_path, silver_vehicle_path,
        silver_date_path, gold_output_path):
    """
    Fuel efficiency audit: compare actual km/L vs baseline, flag outliers.
    Processes only today's fuel transactions.
    """
    print(f"[fuel_efficiency_audit] run_date={run_date}")

    # ── Step 1: Read Silver fact_fuel (only today's transactions) ──
    # Silver layer saves transactions with txn_date = run_date - 1
    # So we must audit target_date = run_date - 1
    from datetime import timedelta as td
    target_date = str(date.fromisoformat(run_date) - td(days=1))
    print(f"[fuel_efficiency_audit] Target audit date: {target_date}")

    try:
        fuel_df = (
            spark.read.format("delta").load(silver_fuel_path)
            .filter(col("txn_date") == lit(target_date))
        )
    except Exception as e:
        print(f"[fuel_efficiency_audit] ⚠ Could not read fact_fuel: {e}")
        return

    fuel_count = fuel_df.count()
    print(f"[fuel_efficiency_audit] Fuel transactions for {run_date}: {fuel_count}")
    if fuel_count == 0:
        print("[fuel_efficiency_audit] ⚠ No fuel data for today. Skipping.")
        return

    # ── Step 2: Exclude maintenance days ──
    fuel_df = fuel_df.filter(col("is_maintenance_day") == False)  # noqa: E712
    after_maint = fuel_df.count()
    excluded_maint = fuel_count - after_maint
    if excluded_maint > 0:
        print(f"[fuel_efficiency_audit] Excluded {excluded_maint} maintenance day transactions")

    # ── Step 3: Exclude weekends (JOIN dim_date) ──
    try:
        date_df = (
            spark.read.format("delta").load(silver_date_path)
            .select("date_id", "is_weekend")
        )
        fuel_df = (
            fuel_df.join(date_df, on="date_id", how="left")
            .filter(col("is_weekend") == False)  # noqa: E712
            .drop("is_weekend")
        )
        after_weekend = fuel_df.count()
        excluded_weekend = after_maint - after_weekend
        if excluded_weekend > 0:
            print(f"[fuel_efficiency_audit] Excluded {excluded_weekend} weekend transactions")
    except Exception as e:
        print(f"[fuel_efficiency_audit] ⚠ Could not read dim_date: {e}")
        print("[fuel_efficiency_audit] Proceeding without weekend exclusion")

    if fuel_df.count() == 0:
        print("[fuel_efficiency_audit] ⚠ All transactions excluded. Skipping.")
        return

    # ── Step 4: Calculate distance per VIN ──
    # We need ALL historical fuel data for this VIN to compute distance
    # (previous odometer may be from a prior day)
    all_fuel = (
        spark.read.format("delta").load(silver_fuel_path)
        .select("vin", "transaction_timestamp", "odometer_reading_km", "fuel_liters", "txn_date")
    )

    # Window: per VIN, ordered by transaction timestamp
    vin_window = Window.partitionBy("vin").orderBy("transaction_timestamp")
    all_fuel = all_fuel.withColumn(
        "prev_odometer", lag("odometer_reading_km").over(vin_window)
    )
    all_fuel = all_fuel.withColumn(
        "distance_km",
        col("odometer_reading_km") - col("prev_odometer")
    )

    # Filter to only the target day's transactions (after distance calculation)
    today_fuel = all_fuel.filter(col("txn_date") == lit(target_date))

    # Join back with fuel_df to retain is_maintenance_day filter
    # Use transaction_timestamp as the join key from the filtered set
    today_with_distance = (
        fuel_df.alias("f")
        .join(
            today_fuel.select(
                col("vin").alias("d_vin"),
                col("transaction_timestamp").alias("d_ts"),
                "distance_km",
            ).alias("d"),
            (col("f.vin") == col("d.d_vin"))
            & (col("f.transaction_timestamp") == col("d.d_ts")),
            how="inner"
        )
        .drop("d_vin", "d_ts")
    )

    # Drop rows with no previous odometer (first txn for VIN)
    before_dist = today_with_distance.count()
    today_with_distance = today_with_distance.filter(
        col("distance_km").isNotNull() & (col("distance_km") > 0)
    )
    dropped_dist = before_dist - today_with_distance.count()
    if dropped_dist > 0:
        print(f"[fuel_efficiency_audit] Excluded {dropped_dist} txns (no prev odometer or zero/neg distance)")

    if today_with_distance.count() == 0:
        print("[fuel_efficiency_audit] ⚠ No valid distance data. Skipping.")
        return

    # ── Step 5: Aggregate Daily Distance and Calculate km_per_liter ──
    from pyspark.sql.functions import sum as _sum

    # A vehicle might refuel multiple times a day. We must aggregate by VIN and date
    # so we don't violate Postgres PRIMARY KEY (vin, audit_date) unique constraints.
    today_with_distance = today_with_distance.groupBy("vin", "txn_date").agg(
        _sum("distance_km").alias("daily_distance_km"),
        _sum("fuel_liters").alias("daily_fuel_liters")
    )

    today_with_distance = today_with_distance.withColumn(
        "km_per_liter",
        (col("daily_distance_km") / col("daily_fuel_liters")).cast(FloatType())
    )

    # ── Step 6: JOIN dim_vehicle for baseline + model ──
    try:
        vehicle_df = (
            spark.read.format("delta").load(silver_vehicle_path)
            .filter(col("is_active") == True)  # noqa: E712
            .select(
                col("vin").alias("v_vin"),
                col("model"),
                col("baseline_efficiency").alias("baseline_kmpl"),
            )
        )
    except Exception as e:
        print(f"[fuel_efficiency_audit] ⚠ Could not read dim_vehicle: {e}")
        return

    audit_df = (
        today_with_distance.join(
            vehicle_df,
            col("vin") == col("v_vin"),
            how="inner"
        )
        .drop("v_vin")
    )

    # ── Step 7: Calculate variance_pct ──
    # BRD: "12% over baseline" = vehicle doing WORSE than baseline.
    # variance_pct = ((baseline - actual) / baseline) * 100
    # Positive variance_pct means doing worse than baseline.
    audit_df = audit_df.withColumn(
        "variance_pct",
        (((col("baseline_kmpl") - col("km_per_liter")) / col("baseline_kmpl")) * 100)
        .cast(FloatType())
    )

    # ── Step 8: Determine status ──
    # FLAGGED if variance > 12% (vehicle performing worse than threshold)
    audit_df = audit_df.withColumn(
        "status",
        when(col("variance_pct") > VARIANCE_THRESHOLD_PCT, lit("FLAGGED"))
        .otherwise(lit("OK"))
    )

    # ── Build final Gold schema ──
    audit_df = audit_df.select(
        col("vin"),
        col("model"),
        col("txn_date").alias("audit_date"),
        col("km_per_liter"),
        col("baseline_kmpl"),
        col("variance_pct"),
        col("status"),
        current_timestamp().alias("created_at"),
    )

    result_count = audit_df.count()
    flagged = audit_df.filter(col("status") == "FLAGGED").count()
    ok = audit_df.filter(col("status") == "OK").count()
    print(f"[fuel_efficiency_audit] Audit results: {result_count} total | FLAGGED: {flagged} | OK: {ok}")

    # ── Write to Gold Delta table ──
    if gold_table_exists(spark, gold_output_path):
        print("[fuel_efficiency_audit] Gold exists → Delta MERGE")
        gold_table = DeltaTable.forPath(spark, gold_output_path)
        (
            gold_table.alias("existing")
            .merge(
                audit_df.alias("incoming"),
                "existing.vin = incoming.vin AND existing.audit_date = incoming.audit_date"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        total = spark.read.format("delta").load(gold_output_path).count()
        print(f"[fuel_efficiency_audit] ✓ MERGE complete. Total Gold rows: {total}")

        # VACUUM
        DeltaTable.forPath(spark, gold_output_path).vacuum()
        print("[fuel_efficiency_audit] ✓ VACUUM complete.")
    else:
        print("[fuel_efficiency_audit] Gold does NOT exist → Bootstrap")
        (
            audit_df.write.format("delta")
            .mode("overwrite").option("overwriteSchema", "true")
            .save(gold_output_path)
        )
        print(f"[fuel_efficiency_audit] ✓ Bootstrap: {result_count} rows")


# ──────────────────────────────────────────────────────────────
# Glue Job Entry Point
# ──────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "run_date",
    "silver_fuel_path",
    "silver_vehicle_path",
    "silver_date_path",
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
print(f"  Fuel Efficiency Audit — Gold Layer Glue Job")
print(f"  Run Date       : {run_date}")
print(f"  Silver Fuel    : {args['silver_fuel_path']}")
print(f"  Silver Vehicle : {args['silver_vehicle_path']}")
print(f"  Silver Date    : {args['silver_date_path']}")
print(f"  Gold Output    : {args['gold_output_path']}")
print("=" * 60)

try:
    run(spark, run_date,
        args["silver_fuel_path"].rstrip("/"),
        args["silver_vehicle_path"].rstrip("/"),
        args["silver_date_path"].rstrip("/"),
        args["gold_output_path"].rstrip("/"))
    print("✓ Fuel Efficiency Audit completed successfully.")
except Exception as e:
    print(f"✗ Fuel Efficiency Audit failed: {e}")
    raise
finally:
    job.commit()
