"""
AWS Glue Job — Monthly Safety Strikes Reset
=============================================
Resets strike_count and current_adjusted_rate for all ACTIVE drivers
in the report.driver_safety_status PostgreSQL table at the start of
each month. SUSPENDED drivers are left untouched.

Runs on the 1st of every month as part of the omniroute_monthly_pipeline
DAG (after the monthly_rate_deduction_report job).

Logic:
  - ACTIVE drivers  → strike_count = 0, current_adjusted_rate = base_rate,
                       month = new month
  - SUSPENDED drivers → no changes

Glue Job Parameters:
  --run_month    : Month being processed (YYYY-MM, e.g. 2026-05)
  --run_date     : Logical date (YYYY-MM-DD)
  --pg_host      : PostgreSQL host
  --pg_port      : PostgreSQL port (default 5432)
  --pg_database  : PostgreSQL database name
  --pg_user      : PostgreSQL username
  --pg_password  : PostgreSQL password

Idempotency:
  Running this job multiple times in the same month is safe — it sets
  strike_count to 0 and current_adjusted_rate to base_rate for ACTIVE
  drivers, which is the same result regardless of how many times it runs.
"""

import sys
from datetime import date

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import SparkConf
from pyspark.context import SparkContext


# ──────────────────────────────────────────────────────────────
# JDBC Helpers
# ──────────────────────────────────────────────────────────────
def execute_postgres_query(spark, jdbc_url, query, user, password):
    """Execute a raw SQL query against PostgreSQL using Py4J."""
    try:
        sc = spark.sparkContext
        driver_manager = sc._gateway.jvm.java.sql.DriverManager

        props = sc._gateway.jvm.java.util.Properties()
        props.setProperty("user", user)
        props.setProperty("password", password)
        props.setProperty("sslmode", "require")

        connection = driver_manager.getConnection(jdbc_url, props)
        statement = connection.createStatement()
        result = statement.executeUpdate(query)
        connection.close()
        print(f"  ✓ Executed: {query}")
        return result
    except Exception as e:
        print(f"  ✗ Query Failed: {e}")
        raise


def read_postgres_count(spark, jdbc_url, query, user, password):
    """Execute a SELECT query and return the scalar integer result."""
    try:
        sc = spark.sparkContext
        driver_manager = sc._gateway.jvm.java.sql.DriverManager

        props = sc._gateway.jvm.java.util.Properties()
        props.setProperty("user", user)
        props.setProperty("password", password)
        props.setProperty("sslmode", "require")

        connection = driver_manager.getConnection(jdbc_url, props)
        statement = connection.createStatement()
        rs = statement.executeQuery(query)
        rs.next()
        count = rs.getInt(1)
        connection.close()
        return count
    except Exception as e:
        print(f"  ⚠ Count query failed: {e}")
        return -1


# ──────────────────────────────────────────────────────────────
# Core Logic
# ──────────────────────────────────────────────────────────────
def run(spark, run_month, jdbc_url, pg_user, pg_password):
    """Reset strike_count for ACTIVE drivers, leave SUSPENDED untouched."""

    # ── Pre-reset counts ──
    print("\n[1/3] Fetching pre-reset driver counts...")

    active_count = read_postgres_count(
        spark, jdbc_url,
        "SELECT COUNT(*) FROM report.driver_safety_status WHERE status = 'ACTIVE'",
        pg_user, pg_password,
    )
    suspended_count = read_postgres_count(
        spark, jdbc_url,
        "SELECT COUNT(*) FROM report.driver_safety_status WHERE status = 'SUSPENDED'",
        pg_user, pg_password,
    )
    total_count = read_postgres_count(
        spark, jdbc_url,
        "SELECT COUNT(*) FROM report.driver_safety_status",
        pg_user, pg_password,
    )

    print(f"  ACTIVE drivers    : {active_count}")
    print(f"  SUSPENDED drivers : {suspended_count}")
    print(f"  Total drivers     : {total_count}")

    # ── Reset ACTIVE drivers ──
    print(f"\n[2/3] Resetting strikes for ACTIVE drivers (month → {run_month}-01)...")

    reset_sql = f"""
        UPDATE report.driver_safety_status
        SET    strike_count          = 0,
               current_adjusted_rate = base_rate,
               month                 = '{run_month}-01'
        WHERE  status = 'ACTIVE'
    """

    rows_updated = execute_postgres_query(
        spark, jdbc_url, reset_sql, pg_user, pg_password,
    )
    print(f"  ✓ {rows_updated} ACTIVE driver(s) reset")

    # ── Post-reset verification ──
    print("\n[3/3] Post-reset verification...")

    non_zero_active = read_postgres_count(
        spark, jdbc_url,
        "SELECT COUNT(*) FROM report.driver_safety_status "
        "WHERE status = 'ACTIVE' AND strike_count != 0",
        pg_user, pg_password,
    )
    if non_zero_active == 0:
        print("  ✓ Verification passed: all ACTIVE drivers have strike_count = 0")
    else:
        print(f"  ⚠ Verification WARNING: {non_zero_active} ACTIVE driver(s) still have non-zero strikes")

    suspended_after = read_postgres_count(
        spark, jdbc_url,
        "SELECT COUNT(*) FROM report.driver_safety_status WHERE status = 'SUSPENDED'",
        pg_user, pg_password,
    )
    print(f"  SUSPENDED drivers (unchanged): {suspended_after}")


# ──────────────────────────────────────────────────────────────
# Glue Job Entry Point
# ──────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "run_month",
    "run_date",
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

run_month = args["run_month"]       # e.g. "2026-05"
run_date = args["run_date"]         # e.g. "2026-05-01"

# ── Build JDBC URL ──
jdbc_url = f"jdbc:postgresql://{args['pg_host']}:{args['pg_port']}/{args['pg_database']}"

print("=" * 60)
print("  Monthly Safety Strikes Reset — Glue Job")
print(f"  Run Month   : {run_month}")
print(f"  Run Date    : {run_date}")
print(f"  JDBC URL    : {jdbc_url}")
print(f"  PG User     : {args['pg_user']}")
print("=" * 60)

try:
    run(spark, run_month, jdbc_url, args["pg_user"], args["pg_password"])
    print("\n✓ Monthly safety strikes reset completed successfully.")
except Exception as e:
    print(f"\n✗ Monthly safety strikes reset failed: {e}")
    raise
finally:
    job.commit()
