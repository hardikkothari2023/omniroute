"""
OmniRoute — Unified Pipeline DAG (Daily + Yearly)
===================================================
Single DAG handling BOTH daily and yearly pipelines.

Schedule: Daily @ 00:00 UTC

Execution Windows:
  - Midnight (00:00 UTC): Bronze (registry + assignment), Silver registry/assignment,
    Gold fleet snapshot, + yearly tasks (maintenance + dim_date) when needed.
  - Morning  (07:00 UTC): Bronze (fuel), Silver fuel, Gold fuel audit,
    Gold to PostgreSQL.

Yearly Retry Logic:
  Uses Airflow Variable `omniroute_yearly_done_YYYY` to track yearly task completion.
  On Jan 1st, yearly tasks run. If they fail, they retry each subsequent day
  until success. Once successful, the Variable is set to 'true' and yearly tasks
  are skipped for the rest of the year.

DAG Dependency Graph:
═══════════════════════
  start
    │
    ├── check_yearly_needed ─────────────────────────────────┐
    │     │                                                  │
    │   [yearly needed]                              [skip yearly]
    │     │                                                  │
    │     ▼                                                  │
    │   yearly_bronze_ingest                                 │
    │     │                                                  │
    │     ├──► silver_maintenance                            │
    │     ├──► silver_dim_date                               │
    │     │                                                  │
    │     ▼                                                  │
    │   mark_yearly_done                                     │
    │     │                                                  │
    │     └───────────────────────────► yearly_gate ◄────────┘
    │
    ├── daily_bronze_midnight (registry + assignment)
    │     │
    │     ▼
    │   silver_vehicle_registry
    │     │
    │     ▼
    │   silver_vehicle_assignment
    │     │
    │     ▼
    │   gold_active_fleet_snapshot
    │
    ▼
  join_midnight ◄── (gold_fleet_snapshot + yearly_gate)
    │
    ▼
  wait_for_fuel_data (TimeSensor → 07:00 UTC)
    │
    ▼
  daily_bronze_fuel (fuel_transactions only)
    │
    ▼
  silver_fuel_transactions
    │
    ▼
  gold_fuel_efficiency_audit
    │
    ▼
  gold_to_postgres
    │
    ▼
  end

Per BRD:
  - Vehicle Registry:   daily (available at ~00:00 UTC)
  - Vehicle Assignment: daily incremental (available at ~00:00 UTC)
  - Fuel Transactions:  daily (available at 07:00 UTC)
  - Maintenance Logs:   yearly on Jan 1st
  - dim_date:           yearly on Jan 1st
"""

import json
import os
import sys
from datetime import datetime, timedelta, time
from pathlib import Path

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.standard.sensors.time import TimeSensor
from airflow.models import Variable


# ──────────────────────────────────────────────────────────────
# Load Configuration from s3_paths.json
# ──────────────────────────────────────────────────────────────
CONFIG_PATH = Path(os.path.dirname(os.path.abspath(__file__))).parent / "s3_paths.json"

with open(CONFIG_PATH, "r") as f:
    config = json.load(f)

# ── Bronze S3 paths ──
BRONZE_LANDING_PATH = config["bronze"]["landing"]
BRONZE_INGESTED_PATH = config["bronze"]["ingested"]
BRONZE_QUARANTINE_PATH = config["bronze"]["quarantine"]
BRONZE_ARCHIVE_PATH = config["bronze"]["archive"]

# ── Silver S3 paths ──
SILVER_VEHICLE_REGISTRY_PATH = config["silver"]["tables"]["vehicle_registry"]
SILVER_VEHICLE_ASSIGNMENT_PATH = config["silver"]["tables"]["vehicle_assignment"]
SILVER_FUEL_TRANSACTIONS_PATH = config["silver"]["tables"]["fuel_transactions"]
SILVER_MAINTENANCE_PATH = config["silver"]["tables"]["dim_maintenance"]
SILVER_DATE_PATH = config["silver"]["tables"]["dim_date"]

# ── Gold S3 paths ──
GOLD_FUEL_AUDIT_PATH = config["gold"]["tables"]["fuel_efficiency_audit"]
GOLD_FLEET_SNAPSHOT_PATH = config["gold"]["tables"]["active_fleet_snapshot"]

# ── Glue job configs (daily) ──
GLUE_BRONZE = config["glue"]["jobs"]["bronze_ingest"]
GLUE_SILVER_REGISTRY = config["glue"]["jobs"]["silver_vehicle_registry"]
GLUE_SILVER_ASSIGNMENT = config["glue"]["jobs"]["silver_vehicle_assignment"]
GLUE_SILVER_FUEL = config["glue"]["jobs"]["silver_fuel_transactions"]
GLUE_SILVER_MAINTENANCE = config["glue"]["jobs"]["silver_maintenance_schedules"]
GLUE_GOLD_FUEL_AUDIT = config["glue"]["jobs"]["gold_fuel_efficiency_audit"]
GLUE_GOLD_FLEET_SNAPSHOT = config["glue"]["jobs"]["gold_active_fleet_snapshot"]
GLUE_GOLD_TO_POSTGRES = config["glue"]["jobs"]["gold_to_postgres"]

# ── Glue job configs (yearly) ──
GLUE_YEARLY_BRONZE = config["glue_yearly"]["jobs"]["bronze_ingest"]
GLUE_SILVER_DIM_DATE = config["glue_yearly"]["jobs"]["dim_date"]

RUN_DATE = "{{ ds }}"
AWS_CONN_ID = "aws_default"


# ──────────────────────────────────────────────────────────────
# Helper — Build GlueJobOperator with Delta Lake enabled
# ──────────────────────────────────────────────────────────────
def build_glue_task(task_id, glue_config, script_args):
    """Build a GlueJobOperator with Delta Lake enabled."""
    return GlueJobOperator(
        task_id=task_id,
        job_name=glue_config["job_name"],
        iam_role_name=glue_config["iam_role_name"],
        region_name="us-east-1",
        wait_for_completion=True,
        verbose=True,
        script_args=script_args,
        create_job_kwargs={
            "GlueVersion": glue_config["glue_version"],
            "NumberOfWorkers": glue_config["number_of_workers"],
            "WorkerType": glue_config["worker_type"],
            "Timeout": glue_config["timeout_minutes"],
            "MaxRetries": glue_config.get("max_retries", 1),
            "MaxCapacity": None,
            "Command": {
                "Name": "glueetl",
                "ScriptLocation": glue_config["script_location"],
                "PythonVersion": "3",
            },
            "DefaultArguments": {
                "--datalake-formats": "delta",
                "--job-bookmark-option": "job-bookmark-disable",
            },
        },
    )


# ──────────────────────────────────────────────────────────────
# Yearly Check Functions
# ──────────────────────────────────────────────────────────────
def check_yearly_needed(**context):
    """
    Decide whether yearly tasks (maintenance + dim_date) need to run.

    Uses Airflow Variable `omniroute_yearly_done_YYYY`:
      - If 'true' → skip (already completed this year)
      - If 'false' or missing → run yearly tasks

    This enables retry-until-success: if yearly fails on Jan 1st,
    it retries on Jan 2nd, 3rd, etc. until it succeeds.
    """
    execution_date = context["logical_date"]
    year = execution_date.year
    var_name = f"omniroute_yearly_done_{year}"

    yearly_done = Variable.get(var_name, default_var="false")
    print(f"[yearly_check] Variable '{var_name}' = '{yearly_done}'")

    if yearly_done == "true":
        print(f"[yearly_check] Yearly tasks already completed for {year}. Skipping.")
        return "skip_yearly"
    else:
        print(f"[yearly_check] Yearly tasks NOT yet done for {year}. Running.")
        return "yearly_bronze_ingest"


def mark_yearly_done(**context):
    """Set the Airflow Variable to mark yearly tasks as completed."""
    execution_date = context["logical_date"]
    year = execution_date.year
    var_name = f"omniroute_yearly_done_{year}"
    Variable.set(var_name, "true")
    print(f"[yearly_check] ✓ Set '{var_name}' = 'true'. Yearly tasks won't run again in {year}.")


# ──────────────────────────────────────────────────────────────
# DAG Default Arguments
# ──────────────────────────────────────────────────────────────
default_args = {
    "owner": "omniroute",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


# ──────────────────────────────────────────────────────────────
# DAG Definition
# ──────────────────────────────────────────────────────────────
with DAG(
    dag_id="omniroute_unified_pipeline",
    description=(
        "Unified pipeline: Daily Bronze/Silver/Gold + Yearly maintenance/dim_date. "
        "Midnight window (00:00 UTC) for registry+assignment, "
        "Morning window (07:00 UTC) for fuel+gold analytics+reporting."
    ),
    schedule="0 0 * * *",              # Daily at 00:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bronze", "silver", "gold", "glue", "delta", "daily", "yearly", "unified"],
    default_args=default_args,
    max_active_runs=1,
) as dag:

    # ── Markers ──
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # ══════════════════════════════════════════════════════════
    # YEARLY BRANCH — maintenance schedules + dim_date
    # ══════════════════════════════════════════════════════════
    check_yearly = BranchPythonOperator(
        task_id="check_yearly_needed",
        python_callable=check_yearly_needed,
    )

    skip_yearly = EmptyOperator(task_id="skip_yearly")

    # ── Yearly Bronze: Ingest maintenance_schedules.csv ──
    yearly_bronze = GlueJobOperator(
        task_id="yearly_bronze_ingest",
        job_name=GLUE_YEARLY_BRONZE["job_name"],
        iam_role_name=GLUE_YEARLY_BRONZE["iam_role_name"],
        script_location=GLUE_YEARLY_BRONZE["script_location"],
        region_name="us-east-1",
        create_job_kwargs={
            "GlueVersion": GLUE_YEARLY_BRONZE["glue_version"],
            "NumberOfWorkers": GLUE_YEARLY_BRONZE["number_of_workers"],
            "WorkerType": GLUE_YEARLY_BRONZE["worker_type"],
        },
        script_args={
            "--run_date": RUN_DATE,
            "--landing_path": BRONZE_LANDING_PATH,
            "--ingested_path": BRONZE_INGESTED_PATH,
            "--quarantine_path": BRONZE_QUARANTINE_PATH,
            "--archive_path": BRONZE_ARCHIVE_PATH,
        },
        wait_for_completion=True,
        verbose=True,
        aws_conn_id=AWS_CONN_ID,
    )

    # ── Silver: Maintenance Schedules → dim_maintenance ──
    silver_maintenance = build_glue_task(
        task_id="silver_maintenance_schedules",
        glue_config=GLUE_SILVER_MAINTENANCE,
        script_args={
            "--run_date": RUN_DATE,
            "--bronze_ingested_path": BRONZE_INGESTED_PATH,
            "--silver_output_path": SILVER_MAINTENANCE_PATH,
        },
    )

    # ── Silver: Generate dim_date for the year ──
    silver_dim_date = build_glue_task(
        task_id="silver_dim_date",
        glue_config=GLUE_SILVER_DIM_DATE,
        script_args={
            "--year": "{{ logical_date.year }}",
            "--silver_output_path": SILVER_DATE_PATH,
        },
    )

    # ── Mark yearly as done (only runs after maintenance + dim_date succeed) ──
    mark_done = PythonOperator(
        task_id="mark_yearly_done",
        python_callable=mark_yearly_done,
    )

    # ── Gate: joins yearly and skip_yearly branches ──
    yearly_gate = EmptyOperator(
        task_id="yearly_gate",
        trigger_rule="none_failed_min_one_success",
    )

    # ══════════════════════════════════════════════════════════
    # MIDNIGHT WINDOW — Bronze (registry + assignment) → Silver → Gold
    # ══════════════════════════════════════════════════════════

    # ── Daily Bronze: Ingest vehicle_registry.csv + vehicle_assignment.csv ──
    daily_bronze_midnight = GlueJobOperator(
        task_id="daily_bronze_midnight",
        job_name=GLUE_BRONZE["job_name"],
        iam_role_name=GLUE_BRONZE["iam_role_name"],
        script_location=GLUE_BRONZE["script_location"],
        region_name="us-east-1",
        create_job_kwargs={
            "GlueVersion": GLUE_BRONZE["glue_version"],
            "NumberOfWorkers": GLUE_BRONZE["number_of_workers"],
            "WorkerType": GLUE_BRONZE["worker_type"],
        },
        script_args={
            "--run_date": RUN_DATE,
            "--landing_path": BRONZE_LANDING_PATH,
            "--ingested_path": BRONZE_INGESTED_PATH,
            "--quarantine_path": BRONZE_QUARANTINE_PATH,
            "--archive_path": BRONZE_ARCHIVE_PATH,
            "--datasets": "vehicle_registry,vehicle_assignment",
        },
        wait_for_completion=True,
        verbose=True,
        aws_conn_id=AWS_CONN_ID,
        retries=2,
        retry_delay=timedelta(minutes=3),
    )

    # ── Silver: Vehicle Registry → dim_vehicle ──
    silver_registry = build_glue_task(
        task_id="silver_vehicle_registry",
        glue_config=GLUE_SILVER_REGISTRY,
        script_args={
            "--run_date": RUN_DATE,
            "--bronze_ingested_path": BRONZE_INGESTED_PATH,
            "--silver_output_path": SILVER_VEHICLE_REGISTRY_PATH,
        },
    )

    # ── Silver: Vehicle Assignment → dim_vehicle_assignment_scd2 ──
    silver_assignment = build_glue_task(
        task_id="silver_vehicle_assignment",
        glue_config=GLUE_SILVER_ASSIGNMENT,
        script_args={
            "--run_date": RUN_DATE,
            "--bronze_ingested_path": BRONZE_INGESTED_PATH,
            "--silver_output_path": SILVER_VEHICLE_ASSIGNMENT_PATH,
            "--silver_vehicle_path": SILVER_VEHICLE_REGISTRY_PATH,
        },
    )

    # ── Gold: Active Fleet Snapshot (BRD) ──
    # Daily count of IN-TRANSIT vehicles by model — doesn't need fuel data
    gold_fleet_snapshot = build_glue_task(
        task_id="gold_active_fleet_snapshot",
        glue_config=GLUE_GOLD_FLEET_SNAPSHOT,
        script_args={
            "--run_date": RUN_DATE,
            "--silver_assignment_path": SILVER_VEHICLE_ASSIGNMENT_PATH,
            "--silver_vehicle_path": SILVER_VEHICLE_REGISTRY_PATH,
            "--gold_output_path": GOLD_FLEET_SNAPSHOT_PATH,
        },
    )

    # ══════════════════════════════════════════════════════════
    # JOIN — Wait for midnight tasks + yearly gate before fuel
    # ══════════════════════════════════════════════════════════
    join_midnight = EmptyOperator(
        task_id="join_midnight",
        trigger_rule="none_failed_min_one_success",
    )

    # ══════════════════════════════════════════════════════════
    # MORNING WINDOW — Wait for 07:00 UTC, then process fuel
    # ══════════════════════════════════════════════════════════

    # ── Sensor: Wait until 07:00 UTC for fuel CSV to be available ──
    wait_for_fuel = TimeSensor(
        task_id="wait_for_fuel_data",
        target_time=time(7, 0),
        mode="reschedule",        # Release worker between checks
        poke_interval=300,        # Check every 5 minutes
        timeout=8 * 3600,         # Max wait: 8 hours (fail if past 08:00 UTC)
    )

    # ── Daily Bronze: Ingest fuel_transactions.csv only ──
    daily_bronze_fuel = GlueJobOperator(
        task_id="daily_bronze_fuel",
        job_name=GLUE_BRONZE["job_name"],
        iam_role_name=GLUE_BRONZE["iam_role_name"],
        script_location=GLUE_BRONZE["script_location"],
        region_name="us-east-1",
        create_job_kwargs={
            "GlueVersion": GLUE_BRONZE["glue_version"],
            "NumberOfWorkers": GLUE_BRONZE["number_of_workers"],
            "WorkerType": GLUE_BRONZE["worker_type"],
        },
        script_args={
            "--run_date": RUN_DATE,
            "--landing_path": BRONZE_LANDING_PATH,
            "--ingested_path": BRONZE_INGESTED_PATH,
            "--quarantine_path": BRONZE_QUARANTINE_PATH,
            "--archive_path": BRONZE_ARCHIVE_PATH,
            "--datasets": "fuel_transactions",
        },
        wait_for_completion=True,
        verbose=True,
        aws_conn_id=AWS_CONN_ID,
        retries=2,
        retry_delay=timedelta(minutes=3),
    )

    # ── Silver: Fuel Transactions → fact_fuel ──
    silver_fuel = build_glue_task(
        task_id="silver_fuel_transactions",
        glue_config=GLUE_SILVER_FUEL,
        script_args={
            "--run_date": RUN_DATE,
            "--bronze_ingested_path": BRONZE_INGESTED_PATH,
            "--silver_output_path": SILVER_FUEL_TRANSACTIONS_PATH,
            "--silver_maintenance_path": SILVER_MAINTENANCE_PATH,
            "--silver_assignment_path": SILVER_VEHICLE_ASSIGNMENT_PATH,
            "--silver_vehicle_path": SILVER_VEHICLE_REGISTRY_PATH,
        },
    )

    # ── Gold: Fuel Efficiency Audit (BRD) ──
    gold_fuel_audit = build_glue_task(
        task_id="gold_fuel_efficiency_audit",
        glue_config=GLUE_GOLD_FUEL_AUDIT,
        script_args={
            "--run_date": RUN_DATE,
            "--silver_fuel_path": SILVER_FUEL_TRANSACTIONS_PATH,
            "--silver_vehicle_path": SILVER_VEHICLE_REGISTRY_PATH,
            "--silver_date_path": SILVER_DATE_PATH,
            "--gold_output_path": GOLD_FUEL_AUDIT_PATH,
        },
    )

    # ── Reporting: Load Gold → PostgreSQL (BRD 5.2) ──
    gold_to_postgres = build_glue_task(
        task_id="gold_to_postgres",
        glue_config=GLUE_GOLD_TO_POSTGRES,
        script_args={
            "--run_date": RUN_DATE,
            "--silver_assignment_path": SILVER_VEHICLE_ASSIGNMENT_PATH,
            "--silver_vehicle_path": SILVER_VEHICLE_REGISTRY_PATH,
            "--silver_date_path": SILVER_DATE_PATH,
            "--gold_fuel_audit_path": GOLD_FUEL_AUDIT_PATH,
            "--gold_fleet_snapshot_path": GOLD_FLEET_SNAPSHOT_PATH,
            "--pg_host": "{{ var.value.pg_host }}",
            "--pg_port": "{{ var.value.get('pg_port', '5432') }}",
            "--pg_database": "{{ var.value.pg_database }}",
            "--pg_user": "{{ var.value.pg_user }}",
            "--pg_password": "{{ var.value.pg_password }}",
        },
    )

    # ══════════════════════════════════════════════════════════
    # TASK DEPENDENCIES
    # ══════════════════════════════════════════════════════════

    # ── Yearly branch ──
    start >> check_yearly
    check_yearly >> yearly_bronze >> [silver_maintenance, silver_dim_date] >> mark_done >> yearly_gate
    check_yearly >> skip_yearly >> yearly_gate

    # ── Daily midnight: Bronze → Silver → Gold fleet snapshot ──
    start >> daily_bronze_midnight >> silver_registry >> silver_assignment >> gold_fleet_snapshot

    # ── Join: wait for both midnight path and yearly gate ──
    [gold_fleet_snapshot, yearly_gate] >> join_midnight

    # ── Morning: wait for 07:00 UTC, then process fuel pipeline ──
    join_midnight >> wait_for_fuel >> daily_bronze_fuel >> silver_fuel
    silver_fuel >> gold_fuel_audit
    [gold_fuel_audit, gold_fleet_snapshot] >> gold_to_postgres >> end
