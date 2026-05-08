"""
OmniRoute — Midnight Pipeline DAG (Unified Daily + Monthly)
=============================================================
Schedule: Daily @ 00:00 UTC

Handles:
  - Yearly: Maintenance schedules + dim_date (with Variable-based retry)
  - Daily: Bronze (registry + assignment) → Safety Snapshot → Silver → Gold
  - Monthly: Rate deduction report + safety strikes reset (with Variable-based month gating)

DAG Dependency Graph:
═══════════════════════
  start
    │
    ▼
  check_yearly_needed ─────────────────────────────────┐
    │                                                  │
  [yearly needed]                              [skip yearly]
    │                                                  │
    ▼                                                  │
  yearly_bronze_ingest                                 │
    │                                                  │
    ├──► silver_maintenance                            │
    ├──► silver_dim_date                               │
    │                                                  │
    ▼                                                  │
  mark_yearly_done                                     │
    │                                                  │
    └───────────────────────────► yearly_gate ◄────────┘
                                    │
                                    ▼
                          daily_bronze_midnight
                                    │
                                    ▼
                          daily_safety_snapshot
                                    │
                        ┌───────────┴───────────────┐
                        │                           │
                        ▼                           ▼
              silver_vehicle_registry    check_monthly_needed ────────┐
                        │                       │                    │
                        ▼                 [monthly needed]     [skip monthly]
              silver_vehicle_assignment         │                    │
                        │                       ▼                    │
                        ▼             monthly_rate_deduction         │
              gold_active_fleet_snapshot        │                    │
                        │                       ▼                    │
                        │             safety_strikes_reset           │
                        │                       │                    │
                        │                       ▼                    │
                        │             mark_monthly_done              │
                        │                       │                    │
                        │                       └──► monthly_gate ◄──┘
                        │                               │
                        └──────────► end ◄──────────────┘

Yearly Retry Logic:
  Uses Airflow Variable `omniroute_yearly_done_YYYY`.
  If yearly tasks fail on Jan 1st, they retry daily until success.
  Once successful, the Variable is set to 'true' and yearly tasks
  are skipped for the rest of the year.

Monthly Retry Logic:
  Uses Airflow Variable `omniroute_monthly_done_YYYY_MM`.
  On the first run of a new month, the Variable is absent → monthly
  jobs run. Once successful, the Variable is set to 'true' and
  monthly tasks are skipped for the rest of that month.
"""

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
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
SILVER_MAINTENANCE_PATH = config["silver"]["tables"]["dim_maintenance"]
SILVER_DATE_PATH = config["silver"]["tables"]["dim_date"]

# ── Gold S3 paths ──
GOLD_FLEET_SNAPSHOT_PATH = config["gold"]["tables"]["active_fleet_snapshot"]
GOLD_DAILY_SAFETY_SNAPSHOT_PATH = config["gold"]["tables"]["daily_safety_snapshot"]
GOLD_MONTHLY_RATE_DEDUCTION_PATH = config["gold"]["tables"]["monthly_rate_deduction"]

# ── Glue job configs — Daily ──
GLUE_BRONZE = config["glue"]["jobs"]["bronze_ingest"]
GLUE_SILVER_REGISTRY = config["glue"]["jobs"]["silver_vehicle_registry"]
GLUE_SILVER_ASSIGNMENT = config["glue"]["jobs"]["silver_vehicle_assignment"]
GLUE_SILVER_MAINTENANCE = config["glue"]["jobs"]["silver_maintenance_schedules"]
GLUE_GOLD_FLEET_SNAPSHOT = config["glue"]["jobs"]["gold_active_fleet_snapshot"]
GLUE_DAILY_SAFETY_SNAPSHOT = config["glue"]["jobs"]["daily_safety_snapshot"]

# ── Glue job configs — Yearly ──
GLUE_YEARLY_BRONZE = config["glue_yearly"]["jobs"]["bronze_ingest"]
GLUE_SILVER_DIM_DATE = config["glue_yearly"]["jobs"]["dim_date"]

# ── Glue job configs — Monthly ──
GLUE_RATE_DEDUCTION = config["glue_monthly"]["jobs"]["monthly_rate_deduction_report"]
GLUE_SAFETY_RESET = config["glue_monthly"]["jobs"]["safety_strikes_reset"]

RUN_DATE = "{{ ds }}"
RUN_MONTH = "{{ logical_date.strftime('%Y-%m') }}"
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
    Uses Airflow Variable `omniroute_yearly_done_YYYY`.
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
# Monthly Check Functions
# ──────────────────────────────────────────────────────────────
def check_monthly_needed(**context):
    """
    Decide whether monthly tasks (rate deduction + strikes reset) need to run.
    Uses Airflow Variable `omniroute_monthly_done_YYYY_MM`.
    On the first run of a new month the variable is absent → run monthly jobs.
    Once successful the variable is set to 'true' → skip for the rest of the month.
    """
    execution_date = context["logical_date"]
    month_key = execution_date.strftime("%Y_%m")
    var_name = f"omniroute_monthly_done_{month_key}"

    monthly_done = Variable.get(var_name, default_var="false")
    print(f"[monthly_check] Variable '{var_name}' = '{monthly_done}'")

    if monthly_done == "true":
        print(f"[monthly_check] Monthly tasks already completed for {month_key}. Skipping.")
        return "skip_monthly"
    else:
        print(f"[monthly_check] Monthly tasks NOT yet done for {month_key}. Running.")
        return "monthly_rate_deduction_report"


def mark_monthly_done(**context):
    """Set the Airflow Variable to mark monthly tasks as completed for this month."""
    execution_date = context["logical_date"]
    month_key = execution_date.strftime("%Y_%m")
    var_name = f"omniroute_monthly_done_{month_key}"
    Variable.set(var_name, "true")
    print(f"[monthly_check] ✓ Set '{var_name}' = 'true'. Monthly tasks won't run again in {month_key}.")


# ──────────────────────────────────────────────────────────────
# DAG Definition
# ──────────────────────────────────────────────────────────────
default_args = {
    "owner": "omniroute",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="omniroute_midnight_pipeline",
    description=(
        "Unified midnight pipeline: Yearly (maintenance/dim_date) → "
        "Daily Bronze (registry+assignment) → Safety Snapshot → Silver → "
        "Gold fleet snapshot + Monthly (rate deduction/strikes reset) with "
        "Variable-based gating."
    ),
    schedule="0 0 * * *",              # Daily at 00:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bronze", "silver", "gold", "glue", "delta", "daily", "yearly", "monthly", "midnight"],
    default_args=default_args,
    max_active_runs=1,
) as dag:

    # ── Markers ──
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",
    )

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

    silver_maintenance = build_glue_task(
        task_id="silver_maintenance_schedules",
        glue_config=GLUE_SILVER_MAINTENANCE,
        script_args={
            "--run_date": RUN_DATE,
            "--bronze_ingested_path": BRONZE_INGESTED_PATH,
            "--silver_output_path": SILVER_MAINTENANCE_PATH,
        },
    )

    silver_dim_date = build_glue_task(
        task_id="silver_dim_date",
        glue_config=GLUE_SILVER_DIM_DATE,
        script_args={
            "--year": "{{ logical_date.year }}",
            "--silver_output_path": SILVER_DATE_PATH,
        },
    )

    mark_done = PythonOperator(
        task_id="mark_yearly_done",
        python_callable=mark_yearly_done,
    )

    yearly_gate = EmptyOperator(
        task_id="yearly_gate",
        trigger_rule="none_failed_min_one_success",
    )

    # ══════════════════════════════════════════════════════════
    # DAILY — Bronze (registry + assignment) → Safety Snapshot
    # ══════════════════════════════════════════════════════════
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

    daily_safety_snapshot = build_glue_task(
        task_id="daily_safety_snapshot",
        glue_config=GLUE_DAILY_SAFETY_SNAPSHOT,
        script_args={
            "--run_date":       RUN_DATE,
            "--silver_assignment_path": SILVER_VEHICLE_ASSIGNMENT_PATH,
            "--gold_output_path": GOLD_DAILY_SAFETY_SNAPSHOT_PATH,
            "--pg_host":     "{{ var.value.pg_host }}",
            "--pg_port":     "{{ var.value.get('pg_port', '5432') }}",
            "--pg_database": "{{ var.value.pg_database }}",
            "--pg_user":     "{{ var.value.pg_user }}",
            "--pg_password": "{{ var.value.pg_password }}",
        },
    )

    # ══════════════════════════════════════════════════════════
    # BRANCH 1 — Silver vehicle registry → assignment → Gold
    # ══════════════════════════════════════════════════════════
    silver_registry = build_glue_task(
        task_id="silver_vehicle_registry",
        glue_config=GLUE_SILVER_REGISTRY,
        script_args={
            "--run_date": RUN_DATE,
            "--bronze_ingested_path": BRONZE_INGESTED_PATH,
            "--silver_output_path": SILVER_VEHICLE_REGISTRY_PATH,
        },
    )

    silver_assignment = build_glue_task(
        task_id="silver_vehicle_assignment",
        glue_config=GLUE_SILVER_ASSIGNMENT,
        script_args={
            "--run_date": RUN_DATE,
            "--bronze_ingested_path": BRONZE_INGESTED_PATH,
            "--silver_output_path": SILVER_VEHICLE_ASSIGNMENT_PATH,
            "--silver_vehicle_path": SILVER_VEHICLE_REGISTRY_PATH,
            "--silver_maintenance_path": SILVER_MAINTENANCE_PATH,
            "--pg_host": "{{ var.value.pg_host }}",
            "--pg_port": "{{ var.value.get('pg_port', '5432') }}",
            "--pg_database": "{{ var.value.pg_database }}",
            "--pg_user": "{{ var.value.pg_user }}",
            "--pg_password": "{{ var.value.pg_password }}",
        },
    )

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
    # BRANCH 2 — Monthly: rate deduction → strikes reset
    # ══════════════════════════════════════════════════════════
    check_monthly = BranchPythonOperator(
        task_id="check_monthly_needed",
        python_callable=check_monthly_needed,
    )

    skip_monthly = EmptyOperator(task_id="skip_monthly")

    monthly_rate_deduction = build_glue_task(
        task_id="monthly_rate_deduction_report",
        glue_config=GLUE_RATE_DEDUCTION,
        script_args={
            "--run_month":          RUN_MONTH,
            "--run_date":           RUN_DATE,
            "--gold_snapshot_path": GOLD_DAILY_SAFETY_SNAPSHOT_PATH,
            "--gold_output_path":   GOLD_MONTHLY_RATE_DEDUCTION_PATH,
        },
    )

    safety_strikes_reset = build_glue_task(
        task_id="safety_strikes_reset",
        glue_config=GLUE_SAFETY_RESET,
        script_args={
            "--run_month":              RUN_MONTH,
            "--run_date":               RUN_DATE,
            # PostgreSQL connection (injected via Airflow Variables)
            "--pg_host":     "{{ var.value.pg_host }}",
            "--pg_port":     "{{ var.value.get('pg_port', '5432') }}",
            "--pg_database": "{{ var.value.pg_database }}",
            "--pg_user":     "{{ var.value.pg_user }}",
            "--pg_password": "{{ var.value.pg_password }}",
        },
    )

    mark_monthly = PythonOperator(
        task_id="mark_monthly_done",
        python_callable=mark_monthly_done,
    )

    monthly_gate = EmptyOperator(
        task_id="monthly_gate",
        trigger_rule="none_failed_min_one_success",
    )

    # ══════════════════════════════════════════════════════════
    # TASK DEPENDENCIES
    # ══════════════════════════════════════════════════════════

    # ── Yearly branch ──
    start >> check_yearly
    check_yearly >> yearly_bronze >> [silver_maintenance, silver_dim_date] >> mark_done >> yearly_gate
    check_yearly >> skip_yearly >> yearly_gate

    # ── Daily path: yearly gate must pass before daily work begins ──
    yearly_gate >> daily_bronze_midnight >> daily_safety_snapshot

    # ── Branch 1: Silver → Gold fleet snapshot ──
    daily_safety_snapshot >> silver_registry >> silver_assignment >> gold_fleet_snapshot

    # ── Branch 2: Monthly (Variable-gated) ──
    daily_safety_snapshot >> check_monthly
    check_monthly >> monthly_rate_deduction >> safety_strikes_reset >> mark_monthly >> monthly_gate
    check_monthly >> skip_monthly >> monthly_gate

    # ── End waits for both branches ──
    [gold_fleet_snapshot, monthly_gate] >> end
