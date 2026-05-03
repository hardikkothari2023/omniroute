"""
OmniRoute — Morning Fuel Pipeline DAG (DAG 2 of 2)
=====================================================
Schedule: Daily @ 07:00 UTC

Runs ONLY after the midnight pipeline (DAG 1) completes successfully.
Uses ExternalTaskSensor to wait for omniroute_midnight_pipeline.end.

Handles:
  - Bronze ingestion (fuel_transactions only)
  - Silver fuel transactions
  - Gold fuel efficiency audit
  - Gold to PostgreSQL reporting

DAG Dependency Graph:
═══════════════════════
  wait_for_midnight_pipeline (ExternalTaskSensor)
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
"""

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor


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

# ── Glue job configs ──
GLUE_BRONZE = config["glue"]["jobs"]["bronze_ingest"]
GLUE_SILVER_FUEL = config["glue"]["jobs"]["silver_fuel_transactions"]
GLUE_GOLD_FUEL_AUDIT = config["glue"]["jobs"]["gold_fuel_efficiency_audit"]
GLUE_GOLD_TO_POSTGRES = config["glue"]["jobs"]["gold_to_postgres"]

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
    dag_id="omniroute_morning_fuel_pipeline",
    description=(
        "Morning pipeline: Waits for midnight pipeline success, then "
        "Bronze (fuel) → Silver fuel → Gold fuel audit → PostgreSQL."
    ),
    schedule="0 7 * * *",              # Daily at 07:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bronze", "silver", "gold", "glue", "delta", "daily", "fuel", "morning"],
    default_args=default_args,
    max_active_runs=1,
) as dag:

    # ══════════════════════════════════════════════════════════
    # GATE — Wait for midnight pipeline to complete
    # ══════════════════════════════════════════════════════════
    # ExternalTaskSensor waits for the "end" task of the midnight
    # pipeline to succeed. Both DAGs share the same data_interval
    # (same ds) since they run on the same calendar day.
    #
    # execution_date_fn maps this DAG's logical_date to the
    # midnight DAG's logical_date. Since both run daily:
    #   Morning DAG (07:00 UTC, May 3) → ds = 2026-05-03
    #   Midnight DAG (00:00 UTC, May 3) → ds = 2026-05-03
    # They share the same ds, so no date offset is needed.
    wait_for_midnight = ExternalTaskSensor(
        task_id="wait_for_midnight_pipeline",
        external_dag_id="omniroute_midnight_pipeline",
        external_task_id="end",
        mode="reschedule",            # Release worker between checks
        poke_interval=120,            # Check every 2 minutes
        timeout=3600,                 # Fail after 1 hour of waiting
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],
    )

    end = EmptyOperator(task_id="end")

    # ══════════════════════════════════════════════════════════
    # BRONZE — Ingest fuel_transactions.csv only
    # ══════════════════════════════════════════════════════════
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

    # ══════════════════════════════════════════════════════════
    # SILVER — Fuel Transactions → fact_fuel
    # ══════════════════════════════════════════════════════════
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

    # ══════════════════════════════════════════════════════════
    # GOLD — Fuel Efficiency Audit
    # ══════════════════════════════════════════════════════════
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

    # ══════════════════════════════════════════════════════════
    # REPORTING — Gold → PostgreSQL
    # ══════════════════════════════════════════════════════════
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
    wait_for_midnight >> daily_bronze_fuel >> silver_fuel >> gold_fuel_audit >> gold_to_postgres >> end
