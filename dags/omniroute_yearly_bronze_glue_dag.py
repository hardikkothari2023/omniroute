"""
OmniRoute — Yearly Bronze + Silver Pipeline DAG (Glue-based)
==============================================================
Schedule : Yearly on January 1st at midnight
Pipeline : Bronze (maintenance_schedules.csv → Parquet) → Silver (fact_maintenance Delta)
           Silver (dim_date Delta generation)

DAG Dependency Graph:
═══════════════════════
  start
    │
    ├───────────────────────────────────┐
    ▼                                   ▼
  trigger_glue_yearly_bronze_ingest   silver_dim_date
    │
    ▼
  silver_maintenance_schedules
    │
    ├───────────────────────────────────┘
    ▼
  end

The maintenance_schedules Silver transformation runs here because
this yearly DAG only ingests maintenance_schedules.csv into Bronze.
The dim_date Silver transformation also runs here to generate dates for the year.
The other 3 Silver tables (registry, assignment, fuel) are handled
by the daily Bronze+Silver DAG.
"""

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator


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
SILVER_MAINTENANCE_PATH = config["silver"]["tables"]["dim_maintenance"]
SILVER_DATE_PATH = config["silver"]["tables"]["dim_date"]

# ── Glue job configs ──
GLUE_YEARLY_BRONZE = config["glue_yearly"]["jobs"]["bronze_ingest"]
GLUE_SILVER_MAINTENANCE = config["glue"]["jobs"]["silver_maintenance_schedules"]
GLUE_SILVER_DIM_DATE = config["glue_yearly"]["jobs"]["dim_date"]

RUN_DATE = "{{ ds }}"
AWS_CONN_ID = "aws_default"


# ──────────────────────────────────────────────────────────────
# Helper — Build Silver GlueJobOperator
# ──────────────────────────────────────────────────────────────
def build_silver_task(task_id, glue_config, script_args):
    """Build a Silver GlueJobOperator with Delta Lake enabled."""
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
# DAG Default Arguments
# ──────────────────────────────────────────────────────────────
default_args = {
    "owner": "omniroute",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


# ──────────────────────────────────────────────────────────────
# DAG Definition
# ──────────────────────────────────────────────────────────────
with DAG(
    dag_id="omniroute_yearly_bronze_silver_pipeline",
    description=(
        "Yearly pipeline: Maintenance Schedules CSV→Parquet ingestion, "
        "then Silver Delta Lake transformation. Also generates dim_date."
    ),
    schedule="0 0 1 1 *",                # Run at midnight on Jan 1st
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bronze", "silver", "glue", "delta", "yearly", "maintenance", "dim_date"],
    default_args=default_args,
    max_active_runs=1,
) as dag:

    # ── Markers ──
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # ──────────────────────────────────────────
    # BRONZE: Yearly Maintenance Schedules Ingestion
    # ──────────────────────────────────────────
    trigger_yearly_bronze = GlueJobOperator(
        task_id="trigger_glue_yearly_bronze_ingest",
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
        retries=1,
        retry_delay=timedelta(minutes=3),
    )

    # ──────────────────────────────────────────
    # SILVER: Maintenance Schedules → dim_maintenance
    # ──────────────────────────────────────────
    silver_maintenance = build_silver_task(
        task_id="silver_maintenance_schedules",
        glue_config=GLUE_SILVER_MAINTENANCE,
        script_args={
            "--run_date": RUN_DATE,
            "--bronze_ingested_path": BRONZE_INGESTED_PATH,
            "--silver_output_path": SILVER_MAINTENANCE_PATH,
        },
    )

    # ──────────────────────────────────────────
    # SILVER: Generate dim_date for the year
    # ──────────────────────────────────────────
    silver_dim_date = build_silver_task(
        task_id="silver_dim_date",
        glue_config=GLUE_SILVER_DIM_DATE,
        script_args={
            "--year": "{{ logical_date.year }}",  # Pass the execution year
            "--silver_output_path": SILVER_DATE_PATH,
        },
    )

    # ──────────────────────────────────────────
    # Task Dependencies
    # ──────────────────────────────────────────
    # Maintenance flows Bronze -> Silver
    start >> trigger_yearly_bronze >> silver_maintenance >> end
    
    # dim_date is independent of Bronze ingest, can run in parallel
    start >> silver_dim_date >> end
