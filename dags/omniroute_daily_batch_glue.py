"""
OmniRoute Daily Batch DAG (AWS Glue version)
============================================
Schedule: Daily @ 05:00 UTC
Pipeline: Bronze → Silver → Gold → Reporting

Ingests vehicle registry, vehicle assignment, and fuel transactions,
then transforms, builds business tables (SCD2, fuel audit, fleet snapshot),
loads reporting DB, and generates CSV reports via AWS Glue.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.sdk import TaskGroup

# ──────────────────────────────────────────────
# DAG Configuration
# ──────────────────────────────────────────────
RUN_DATE = "{{ ds }}"  # YYYY-MM-DD execution date

default_args = {
    "owner": "omniroute",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

GLUE_IAM_ROLE = "OmniRouteGlueRole"
GLUE_REGION = "us-east-1"
GLUE_SCRIPT_BUCKET = "s3://omniroute-glue-scripts/glue_jobs"

# ──────────────────────────────────────────────
# DAG Definition
# ──────────────────────────────────────────────
with DAG(
    dag_id="omniroute_daily_batch_glue",
    description="Daily batch pipeline via AWS Glue: Bronze → Silver → Gold → Reporting",
    schedule="0 5 * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["batch", "daily", "core", "glue"],
    default_args=default_args,
    max_active_runs=1,
) as dag:

    # ──────────────────────────────────────────
    # BRONZE — Ingest raw data from landing/
    # ──────────────────────────────────────────
    with TaskGroup("bronze") as bronze:

        ingest_bronze_layer = GlueJobOperator(
            task_id="ingest_bronze_layer",
            job_name="omniroute_daily_ingest_bronze",
            script_location=f"{GLUE_SCRIPT_BUCKET}/daily_ingest_bronze_glue.py",
            script_args={
                "--run_date": RUN_DATE,
                "--landing_path": "{{ var.json.s3_paths.bronze.landing }}",
                "--ingested_path": "{{ var.json.s3_paths.bronze.ingested }}",
                "--quarantine_path": "{{ var.json.s3_paths.bronze.quarantine }}",
            },
            iam_role_name=GLUE_IAM_ROLE,
            region_name=GLUE_REGION,
            num_of_dpus=2,
            create_job_kwargs={"GlueVersion": "4.0", "WorkerType": "G.1X", "NumberOfWorkers": 2},
        )

    # ──────────────────────────────────────────
    # SILVER — Cleanse, dedup, enrich
    # ──────────────────────────────────────────
    with TaskGroup("silver") as silver:

        transform_assignment = GlueJobOperator(
            task_id="transform_vehicle_assignment",
            job_name="omniroute_transform_vehicle_assignment",
            script_location=f"{GLUE_SCRIPT_BUCKET}/daily_transform_vehicle_assignment_glue.py",
            script_args={
                "--run_date": RUN_DATE,
            },
            iam_role_name=GLUE_IAM_ROLE,
            region_name=GLUE_REGION,
            create_job_kwargs={"GlueVersion": "4.0", "WorkerType": "G.1X", "NumberOfWorkers": 2},
        )

        transform_fuel = GlueJobOperator(
            task_id="transform_fuel_transactions",
            job_name="omniroute_transform_fuel_transactions",
            script_location=f"{GLUE_SCRIPT_BUCKET}/daily_transform_fuel_transactions_glue.py",
            script_args={
                "--run_date": RUN_DATE,
            },
            iam_role_name=GLUE_IAM_ROLE,
            region_name=GLUE_REGION,
            create_job_kwargs={"GlueVersion": "4.0", "WorkerType": "G.1X", "NumberOfWorkers": 2},
        )

    # ──────────────────────────────────────────
    # Task Dependencies
    # ──────────────────────────────────────────

    # Bronze → Silver
    ingest_bronze_layer >> transform_assignment
    ingest_bronze_layer >> transform_fuel

