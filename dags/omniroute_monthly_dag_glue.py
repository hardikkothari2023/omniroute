"""
OmniRoute — Monthly Pipeline DAG
==================================
Schedule: 1st of every month @ 01:00 UTC

Runs two sequential Glue jobs that perform end-of-month processing:

  1. monthly_rate_deduction_report — Reads daily safety snapshots for the
                                     previous month, computes per-driver
                                     total strikes, rate deductions, and final
                                     payable. Writes report to S3 Gold.

  2. safety_strikes_reset     — Resets strike_count and current_adjusted_rate
                                for all ACTIVE drivers in Postgres. SUSPENDED
                                drivers are left untouched.

DAG Dependency Graph:
═══════════════════════
  start
    │
    ▼
  monthly_rate_deduction_report
    │
    ▼
  safety_strikes_reset
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


# ──────────────────────────────────────────────────────────────
# Load Configuration from s3_paths.json
# ──────────────────────────────────────────────────────────────
CONFIG_PATH = Path(os.path.dirname(os.path.abspath(__file__))).parent / "s3_paths.json"

with open(CONFIG_PATH, "r") as f:
    config = json.load(f)

# ── Gold S3 paths ──
GOLD_DAILY_SAFETY_SNAPSHOT_PATH = config["gold"]["tables"]["daily_safety_snapshot"]
GOLD_MONTHLY_RATE_DEDUCTION_PATH = config["gold"]["tables"]["monthly_rate_deduction"]

# ── Monthly Glue job configs ──
GLUE_SAFETY_RESET   = config["glue_monthly"]["jobs"]["safety_strikes_reset"]
GLUE_RATE_DEDUCTION = config["glue_monthly"]["jobs"]["monthly_rate_deduction_report"]

RUN_DATE    = "{{ ds }}"
RUN_MONTH   = "{{ logical_date.strftime('%Y-%m') }}"   # e.g. 2026-05
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
    dag_id="omniroute_monthly_pipeline",
    description=(
        "Monthly pipeline: Safety strikes reset → Monthly rate deduction report. "
        "Runs on the 1st of every month at 01:00 UTC."
    ),
    schedule="0 1 1 * *",             # 1st of every month at 01:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["gold", "glue", "delta", "monthly", "safety", "reporting"],
    default_args=default_args,
    max_active_runs=1,
) as dag:

    # ── Markers ──
    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    # ══════════════════════════════════════════════════════════
    # STEP 1 — Monthly Rate Deduction Report
    # ══════════════════════════════════════════════════════════
    # Reads all daily safety snapshots for the previous month,
    # computes per-driver total strikes, rate deductions, final
    # payable, and suspension status. Writes to S3 Gold only.
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

    # ══════════════════════════════════════════════════════════
    # STEP 2 — Safety Strikes Reset
    # ══════════════════════════════════════════════════════════
    # Archives the previous month's safety strike tallies to the
    # Gold layer, then clears all driver strike counters in
    # PostgreSQL so the new month begins at zero.
    # Runs AFTER the rate deduction report so the report can
    # read the final unmodified tally.
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

    # ══════════════════════════════════════════════════════════
    # TASK DEPENDENCIES
    # ══════════════════════════════════════════════════════════
    # Rate deduction report runs FIRST so it reads the month's
    # final strike tallies before they are cleared by the reset.
    start >> monthly_rate_deduction >> safety_strikes_reset >> end
