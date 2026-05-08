# 🚚 OmniRoute — Smart Logistics Data Engine

> An enterprise-grade Data Warehouse solution that ingests, processes, and analyzes high-velocity telemetry data from a global fleet of delivery vehicles using a **Medallion Architecture** on AWS.

---

## Table of Contents

- [Overview](#overview)
- [Business Objectives](#business-objectives)
- [Architecture](#architecture)
- [Data Sources](#data-sources)
- [Medallion Architecture — S3 Layout](#medallion-architecture--s3-layout)
- [Airflow DAGs](#airflow-dags)
  - [1. Midnight Pipeline](#1-midnight-pipeline-omniroute_midnight_pipeline)
  - [2. Morning Fuel Pipeline](#2-morning-fuel-pipeline-omniroute_morning_fuel_pipeline)
  - [3. EMR Streaming Pipeline](#3-emr-streaming-pipeline-omniroute_emr_streaming_pipeline)
- [Spark / Glue Jobs](#spark--glue-jobs)
  - [Batch Jobs](#batch-jobs)
  - [Streaming Jobs](#streaming-jobs)
- [Reporting Layer — PostgreSQL Schema](#reporting-layer--postgresql-schema)
- [Safety & Penalty Framework](#safety--penalty-framework)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
  - [Airflow Variables](#airflow-variables)
  - [Airflow Connections](#airflow-connections)
- [Prerequisites](#prerequisites)
- [Getting Started](#getting-started)

---

## Overview

OmniRoute is a sophisticated data platform that combines **batch ETL** and **real-time streaming** to deliver a comprehensive view of asset health, driver behavior, and operational efficiency for a fleet logistics operation. The system is built exclusively on **AWS** (S3, Glue, EMR, Kafka, PostgreSQL) and orchestrated by **Apache Airflow**.

---

## Business Objectives

| Pillar | Description |
|---|---|
| **Asset Lifecycle Management** | Historical records of vehicle and driver transitions using SCD Type 2 logic |
| **Operational Efficiency** | Fuel consumption auditing — flags vehicles exceeding fleet baseline by >12% |
| **Safety Compliance** | Real-time monitoring of speeding (>110 km/h) and geofence violations |
| **Financial Accountability** | Automated 5% per-strike deductions from driver rates with monthly cooldown resets |

---

## Architecture

```
                        ┌──────────────┐
                        │  Kafka Topic │  (vehicle telemetry — speed, RPM, GPS)
                        └──────┬───────┘
                               │
  CSV/JSON files on S3         │ Real-time stream
  (vehicle_registry,           │
   vehicle_assignment,         │
   fuel_transactions,          ▼
   maintenance_schedules) ──► ┌──────────────────────────────────────────────┐
                              │            AWS EMR Cluster                   │
                              │  ┌──────────┐ ┌──────────┐ ┌──────────┐     │
                              │  │  Bronze   │ │  Silver  │ │   Gold   │     │
                              │  │ Streaming │ │ Streaming│ │ Streaming│     │
                              │  └──────────┘ └──────────┘ └──────────┘     │
                              └──────────────────────────────────────────────┘
                                               │
  ┌────────────────────────────────────────────┼──────────────────────────┐
  │                   AWS Glue (Batch ETL)     │                         │
  │  ┌─────────┐   ┌──────────┐   ┌────────┐  │  ┌───────────────────┐  │
  │  │ Bronze  │──►│  Silver  │──►│  Gold  │──┼─►│   PostgreSQL      │  │
  │  │ Ingest  │   │Transform │   │Aggreg. │  │  │   (Reporting)     │  │
  │  └─────────┘   └──────────┘   └────────┘  │  └───────────────────┘  │
  └────────────────────────────────────────────┴──────────────────────────┘
                               │
                               ▼
                    ┌───────────────────┐
                    │   Apache Airflow  │  (Orchestration — 3 DAGs)
                    └───────────────────┘
```

**Key Technologies:**

| Component | Technology |
|---|---|
| **Storage** | Amazon S3 (Data Lake — Bronze / Silver / Gold) |
| **Batch Compute** | AWS Glue 4.0 (PySpark + Delta Lake) |
| **Stream Compute** | Apache Spark Structured Streaming on EMR 7.12.0 |
| **Message Broker** | Apache Kafka |
| **Orchestration** | Apache Airflow 3.2 |
| **Reporting DB** | PostgreSQL |
| **Table Format** | Delta Lake |

---

## Data Sources

| Source | Format | Frequency | Description |
|---|---|---|---|
| Vehicle Registry | CSV | Daily | Master list — VINs, models, manufacturing years |
| Vehicle Assignment | CSV | Daily (incremental) | Driver ↔ Vehicle mapping with `daily_rate` |
| Maintenance Schedules | CSV | Yearly (Jan 1st) | Scheduled downtime and service dates |
| Fuel Transactions | CSV | Daily | Fuel quantity, cost, odometer readings |
| Telemetry Stream | JSON (Kafka) | Real-time | Speed, RPM, GPS coordinates |
| Restricted Zones | JSON / PostgreSQL | Static / ad-hoc | No-go GPS bounding boxes |

---

## Medallion Architecture — S3 Layout

### Bronze (Raw)

```
s3://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group5-bronze/
├── landing/             # Raw CSV drop zone
├── ingested/            # Validated Parquet (schema-checked)
├── quarantine/          # Rejected / malformed records
└── archive/             # Processed files moved here after ingestion
```

### Silver (Cleansed & Enriched)

```
s3://ttn-de-bootcamp-silver-us-east-1/poc-bootcamp-group5-silver/
├── silver.fuel_transactions/
├── silver.dim_maintenance/
├── silver.dim_date/
├── silver.telemetry/            # (Streaming — Delta)
├── silver.vehicle_assignment/   # SCD Type 2
└── silver.vehicle_registry/
```

### Gold (Business Aggregates)

```
s3://ttn-de-bootcamp-gold-us-east-1/poc-bootcamp-group5-gold/
├── gold.fuel_efficiency_audit/
├── gold.active_fleet_snapshot/
├── gold.daily_safety_snapshot/
└── gold.monthly_rate_deduction/
```

---

## Airflow DAGs

### 1. Midnight Pipeline (`omniroute_midnight_pipeline`)

**Schedule:** `0 0 * * *` — Daily at 00:00 UTC

Unified pipeline handling yearly, daily, and monthly workloads with **Variable-based gating** for idempotent retries.

```
start
  │
  ▼
check_yearly_needed ──────────────────────────┐
  │                                           │
[yearly needed]                         [skip yearly]
  │                                           │
  ▼                                           │
yearly_bronze_ingest                          │
  ├──► silver_maintenance                     │
  ├──► silver_dim_date                        │
  ▼                                           │
mark_yearly_done                              │
  │                                           │
  └────────────────► yearly_gate ◄────────────┘
                        │
                        ▼
              daily_bronze_midnight
                        │
                        ▼
              daily_safety_snapshot
                        │
            ┌───────────┴──────────────┐
            │                          │
            ▼                          ▼
  silver_vehicle_registry    check_monthly_needed ────────┐
            │                      │                      │
            ▼                [monthly needed]        [skip monthly]
  silver_vehicle_assignment        │                      │
            │                      ▼                      │
            ▼            monthly_rate_deduction           │
  gold_active_fleet_snapshot       │                      │
            │                      ▼                      │
            │            safety_strikes_reset             │
            │                      │                      │
            │                      ▼                      │
            │            mark_monthly_done                │
            │                      └───► monthly_gate ◄───┘
            │                                │
            └──────────► end ◄───────────────┘
```

**Yearly tasks** use Airflow Variable `omniroute_yearly_done_YYYY` — if they fail on Jan 1, they retry daily until success, then skip for the rest of the year.

**Monthly tasks** use Airflow Variable `omniroute_monthly_done_YYYY_MM` — run on the first day of the month; once successful, skip for the rest of that month.

---

### 2. Morning Fuel Pipeline (`omniroute_morning_fuel_pipeline`)

**Schedule:** `0 5 * * *` — Daily at 05:00 UTC

Runs after the midnight pipeline and handles fuel-related analytics and reporting.

```
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
```

---

### 3. EMR Streaming Pipeline (`omniroute_emr_streaming_pipeline`)

**Schedule:** Manual trigger

Creates a transient EMR cluster and runs three concurrent Spark Structured Streaming steps:

```
start
  │
  ▼
create_emr_cluster
  │
  ▼
add_streaming_steps (submits all 3 at once)
  │
  ├──► watch_bronze_step ──┐
  ├──► watch_silver_step ──┤
  └──► watch_gold_step  ───┘
                            │
                            ▼
                  terminate_emr_cluster
                            │
                            ▼
                           end
```

**EMR Cluster Configuration:**

| Property | Value |
|---|---|
| Release | `emr-7.12.0` |
| Primary node | `m5a.xlarge × 1` |
| Core nodes | `m5a.xlarge × 3` |
| Task nodes | `m5a.xlarge × 1` |
| Step concurrency | 3 (all streams run in parallel) |
| Applications | Hadoop, Hive, Spark, Livy, JupyterEnterpriseGateway |

---

## Spark / Glue Jobs

### Batch Jobs

Located in `spark_jobs/batch/` — executed by AWS Glue via Airflow.

| Job | Layer | Description |
|---|---|---|
| `daily_ingest_bronze_glue.py` | Bronze | CSV ingestion → validated Parquet with quarantine for rejects |
| `yearly_ingest_maintenance_schedules_glue.py` | Bronze | Annual maintenance schedule ingestion |
| `transform_vehicle_registry_glue.py` | Silver | Vehicle master — dedup, type-casting, enrichment |
| `transform_vehicle_assignment_glue.py` | Silver | SCD Type 2 driver/vehicle assignment history |
| `transform_fuel_transactions_glue.py` | Silver | Fuel transaction cleansing with maintenance/assignment joins |
| `transform_maintenance_schedules_glue.py` | Silver | Maintenance schedule normalization |
| `transform_dim_date_glue.py` | Silver | Date dimension generation (yearly) |
| `gold_fuel_efficiency_audit_glue.py` | Gold | Flags vehicles >12% above fleet baseline (excluding weekends & maintenance) |
| `gold_active_fleet_snapshot_glue.py` | Gold | Daily IN-TRANSIT vehicle count by model |
| `daily_safety_snapshot_glue.py` | Gold | Archives current driver safety status from PostgreSQL to S3 |
| `monthly_rate_deduction_report_glue.py` | Gold | Monthly payroll deduction report based on accumulated strikes |
| `monthly_safety_strikes_reset_glue.py` | Gold | Resets strikes and restores rates (excludes SUSPENDED drivers with ≥10 strikes) |
| `gold_to_postgres_glue.py` | Gold → PG | Syncs Gold tables to PostgreSQL reporting schema |

### Streaming Jobs

Located in `spark_jobs/streaming/` — executed on EMR via Airflow.

| Job | Layer | Description |
|---|---|---|
| `bronze_streaming.py` | Bronze | Kafka → S3 Delta table (raw telemetry ingestion) |
| `silver_streaming.py` | Silver | Enrichment, geofence zone matching, validation, DLQ |
| `gold_streaming.py` | Gold | Stateful strike detection engine — speed & zone violations with `applyInPandasWithState` |

**Supporting files:**

- `bootstrap_omniroute.sh` — EMR bootstrap action (installs Python dependencies on cluster nodes)
- `emr_package.sh` — Packages shared libs into `omniroute_libs.zip` for `--py-files`

---

## Reporting Layer — PostgreSQL Schema

The reporting database (`omniroute_reporting`) uses a `report` schema. Full DDL is in [`sql/reporting_ddl.sql`](sql/reporting_ddl.sql).

| Table | Source Layer | Description |
|---|---|---|
| `report.fleet_assignment_history` | Silver | SCD2 vehicle ↔ driver assignment history |
| `report.fuel_efficiency_audit` | Gold | Fuel audit results with variance % and flag status |
| `report.active_fleet_snapshot` | Gold | Daily IN-TRANSIT vehicle count by model |
| `report.dim_vehicle` | Silver | Vehicle master dimension (VIN, model, fuel type, baseline efficiency) |
| `report.dim_date` | Silver | Calendar dimension |
| `report.driver_safety_status` | Gold | Current driver strike count, adjusted rate, and status |
| `report.fact_driver_strike` | Gold | Strike event log (driver, timestamp, violation type) |
| `restricted_zones` | Static | GPS bounding boxes for geofenced no-go areas |

---

## Safety & Penalty Framework

The real-time streaming pipeline enforces a progressive penalty system:

1. **Violation Detection** — A *Safety Strike* is triggered when:
   - Vehicle speed exceeds **110 km/h**, OR
   - Vehicle GPS coordinates fall within a **restricted zone** (geofence)

2. **Financial Impact** — Each strike results in a **5% deduction** from the driver's `daily_rate`:
   - The penalized rate is stored as `current_adjusted_rate` (preserving the original `base_rate`)

3. **Monthly Cooldown** — On the **1st of every month at 00:00 UTC**:
   - Strikes are **reset to zero** for eligible drivers
   - `current_adjusted_rate` is **restored** to `base_rate`

4. **Suspension** — Drivers who accumulate **≥10 strikes** are marked as `SUSPENDED`:
   - Excluded from the monthly cooldown
   - Effectively blocked from fleet operations

---

## Project Structure

```
omniroute/
├── config/
│   ├── s3_paths.json                 # All S3 path configuration (Bronze/Silver/Gold + Glue jobs)
│   ├── airflow_variables.json        # Airflow Variable definitions (PG creds, EMR subnet, etc.)
│   └── airflow_connections.json      # Airflow Connection definitions (aws_default)
│
├── dags/
│   ├── omniroute_midnight_pipeline_dag.py    # DAG 1 — Daily @ 00:00 UTC (yearly + daily + monthly)
│   ├── omniroute_morning_fuel_pipeline_dag.py# DAG 2 — Daily @ 05:00 UTC (fuel pipeline)
│   └── EMR_DAG.py                           # DAG 3 — Manual (EMR streaming cluster)
│
├── spark_jobs/
│   ├── batch/                        # AWS Glue PySpark scripts (Bronze, Silver, Gold)
│   │   ├── daily_ingest_bronze_glue.py
│   │   ├── yearly_ingest_maintenance_schedules_glue.py
│   │   ├── transform_vehicle_registry_glue.py
│   │   ├── transform_vehicle_assignment_glue.py
│   │   ├── transform_fuel_transactions_glue.py
│   │   ├── transform_maintenance_schedules_glue.py
│   │   ├── transform_dim_date_glue.py
│   │   ├── gold_fuel_efficiency_audit_glue.py
│   │   ├── gold_active_fleet_snapshot_glue.py
│   │   ├── daily_safety_snapshot_glue.py
│   │   ├── monthly_rate_deduction_report_glue.py
│   │   ├── monthly_safety_strikes_reset_glue.py
│   │   └── gold_to_postgres_glue.py
│   └── streaming/                    # EMR Spark Structured Streaming scripts
│       ├── bronze_streaming.py
│       ├── silver_streaming.py
│       ├── gold_streaming.py
│       ├── bootstrap_omniroute.sh
│       └── emr_package.sh
│
├── scripts/
│   ├── config.py                     # Shared configuration loader
│   └── producers/                    # Kafka data producers (test/simulation)
│       ├── telemetry_producer.py
│       ├── vehicle_registry_producer.py
│       ├── vehicle_assignment_producer.py
│       ├── fuel_transactions_producer.py
│       ├── maintenance_schedules_producer.py
│       ├── restricted_zones_producer.py
│       └── run_all_producers.py
│
├── sql/
│   └── reporting_ddl.sql             # PostgreSQL DDL for the reporting schema
│
├── tests/
│   └── test_streaming.py             # Streaming pipeline tests
│
├── docs/
│   └── BRD_context.txt               # Business Requirements Document
│
├── requirements.txt
└── .gitignore
```

---

## Configuration

### Airflow Variables

Set these in the Airflow UI or import from `config/airflow_variables.json`:

| Variable | Description |
|---|---|
| `emr_subnet_id` | VPC subnet for EMR cluster provisioning |
| `pg_host` | PostgreSQL host address |
| `pg_port` | PostgreSQL port (default: `5432`) |
| `pg_database` | Reporting database name (`omniroute_reporting`) |
| `pg_user` | PostgreSQL username |
| `pg_password` | PostgreSQL password |
| `omniroute_yearly_done_YYYY` | Yearly job completion gate (auto-managed) |
| `omniroute_monthly_done_YYYY_MM` | Monthly job completion gate (auto-managed) |

### Airflow Connections

| Connection ID | Type | Description |
|---|---|---|
| `aws_default` | AWS | Default AWS connection (`us-east-1`) — uses instance profile / IAM role |

---

## Prerequisites

- **AWS Account** with access to S3, Glue, EMR, and IAM
- **Apache Airflow 3.2+** with the `amazon` provider package
- **Apache Kafka** cluster (for telemetry streaming)
- **PostgreSQL** instance (for the reporting layer)
- **Python 3.10+**

### Key Python Dependencies

```
apache-airflow==3.2.0
pyspark==4.1.1
kafka-python==2.0.2
psycopg2-binary==2.9.9
boto3
pandas==2.2.2
pyarrow
apache-airflow-providers-amazon
```

---

## Getting Started

### 1. Clone the Repository

```bash
git clone <repo-url>
cd omniroute
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Set Up PostgreSQL

```bash
psql -h <host> -U omniroute_user -d omniroute_reporting -f sql/reporting_ddl.sql
```

### 4. Configure Airflow

```bash
# Import variables
airflow variables import config/airflow_variables.json

# Import connections
airflow connections import config/airflow_connections.json
```

### 5. Deploy Glue Scripts to S3

Upload each script in `spark_jobs/batch/` to its corresponding `script_location` defined in `config/s3_paths.json`.

### 6. Deploy Streaming Scripts to EMR S3

```bash
# Package shared libraries
cd spark_jobs/streaming
bash emr_package.sh

# Upload streaming scripts + bootstrap to S3
aws s3 cp bronze_streaming.py s3://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group5-bronze/emr/
aws s3 cp silver_streaming.py s3://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group5-bronze/emr/
aws s3 cp gold_streaming.py   s3://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group5-bronze/emr/
aws s3 cp bootstrap_omniroute.sh s3://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group5-bronze/emr/
```

### 7. Enable DAGs

Copy the DAG files to your Airflow `dags/` folder and enable them from the Airflow UI.

---

## License

Internal project — not for public distribution.
