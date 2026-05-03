# Unified OmniRoute DAG — Daily + Yearly Pipeline

## Goal
Replace the two existing DAGs (`omniroute_daily_bronze_silver_gold_pipeline` and `omniroute_yearly_bronze_silver_pipeline`) with a **single DAG** that handles both daily and yearly runs with correct dependency management.

## BRD Schedule Requirements

| Data Source | Frequency | Schedule | Current DAG |
|-------------|-----------|----------|-------------|
| Vehicle Registry | Daily | 00:00 UTC | daily |
| Vehicle Assignment | Daily (Incremental) | 00:00 UTC | daily |
| Fuel Transactions | Daily | 07:00 UTC | daily |
| Maintenance Schedules | Yearly (Jan 1st) | 00:00 UTC Jan 1 | yearly |
| dim_date | Yearly (Jan 1st) | 00:00 UTC Jan 1 | yearly |

## Design Strategy

### Single DAG, Daily Schedule @ 00:00 UTC

The DAG runs **daily at 00:00 UTC**. On each run, it uses **branching logic** to decide which tasks to execute:

1. **Always run** (daily): Bronze ingestion, Silver registry, Silver assignment, Gold fleet snapshot
2. **Run only on Jan 1st** (yearly): Yearly Bronze ingestion (maintenance CSV), Silver maintenance, Silver dim_date
3. **Depend on yearly success**: Silver fuel transactions, Gold fuel audit, Gold to PostgreSQL — these need maintenance data to exist

### Yearly Task Retry-Until-Success Strategy

Using an **Airflow Variable** (`omniroute_yearly_maintenance_done_YYYY`):

1. On **Jan 1st**: yearly tasks run. If successful → set Variable to `true`. If failed → remains `false`.
2. On **Jan 2nd, 3rd, etc.**: A `BranchPythonOperator` checks the Variable.
   - If `false` → retry yearly tasks, then continue to dependent tasks
   - If `true` → skip yearly tasks, proceed directly to daily-only tasks

> [!IMPORTANT]
> **Fuel transactions depend on maintenance data** (the `is_maintenance_day` flag). Until the yearly maintenance ingestion succeeds, fuel audit data will have incorrect maintenance flags. The DAG ensures this dependency is respected.

## Proposed DAG Graph

```
start (00:00 UTC)
  │
  ├─── check_yearly_needed (BranchPythonOperator)
  │         │
  │    ┌────┴─────────────────┐
  │    ▼                      ▼
  │  yearly_bronze_ingest   skip_yearly (EmptyOperator)
  │    │                      │
  │    ├──► silver_maintenance │
  │    ├──► silver_dim_date    │
  │    │                      │
  │    ▼                      ▼
  │  yearly_complete ◄────────┘  (trigger_rule=none_failed_min_one_success)
  │
  ├─── trigger_daily_bronze (parallel with yearly check)
  │         │
  │         ▼
  │    silver_vehicle_registry
  │         │
  │         ▼
  │    silver_vehicle_assignment
  │
  ▼
  join_daily_and_yearly (trigger_rule=none_failed_min_one_success)
  │
  ▼
  silver_fuel_transactions
  │
  ├──► gold_fuel_efficiency_audit
  ├──► gold_active_fleet_snapshot
  │
  ▼
  gold_to_postgres
  │
  ▼
  end
```

## Open Questions

> [!IMPORTANT]
> **Q1**: The BRD says fuel transactions arrive at **07:00 UTC** but the DAG runs at **00:00 UTC**. Two options:
> - **Option A**: Keep single DAG at 00:00 UTC — fuel transaction Bronze partition from yesterday (the `{{ ds }}` Airflow macro already refers to yesterday's date in daily DAGs, so it picks up data deposited at 07:00 UTC the previous day). This is how the current DAG works.
> - **Option B**: Split into two DAG runs: one at 00:00 UTC (registry + assignment), another at 07:00 UTC (fuel + gold). This adds complexity.
> **Which approach do you prefer?**

> [!NOTE]
> **Q2**: The existing daily DAG's `silver_maintenance_schedules` task (line 84 in the current daily DAG) — I'll **remove** it from the daily flow since maintenance is yearly only. Daily fuel transactions will read the existing Silver maintenance Delta table directly. Correct?

## Proposed Changes

### [NEW] [omniroute_unified_pipeline_dag.py](file:///home/tushar-katyal/Documents/FInal%20project%20main/omniroute/dags/omniroute_unified_pipeline_dag.py)

The unified DAG file with:
- `BranchPythonOperator` to check if yearly tasks are needed
- Airflow Variable (`omniroute_yearly_maintenance_done_YYYY`) tracking
- Correct dependency wiring
- `trigger_rule="none_failed_min_one_success"` on join nodes

### [KEEP] Existing DAGs

The old `omniroute_bronze_glue_dag.py` and `omniroute_yearly_bronze_glue_dag.py` will remain in the repo for reference but should be **disabled** in Airflow once the unified DAG is deployed.

## Verification Plan

### Manual Verification
1. Deploy the new DAG to the Airflow `dags/` folder
2. Verify it appears in the Airflow UI without import errors
3. Test a manual trigger — confirm all tasks execute in correct order
4. Verify the yearly branch logic by checking the Variable in Airflow UI
