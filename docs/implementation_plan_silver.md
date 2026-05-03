# Silver Layer Glue Scripts — Rewrite + Bronze Updates

## Goal

Rewrite all 4 Silver Glue scripts with proper null handling for essential columns, SCD Type 2 via Delta Lake MERGE, partition pruning on Bronze ingested data, and ER-diagram-aligned output schemas. Update Bronze scripts to **not** move/delete data from the `ingested/` path.

---

## Key Design Decisions

### 1. Bronze Data Stays in Place
Bronze ingested data is **not moved** to a `processed/` folder. Silver reads it via partition pruning (`load_date = run_date`). This means:
- Remove all `move_s3_prefix` calls from the existing Silver scripts
- Remove `bronze_processed_path` parameter from Silver scripts and DAGs

### 2. Null Handling Strategy (per ER Diagram + BRD)
Since Bronze accepts everything as-is (including nulls), Silver is where we enforce data quality:

| Table | Column | If NULL → Action |
|-------|--------|------------------|
| **dim_vehicle** | `vin` | **Drop row** (PK, no way to derive) |
| **dim_vehicle** | `model` | Set to `'UNKNOWN'` |
| **dim_vehicle** | `mfg_year` | Set to `0` (or current year if unreasonable) |
| **dim_vehicle** | `fuel_type` | Set to `'UNKNOWN'` (then filtered by valid list, so effectively dropped) |
| **dim_vehicle** | `baseline_kmpl` | Keep NULL — populated downstream or from source if available |
| **dim_vehicle_assignment_scd2** | `vin` | **Drop row** (FK, unmatchable) |
| **dim_vehicle_assignment_scd2** | `driver_id` | **Drop row** (FK, meaningless without driver) |
| **dim_vehicle_assignment_scd2** | `start_timestamp` | **Drop row** (can't derive assignment start) |
| **dim_vehicle_assignment_scd2** | `daily_rate` | Default to `0.0` (better than dropping — rate can be corrected later) |
| **dim_vehicle_assignment_scd2** | `region` | Set to `'UNKNOWN'` |
| **fact_fuel** | `transaction_id` | **Drop row** (PK, can't dedup without it) |
| **fact_fuel** | `vin` | **Drop row** (FK, can't compute efficiency without vehicle) |
| **fact_fuel** | `fuel_liters` | **Drop row** (core metric, can't compute km/l) |
| **fact_fuel** | `odometer_reading` | **Drop row** (core metric, can't compute distance) |
| **fact_fuel** | `timestamp` | **Drop row** (can't derive date_id or txn_date) |
| **fact_maintenance** | `vin` | **Drop row** (FK, can't exclude maintenance days without it) |
| **fact_maintenance** | `service_date` | **Drop row** (can't match to fuel txn dates) |
| **fact_maintenance** | `service_type` | Set to `'UNKNOWN'` |

### 3. SCD Type 2 — Applied on `dim_vehicle_assignment_scd2`
Per the ER diagram, only `dim_vehicle_assignment_scd2` uses SCD Type 2 (History/Bridge). The match key is `(vin, start_date)`:
- **WHEN MATCHED AND data changed** → UPDATE (close old / adjust values)
- **WHEN NOT MATCHED** → INSERT new record

`dim_vehicle` uses SCD Type 1 + Soft Delete (already correctly implemented).

### 4. Partition Pruning
All Silver scripts will read Bronze data using partition pruning:
```python
# Read only today's partition — no full table scan
bronze_partition = f"{bronze_base}{TABLE_NAME}/load_date={run_date}"
bronze_df = spark.read.parquet(bronze_partition)
```

For **vehicle_registry** (daily full snapshot): read only today's partition since the CSV is a full snapshot each day.
For **maintenance_schedules** (yearly full snapshot): read only today's partition (the Jan 1st load).

### 5. ER Diagram Column Alignment
Per the ER diagram and user instruction — use `is_active` but **not** `is_deleted`:
- `dim_vehicle`: `vehicle_sk, vin, model, fuel_type, mfg_year, baseline_efficiency, created_at, updated_at, is_active, audit_run_id`
- `dim_vehicle_assignment_scd2`: `assignment_sk, vehicle_sk, driver_sk, region, daily_rate, start_date, end_date, is_current, status, created_at, updated_at, audit_run_id`
  - Natural Key (NK): `(vehicle_sk, driver_sk, start_date)`
- `fact_fuel`: `fuel_trx_sk, transaction_id, vehicle_sk, driver_sk, date_id, vin, transaction_timestamp, fuel_liters, odometer_reading_km, txn_date, day_of_week, is_weekend, is_maintenance_day, created_at, audit_run_id`
- `fact_maintenance`: `maintenance_sk, vehicle_sk, date_id, vin, service_date, service_type, description, created_at, audit_run_id`

---

## Proposed Changes

### Silver Glue Scripts

#### [MODIFY] [transform_vehicle_registry_glue.py](file:///home/tushar-katyal/Documents/FInal%20project%20main/omniroute/glue_jobs/transform_vehicle_registry_glue.py)
- **Partition Pruning**: Read only `load_date={run_date}` instead of entire Bronze snapshot
- **Null Handling**: 
  - Drop rows where `vin` is NULL/empty
  - Default `model` → `'UNKNOWN'` if NULL
  - Default `mfg_year` → `0` if NULL (after cast)
  - Default `fuel_type` → `'UNKNOWN'` if NULL
  - Keep `baseline_kmpl` from Bronze if available (new column added to Bronze schema)
- **Remove `is_deleted`**: Per user instruction, only use `is_active`
- **Remove `move_s3_prefix`**: Bronze data stays in place
- **Remove `bronze_processed_path`** parameter
- **SCD1 + Soft Delete MERGE**: Keep existing logic but remove `is_deleted` from the merge conditions and update set. Use only `is_active` for soft-delete tracking.

#### [MODIFY] [transform_vehicle_assignment_glue.py](file:///home/tushar-katyal/Documents/FInal%20project%20main/omniroute/glue_jobs/transform_vehicle_assignment_glue.py)
- **Partition Pruning**: Already reading `load_date={run_date}` ✓
- **Null Handling**:
  - Drop rows where `vin`, `driver_id`, or `start_timestamp` is NULL
  - Default `daily_rate` → `0.0` if NULL/invalid
  - Default `region` → `'UNKNOWN'` if NULL
- **SCD2 MERGE**: Already implemented correctly ✓
- **Remove `move_s3_prefix`**: Bronze data stays in place
- **Remove `bronze_processed_path`** parameter

#### [MODIFY] [transform_fuel_transactions_glue.py](file:///home/tushar-katyal/Documents/FInal%20project%20main/omniroute/glue_jobs/transform_fuel_transactions_glue.py)
- **Partition Pruning**: Already reading `load_date={run_date}` ✓
- **Null Handling**:
  - Drop rows where `transaction_id`, `vin`, `fuel_liters`, `odometer_reading`, or `timestamp` is NULL
  - These are all core metrics — can't derive any of them
- **Remove `move_s3_prefix`**: Bronze data stays in place
- **Remove `bronze_processed_path`** parameter

#### [MODIFY] [transform_maintenance_schedules_glue.py](file:///home/tushar-katyal/Documents/FInal%20project%20main/omniroute/glue_jobs/transform_maintenance_schedules_glue.py)
- **Partition Pruning**: Read only `load_date={run_date}` instead of entire Bronze snapshot
- **Null Handling**:
  - Drop rows where `vin` or `service_date` is NULL
  - Default `service_type` → `'UNKNOWN'` if NULL/empty (already implemented ✓)
- **Remove `move_s3_prefix`**: Bronze data stays in place
- **Remove `bronze_processed_path`** parameter

---

### Silver DAG

#### [MODIFY] [omniroute_silver_glue_dag.py](file:///home/tushar-katyal/Documents/FInal%20project%20main/omniroute/dags/omniroute_silver_glue_dag.py)
- Remove `BRONZE_PROCESSED_PATH` from config loading
- Remove `--bronze_processed_path` from all script_args
- Keep dependency graph the same (Registry & Maintenance parallel → Assignment → Fuel)

---

### S3 Config

#### [MODIFY] [s3_paths.json](file:///home/tushar-katyal/Documents/FInal%20project%20main/omniroute/config/s3_paths.json)
- Add Silver Glue job configurations under `glue.jobs` for all 4 Silver transformations (currently missing — DAG references them but they don't exist in config)

---

## Open Questions

> [!IMPORTANT]
> **1. Vehicle Registry: Full Snapshot vs Incremental?**
> Currently the vehicle_registry Silver script reads ALL Bronze partitions (full snapshot). You want partition pruning to read only today's data. Since vehicle_registry is a daily FULL snapshot (every day the complete list arrives), reading only today's partition is correct. However, the soft-delete logic (`whenNotMatchedBySource`) will only work if we compare today's full snapshot against the entire Silver table. **Reading only today's partition preserves this behavior correctly** since today's CSV IS the complete vehicle list.

> [!IMPORTANT]
> **2. Maintenance Schedules: Read only today's partition?**
> Maintenance is loaded once per year (Jan 1st). Reading only today's partition means we read the single yearly load. This is correct since each yearly load is a full replacement. Confirm this is your intent.

> [!WARNING]
> **3. `baseline_kmpl` in Bronze → Silver**
> I noticed you already added `baseline_kmpl` to the Bronze vehicle_registry schema. The ER diagram shows `baseline_efficiency` in dim_vehicle. Should Silver map `baseline_kmpl` → `baseline_efficiency` directly, or should it remain NULL in Silver (populated by Gold layer based on historical fuel data)?

> [!IMPORTANT]
> **4. Confirm: No `is_deleted` column at all?**
> You said "only use `is_active`, rest all as per the ER diagram." The ER diagram for `dim_vehicle` shows both `is_active` and `is_deleted`. Should I remove `is_deleted` entirely and use only `is_active` for soft-delete tracking?

---

## Verification Plan

### Automated Tests
- Verify all 4 Silver scripts are syntactically valid: `python3 -c "import py_compile; py_compile.compile('file.py')"`
- Verify the Silver DAG file parses without errors
- Verify s3_paths.json is valid JSON

### Manual Verification
- Review output column lists against ER diagram
- Trace null-handling logic for each column
- Verify partition pruning paths match Bronze output structure
