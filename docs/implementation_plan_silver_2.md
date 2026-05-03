# Silver Layer Refinements — Null Handling, dim_date, SCD Logic, Cleanup

## Goal

Refine all Silver Glue scripts with updated null handling rules, add a new `dim_date` table, validate `service_type` for fraud, confirm SCD logic choices, and remove `audit_run_id` from all outputs.

---

## User Review Required

> [!IMPORTANT]
> **7 changes** are proposed below. Please review each section carefully.

---

## Answers to Architectural Questions

### Why `fact_maintenance` is a FACT table (not DIM)

Per the ER diagram, `fact_maintenance` is in the **"2. FACT TABLES (CORE ENGINE)"** section. It belongs there because:

| Criteria | fact_maintenance | Dimension tables |
|----------|-----------------|------------------|
| **What each row represents** | A discrete **event** (a service was performed on a vehicle on a specific date) | A description of an **entity** (vehicle, driver, date) |
| **Granularity** | One row per maintenance event | One row per entity |
| **Measures** | The event itself (service_type, date) | Descriptive attributes |
| **Changes over time** | New events are appended | Attributes may change (SCD) |

A fact table records "what happened" — `fact_maintenance` records "Vehicle X had Oil Change on 2026-04-15". It's an **event fact** (no numeric measure, but the occurrence itself is the fact).

### Why `dim_vehicle` uses SCD Type 1 (confirmed CORRECT)

Per the ER diagram, `dim_vehicle` is labeled **"SCD Type 1 + Soft Delete"**. SCD1 is correct because:

- **VIN → model, fuel_type, mfg_year**: These are **immutable** physical properties of a vehicle. A VIN always maps to the same model/year. They don't change over time.
- **If they DO change in source data**: It's a **data correction** (typo fix), not a business change. SCD1 overwrites corrections — which is the right behavior.
- **baseline_efficiency**: May be recalculated/updated — still a correction, not a historical attribute.
- **Soft delete (is_active)**: Tracks whether a vehicle is still in the fleet. If a VIN disappears from the daily source feed → `is_active = FALSE`.

> SCD Type 2 would be wrong here because we'd be preserving "historical" values that were actually just typos.

---

## Proposed Changes

### 1. dim_vehicle — Updated Null Handling

#### [MODIFY] [transform_vehicle_registry_glue.py](file:///home/tushar-katyal/Documents/FInal%20project%20main/omniroute/glue_jobs/transform_vehicle_registry_glue.py)

| Column | Current Behavior | New Behavior |
|--------|-----------------|-------------|
| `model` | NULL → `'UNKNOWN'` | NULL → **DROP row** (model is needed to derive baseline_kmpl) |
| `fuel_type` | NULL → `'UNKNOWN'` → filtered by valid list | NULL → **leave as NULL** (will be handled by valid-list filter downstream; if NULL, it gets filtered out naturally) |
| `baseline_kmpl` | Keep NULL | **Derive from model average** — look up existing Silver data for same model, use AVG(baseline_efficiency). If no match found, keep NULL. |
| `audit_run_id` | Included in output | **Removed** |

**baseline_kmpl derivation logic:**
```python
# After cleaning, before final select:
# 1. Read existing Silver table (if exists)
# 2. Compute avg baseline_efficiency per model
# 3. LEFT JOIN incoming rows on model
# 4. Fill NULL baseline_efficiency with model average
```

---

### 2. dim_vehicle_assignment — Updated Null Handling

#### [MODIFY] [transform_vehicle_assignment_glue.py](file:///home/tushar-katyal/Documents/FInal%20project%20main/omniroute/glue_jobs/transform_vehicle_assignment_glue.py)

| Column | Current Behavior | New Behavior |
|--------|-----------------|-------------|
| `daily_rate` | NULL → `0.0` | NULL → **DROP row** |
| `region` | NULL → `'UNKNOWN'` | NULL → **leave as NULL** |
| `audit_run_id` | Included in output | **Removed** |

---

### 3. fact_maintenance — Service Type Fraud Validation

#### [MODIFY] [transform_maintenance_schedules_glue.py](file:///home/tushar-katyal/Documents/FInal%20project%20main/omniroute/glue_jobs/transform_maintenance_schedules_glue.py)

**Validation approach**: Filter `service_type` against a **known valid list** derived from industry-standard vehicle maintenance categories:

```python
VALID_SERVICE_TYPES = {
    "OIL_CHANGE", "TIRE_ROTATION", "BRAKE_INSPECTION", "BRAKE_REPLACEMENT",
    "ENGINE_TUNE_UP", "TRANSMISSION_SERVICE", "BATTERY_REPLACEMENT",
    "COOLANT_FLUSH", "AIR_FILTER", "FUEL_FILTER", "WHEEL_ALIGNMENT",
    "SUSPENSION_CHECK", "EXHAUST_REPAIR", "AC_SERVICE", "GENERAL_INSPECTION",
    "UNKNOWN"
}
```

- If `service_type` matches → keep as-is
- If `service_type` does NOT match → flag as `'UNVERIFIED'` (keeps the row but marks it)

> [!NOTE]
> This is a basic **whitelist validation**, not full fraud detection. Full fraud detection (e.g., same VIN getting 5 oil changes in a week) would be a Gold layer analytics/ML task.

**Also removing `audit_run_id` from output.**

---

### 4. NEW: dim_date — Date Dimension Table

#### [NEW] [transform_dim_date_glue.py](file:///home/tushar-katyal/Documents/FInal%20project%20main/omniroute/glue_jobs/transform_dim_date_glue.py)

A standalone Glue script that generates a **full year of dates** as a Delta table.

**Schema** (per ER diagram):
| Column | Type | Description |
|--------|------|-------------|
| `date_id` | INT | YYYYMMDD format (PK) |
| `full_date` | DATE | The actual date (unique, can also serve as key) |
| `day` | INT | Day of month (1-31) |
| `month` | INT | Month (1-12) |
| `quarter` | INT | Quarter (1-4) |
| `year` | INT | Year (2026) |
| `day_of_week` | STRING | Monday, Tuesday, etc. |
| `is_weekend` | BOOLEAN | TRUE if Saturday/Sunday |

**Logic**:
- Accepts `--year` parameter (e.g., `2026`)
- Generates all 365/366 dates for that year
- Writes as Delta to Silver dim_date path
- Uses MERGE to avoid duplicates if re-run

**Trigger**: Can be run manually on Jan 1st, or scheduled via an Airflow DAG annually.

---

### 5. fact_fuel — Remove Date Columns (Now in dim_date)

#### [MODIFY] [transform_fuel_transactions_glue.py](file:///home/tushar-katyal/Documents/FInal%20project%20main/omniroute/glue_jobs/transform_fuel_transactions_glue.py)

| Column | Action |
|--------|--------|
| `day_of_week` | **Remove** (now in dim_date, joined via date_id FK) |
| `is_weekend` | **Remove** (now in dim_date, joined via date_id FK) |
| `is_maintenance_day` | **Keep** (this is fact-specific, not a date attribute) |
| `audit_run_id` | **Remove** |

Also remove the `add_weekend_flags()` function entirely since it's no longer needed.

---

### 6. Remove `audit_run_id` from ALL Silver Scripts

Per the ER diagram, `audit_run_id` appears in the Gold layer tables but is marked as FK. Since the user wants it removed from Silver:

| Script | Change |
|--------|--------|
| `transform_vehicle_registry_glue.py` | Remove `audit_run_id` from select, function params, MERGE set |
| `transform_vehicle_assignment_glue.py` | Remove `audit_run_id` from select, function params, MERGE set |
| `transform_fuel_transactions_glue.py` | Remove `audit_run_id` from select, function params |
| `transform_maintenance_schedules_glue.py` | Remove `audit_run_id` from select, function params, MERGE set |

> [!WARNING]
> The ER diagram shows `audit_run_id (FK)` in all Silver tables. Removing it means we lose per-run lineage tracking at the Silver layer. If you want lineage, we can keep it. Confirm removal.

---

## Open Questions

> [!IMPORTANT]
> **1. Service type valid list**: The list I proposed above covers standard vehicle maintenance. Should I add/remove any types specific to OmniRoute's fleet operations?

> [!IMPORTANT]
> **2. baseline_kmpl derivation**: If a model appears for the first time (no historical data), baseline_kmpl will remain NULL. Is that acceptable, or should we have a hardcoded fallback table (e.g., `{"VOLVO_FH16": 3.5, "TATA_PRIMA": 4.2}`)?

> [!IMPORTANT]
> **3. dim_date extra columns**: The ER diagram shows additional columns like `is_holiday`, `is_month_start`, `is_month_end`, `is_fiscal_year_end`, `holiday_name`. Should I include these? Holidays would need a country-specific calendar.

---

## Verification Plan

### Automated Tests
- Re-run each Silver Glue job on existing Bronze data
- Verify row counts (model NULL rows should now be dropped)
- Verify dim_date has 365/366 rows for the year
- Verify fact_fuel no longer has `day_of_week` or `is_weekend` columns

### Manual Verification
- Check Silver Delta tables in S3 for correct schema
- Verify VACUUM cleaned up old Parquet files
