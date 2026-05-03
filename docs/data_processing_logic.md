# OmniRoute — Data Processing Logic & Idempotency Guide

This document explains every merge strategy (SCD1, SCD2, Soft Delete, Insert-Only),
the conditions used, and why. It also covers idempotency guarantees.

---

## Table of Contents
1. [Bronze Ingestion — Idempotent Overwrite](#1-bronze-ingestion)
2. [Silver Vehicle Registry — SCD1 + Soft Delete](#2-silver-vehicle-registry--scd1--soft-delete)
3. [Silver Vehicle Assignment — SCD Type 2](#3-silver-vehicle-assignment--scd-type-2)
4. [Silver Fuel Transactions — Insert-Only MERGE](#4-silver-fuel-transactions--insert-only-merge)
5. [Silver Maintenance Schedules — SCD1](#5-silver-maintenance-schedules--scd1)
6. [Idempotency Analysis](#6-idempotency-analysis)

---

## 1. Bronze Ingestion

**File**: `daily_ingest_bronze_glue.py`
**Strategy**: Read CSV → Validate → Write Parquet (partitioned by `load_date`)

### How It Handles Past + Today's Data
- Each day creates a **separate partition**: `load_date=2026-05-02/`
- Uses `partitionOverwriteMode = "dynamic"` so ONLY today's partition is replaced
- If the same day's job runs twice, it overwrites **only that day's partition** — past days are untouched

### Idempotency
| Scenario | What happens |
|----------|-------------|
| Job runs twice on May 2 | May 2 partition is overwritten (same data) ✅ |
| Job runs on May 3 | Creates May 3 partition, May 2 partition untouched ✅ |

> **Critical Fix Applied**: Without `spark.sql.sources.partitionOverwriteMode = "dynamic"`,
> Spark's default `"static"` mode would **DELETE ALL existing partitions** when writing!

---

## 2. Silver Vehicle Registry — SCD1 + Soft Delete

**File**: `transform_vehicle_registry_glue.py`
**Strategy**: Delta MERGE with SCD Type 1 (overwrite) + Soft Delete

### What is SCD Type 1?
"Just update the record with the latest value. Don't keep history."

If a vehicle's `model` or `fuel_type` changes, we simply overwrite the old value.

### The MERGE Explained

```python
silver_table.alias("existing")
    .merge(incoming_df.alias("incoming"), "existing.vin = incoming.vin")

    # CASE 1: VIN exists in both → UPDATE if anything changed
    .whenMatchedUpdate(
        condition="""
            existing.model != incoming.model
            OR existing.mfg_year != incoming.mfg_year
            OR existing.fuel_type != incoming.fuel_type
            OR existing.is_active = FALSE
        """,
        set={
            "model":              "incoming.model",
            "mfg_year":           "incoming.mfg_year",
            "fuel_type":          "incoming.fuel_type",
            "baseline_efficiency":"incoming.baseline_efficiency",
            "is_active":          lit(True),
            "updated_at":         current_timestamp(),
        }
    )

    # CASE 2: VIN in incoming but NOT in Silver → INSERT new vehicle
    .whenNotMatchedInsertAll()

    # CASE 3: VIN in Silver but NOT in today's incoming → SOFT DELETE
    # (Vehicle no longer in today's registry CSV = deactivated)
    .whenNotMatchedBySourceUpdate(
        condition="existing.is_active = TRUE",
        set={
            "is_active":  lit(False),
            "updated_at": current_timestamp(),
        }
    )
```

### Why `existing.vin = incoming.vin` as the match key?
- VIN is the **primary key** (unique identifier for a vehicle)
- The registry is a **full daily snapshot** — every active vehicle appears in today's CSV
- If a VIN is in today's CSV → it's active (Case 1 or 2)
- If a VIN is NOT in today's CSV → it's been deactivated (Case 3: soft delete)

### How It Handles Past + Today's Data
- **Today's CSV** is a complete snapshot of ALL active vehicles
- Silver table accumulates ALL vehicles ever seen (active + inactive)
- Daily MERGE updates existing records, adds new ones, soft-deletes missing ones
- `is_active = True` means "in today's registry", `False` means "was in registry before but not today"

### Idempotency
Running the same day's data twice produces the **same result**:
- Case 1: Matched rows → same update applied (idempotent)
- Case 2: Already inserted → now matches Case 1 → no change (condition won't trigger)
- Case 3: Same VINs missing → same soft-delete applied

---

## 3. Silver Vehicle Assignment — SCD Type 2 (Two-Pass MERGE)

**File**: `transform_vehicle_assignment_glue.py`
**Strategy**: Delta MERGE with SCD Type 2 (keep full history) using **two passes**

### What is SCD Type 2?
"When data changes, DON'T overwrite. Keep the old record and add a new one."
This preserves the complete history of who drove which vehicle and when.

### Why Can't We Use a Single MERGE?

SCD2 needs to do **two things** for the same incoming row:
1. **CLOSE** the old active record (UPDATE: set end_date, is_current=False)
2. **OPEN** the new active record (INSERT: new row with is_current=True)

A single Delta MERGE can only either UPDATE or INSERT for each incoming row — not both.
So we use **two passes**.

### Why Not Match on `(vin, start_date)` Alone?

With a single MERGE on `(vin, start_date)`:

**Silver has:**

| vin | driver_id | start_date | end_date | is_current |
|-----|-----------|------------|----------|------------|
| ABC | DRV_001 | May 2 | NULL | True |

**Incoming:**

| vin | driver_id | start_date | end_date | is_current |
|-----|-----------|------------|----------|------------|
| ABC | DRV_009 | May 5 | NULL | True |

Match check: `(ABC, May 2)` vs `(ABC, May 5)` → **NO MATCH!** → INSERTS new row.
But the old row `(ABC, May 2, is_current=True)` **stays active**!
Result: **TWO is_current=True records** for VIN ABC!

### The Solution: Two-Pass MERGE

#### PASS 1 — Close Old Active Records

Match key: `vin + is_current = TRUE` (guaranteed 1:1 — only one active per VIN)

```python
silver_table.alias("existing")
    .merge(
        incoming_df.alias("incoming"),
        "existing.vin = incoming.vin AND existing.is_current = TRUE"
    )
    .whenMatchedUpdate(
        # Only close if this is a genuinely NEW assignment (different start_date)
        condition="existing.start_date != incoming.start_date",
        set={
            "end_date":     "incoming.start_date",
            "is_current":   lit(False),
            "status":       lit("ARCHIVED"),
            "updated_at":   current_timestamp(),
        }
    )
    .execute()
```

**Why `existing.start_date != incoming.start_date` in the condition?**
- If start_dates are SAME → same assignment (maybe rate correction) → don't close
- If start_dates are DIFFERENT → genuinely new assignment → close the old one

#### PASS 2 — Insert New Records / Update Existing

Match key: `vin + start_date` (unique assignment identifier)

```python
silver_table.alias("existing")
    .merge(
        incoming_df.alias("incoming"),
        "existing.vin = incoming.vin AND existing.start_date = incoming.start_date"
    )
    .whenMatchedUpdate(
        condition="""
            incoming.daily_rate != existing.daily_rate
            OR incoming.driver_id != existing.driver_id
            OR (incoming.end_date IS NOT NULL AND existing.end_date IS NULL)
            OR (incoming.end_date IS NULL AND existing.end_date IS NOT NULL)
            OR (incoming.end_date != existing.end_date)
        """,
        set={ "driver_id": ..., "daily_rate": ..., "end_date": ..., ... }
    )
    .whenNotMatchedInsertAll()
    .execute()
```

### Concrete Scenario: The Driver Swap (Step by Step)

**Silver BEFORE today's run:**

| vin | driver_id | start_date | end_date | is_current | status |
|-----|-----------|------------|----------|------------|--------|
| ABC | DRV_001 | May 2 | NULL | True | IN-TRANSIT |

**Incoming today (May 5):**

| vin | driver_id | start_date | end_date | is_current |
|-----|-----------|------------|----------|------------|
| ABC | DRV_009 | May 5 | NULL | True |

**PASS 1** — Match on `vin + is_current = TRUE`:
- `existing (ABC, is_current=True)` vs `incoming (ABC)` → **MATCHED**
- Condition: `existing.start_date (May 2) != incoming.start_date (May 5)` → **TRUE**
- Action: CLOSE the old record

**Silver after Pass 1:**

| vin | driver_id | start_date | end_date | is_current | status |
|-----|-----------|------------|----------|------------|--------|
| ABC | DRV_001 | May 2 | **May 5** | **False** | **ARCHIVED** |

**PASS 2** — Match on `vin + start_date`:
- `existing (ABC, May 2)` vs `incoming (ABC, May 5)` → **NO MATCH** (different start_date)
- Action: INSERT new record

**Silver after Pass 2 (FINAL):**

| vin | driver_id | start_date | end_date | is_current | status |
|-----|-----------|------------|----------|------------|--------|
| ABC | DRV_001 | May 2 | May 5 | False | ARCHIVED |
| ABC | DRV_009 | **May 5** | **NULL** | **True** | **IN-TRANSIT** |

Old record closed, new record active, complete audit trail preserved!

### The Update Condition in Pass 2 — Why Each Check?

```sql
incoming.daily_rate != existing.daily_rate       -- Rate correction
OR incoming.driver_id != existing.driver_id      -- Driver correction (same start date)
OR (incoming.end_date IS NOT NULL AND existing.end_date IS NULL)  -- Assignment ended
OR (incoming.end_date IS NULL AND existing.end_date IS NOT NULL)  -- Re-activated
OR (incoming.end_date != existing.end_date)      -- End date correction
```

- **Rate change**: Upstream corrected the daily rate for this assignment
- **Driver correction**: Same vehicle, same start date, different driver (data fix)
- **End date set**: The assignment was open (NULL end_date) and is now closed
- **End date cleared**: Rare — a closed assignment was re-opened
- **End date changed**: The closure date was corrected

### Post-MERGE: Active VIN Enforcement

After both passes, a separate UPDATE archives assignments for vehicles no longer
in the active vehicle registry:

```python
# Read today's active VINs from vehicle registry
active_vins = registry.filter(is_active == True).select("vin")

# Find assignments that are is_current=True but VIN is NOT active
stale = silver.filter(is_current == True)
    .join(active_vins, "left_anti")  # VINs NOT in active registry

# Archive those assignments
DeltaTable.update(
    condition = is_current == True AND vin IN stale_vins,
    set = { is_current: False, status: "ARCHIVED" }
)
```

### How It Handles Past + Today's Data
- Silver keeps **ALL historical assignment records** (that's the point of SCD2)
- Pass 1 closes old active records when a new assignment arrives
- Pass 2 inserts the new assignment or updates existing ones
- The `is_current` flag marks which assignment is the active one for each VIN
- Historical records (`is_current = False`) are never deleted — they're audit trail

### Idempotency
- **Pass 1**: Re-running with same data → old record already closed → values already set → same result ✅
- **Pass 2**: Already inserted → now matches on `(vin, start_date)` → no change (values identical) ✅
- **Post-MERGE VIN check**: Sets same records to `is_current = False` → same result ✅

### What is SCD Type 2?
"When data changes, DON'T overwrite. Keep the old record and add a new one."
This preserves the complete history of who drove which vehicle and when.

### The MERGE Explained

```python
silver_table.alias("existing")
    .merge(
        incoming_df.alias("incoming"),
        "existing.vin = incoming.vin AND existing.start_date = incoming.start_date"
    )

    # CASE 1: Same VIN + same start_date exists → UPDATE if data changed
    .whenMatchedUpdate(
        condition="""
            incoming.daily_rate != existing.daily_rate
            OR incoming.driver_id != existing.driver_id
            OR (incoming.end_date IS NOT NULL AND existing.end_date IS NULL)
            OR (incoming.end_date IS NULL AND existing.end_date IS NOT NULL)
            OR (incoming.end_date != existing.end_date)
        """,
        set={
            "driver_id":  "incoming.driver_id",
            "driver_sk":  "incoming.driver_sk",
            "end_date":   "incoming.end_date",
            "daily_rate": "incoming.daily_rate",
            "region":     "incoming.region",
            "status":     "incoming.status",
            "is_current": "incoming.is_current",
            "updated_at": current_timestamp(),
        }
    )

    # CASE 2: New assignment (different VIN or different start_date) → INSERT
    .whenNotMatchedInsertAll()
```

### ❓ Why `existing.vin = incoming.vin AND existing.start_date = incoming.start_date`?

This is the key question. Here's the reasoning:

**A single VIN can have MULTIPLE assignments over time:**

| vin | driver_id | start_date | end_date | is_current |
|-----|-----------|------------|----------|------------|
| ABC | DRV_001 | 2026-01-01 | 2026-03-01 | false |
| ABC | DRV_002 | 2026-03-01 | 2026-06-01 | false |
| ABC | DRV_005 | 2026-06-01 | NULL | true |

If we matched ONLY on `vin`, the MERGE would try to match the incoming record against
ALL 3 existing records for that VIN, causing ambiguity (Delta requires a 1:1 match).

By matching on `(vin, start_date)`, we uniquely identify each assignment period:
- `(ABC, 2026-01-01)` → the Jan-Mar assignment
- `(ABC, 2026-03-01)` → the Mar-Jun assignment
- `(ABC, 2026-06-01)` → the current assignment

**What happens in each scenario:**

| Incoming Record | Match? | Action |
|----------------|--------|--------|
| `(ABC, 2026-06-01, daily_rate=600)` | YES — matches row 3 | UPDATE if rate/driver changed |
| `(ABC, 2026-07-01, DRV_009)` | NO — new start_date | INSERT as new assignment |
| `(XYZ, 2026-05-01, DRV_010)` | NO — new VIN | INSERT as new assignment |

### The Update Condition — Why Each Check?

```sql
incoming.daily_rate != existing.daily_rate       -- Rate correction
OR incoming.driver_id != existing.driver_id      -- Driver swap (same start date)
OR (incoming.end_date IS NOT NULL AND existing.end_date IS NULL)  -- Assignment ended
OR (incoming.end_date IS NULL AND existing.end_date IS NOT NULL)  -- Re-activated
OR (incoming.end_date != existing.end_date)      -- End date correction
```

- **Rate change**: Upstream corrected the daily rate for this assignment
- **Driver swap**: Same vehicle, same start date, but different driver (data correction)
- **End date set**: The assignment was open (NULL end_date) and is now closed
- **End date cleared**: Rare — a closed assignment was re-opened
- **End date changed**: The closure date was corrected

### Post-MERGE: Active VIN Enforcement

After the MERGE, a separate UPDATE archives assignments for vehicles no longer active:

```python
# Read today's active VINs from vehicle registry
active_vins = registry.filter(is_active == True).select("vin")

# Find assignments that are is_current=True but VIN is NOT active
stale = silver.filter(is_current == True)
    .join(active_vins, "left_anti")  # VINs NOT in active registry

# Archive those assignments
DeltaTable.update(
    condition = is_current == True AND vin IN stale_vins,
    set = { is_current: False, status: "ARCHIVED" }
)
```

### How It Handles Past + Today's Data
- Silver keeps **ALL historical assignment records** (that's the point of SCD2)
- Each day's incoming data adds new records or updates existing ones
- The `is_current` flag marks which assignment is the active one for each VIN
- Historical records (`is_current = False`) are never deleted — they're audit trail

### Idempotency
- **MERGE**: Re-running with same data → matched rows don't trigger update (values identical) → no change ✅
- **Insert**: Already exists → matches Case 1 → no duplicate insert ✅
- **Post-MERGE VIN check**: Sets same records to `is_current = False` → same result ✅

---

## 4. Silver Fuel Transactions — Insert-Only MERGE

**File**: `transform_fuel_transactions_glue.py`
**Strategy**: Delta MERGE, insert-only (no updates to existing records)

### The MERGE Explained

```python
silver_table.alias("existing")
    .merge(df.alias("incoming"), "existing.transaction_id = incoming.transaction_id")
    .whenNotMatchedInsert(values={ ... })
    # NO whenMatchedUpdate — transactions are immutable facts
```

### Why Insert-Only?
Fuel transactions are **immutable facts**. Once recorded:
- The fuel was consumed (can't un-consume it)
- The odometer reading was what it was
- The timestamp is fixed

If the same `transaction_id` appears again (duplicate), it's simply skipped.

### Date Filter (New)
Only transactions from `run_date` are kept:
```python
df = df.filter(col("txn_date") == lit(run_date))
# If run_date = 2026-05-02, only May 2 transactions pass
```

### Idempotency
- Same `transaction_id` arrives twice → already matched → skipped ✅
- Job runs twice for same day → same transactions, same IDs → no duplicates ✅

---

## 5. Silver Maintenance Schedules — SCD1

**File**: `transform_maintenance_schedules_glue.py`
**Strategy**: Delta MERGE with SCD Type 1 (update in place, no history)

### The MERGE Explained

```python
silver_table.alias("existing")
    .merge(
        incoming_df.alias("incoming"),
        "existing.vin = incoming.vin AND existing.service_date = incoming.service_date"
    )

    # If service_type changed for same (vin, date) → UPDATE
    .whenMatchedUpdate(
        condition="existing.service_type != incoming.service_type",
        set={ "service_type": "incoming.service_type" }
    )

    # New maintenance record → INSERT
    .whenNotMatchedInsertAll()
```

### Why `(vin, service_date)` as match key?
Each vehicle has at most **one scheduled maintenance per date**. The combination
`(vin, service_date)` uniquely identifies a maintenance event.

### How It Handles Past + Today's Data
- Maintenance data arrives **yearly** (full year's schedule on Jan 1st)
- Silver accumulates ALL maintenance records across years
- If a service_type correction arrives, it overwrites (SCD1)
- New (vin, date) combinations are inserted

---

## 6. Idempotency Analysis

### What Makes a Pipeline Idempotent?
**Running the same job multiple times with the same input produces the same output.**

### Layer-by-Layer Analysis

| Layer | Script | Idempotent? | Mechanism |
|-------|--------|:-----------:|-----------|
| **Bronze** | `daily_ingest_bronze_glue.py` | ✅ | Dynamic partition overwrite — only replaces today's partition |
| **Silver Registry** | `transform_vehicle_registry_glue.py` | ✅ | Delta MERGE — same input → same MERGE result |
| **Silver Assignment** | `transform_vehicle_assignment_glue.py` | ✅ | Delta MERGE on `(vin, start_date)` — same input → same result |
| **Silver Fuel** | `transform_fuel_transactions_glue.py` | ✅ | Insert-only MERGE on `transaction_id` — duplicates skipped |
| **Silver Maintenance** | `transform_maintenance_schedules_glue.py` | ✅ | Delta MERGE on `(vin, service_date)` — same input → same result |
| **Gold Fuel Audit** | `gold_fuel_efficiency_audit_glue.py` | ✅ | Full overwrite of today's partition in Delta |
| **Gold Fleet Snapshot** | `gold_active_fleet_snapshot_glue.py` | ✅ | Full overwrite of today's snapshot in Delta |

### Failure Scenario: Silver Assignment Fails on Day 1

**Question**: If Silver assignment fails on May 2, what happens when the full DAG runs on May 3?

**Answer**: The system is safe because:

1. **Bronze partition for May 2 is preserved** — Dynamic partition overwrite means May 3's
   bronze run creates `load_date=2026-05-03`, NOT overwriting `load_date=2026-05-02`.

2. **BUT**: The Silver assignment job reads `load_date=run_date` (today's partition only).
   So on May 3, it reads May 3's data, NOT May 2's failed data.

3. **Impact**: May 2's assignments are **not in Silver**. This is acceptable because:
   - May 3's CSV likely contains the corrected/updated assignments
   - The SCD2 merge handles re-appearing VINs correctly
   - No duplicate records are created

4. **To recover May 2**: Manually trigger the DAG with `run_date=2026-05-02`. The Bronze
   partition still exists, and the Silver MERGE is idempotent.

### Failure Scenario: Bronze Fails Mid-Way

If Bronze fails after processing `vehicle_registry.csv` but before `vehicle_assignment.csv`:
- Registry partition is written ✅
- Assignment partition is NOT written ❌
- CSV is NOT archived (archive happens after write)
- **Re-run**: Bronze re-processes all datasets. Registry partition is overwritten (same data).
  Assignment is now written. Both are idempotent. ✅
