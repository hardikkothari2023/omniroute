# Gold Layer & Reporting — Walkthrough

## Summary

Implemented the Gold layer batch processing per BRD requirements: fuel efficiency audit, active fleet snapshot, PostgreSQL reporting layer, and renamed maintenance from fact to dimension.

---

## Files Created

| File | Purpose |
|------|---------|
| [gold_fuel_efficiency_audit_glue.py](file:///home/tushar-katyal/Documents/FInal%20project%20main/omniroute/glue_jobs/gold_fuel_efficiency_audit_glue.py) | Gold: flags vehicles >12% below fuel baseline |
| [gold_active_fleet_snapshot_glue.py](file:///home/tushar-katyal/Documents/FInal%20project%20main/omniroute/glue_jobs/gold_active_fleet_snapshot_glue.py) | Gold: daily IN-TRANSIT vehicle count by model |
| [gold_to_postgres_glue.py](file:///home/tushar-katyal/Documents/FInal%20project%20main/omniroute/glue_jobs/gold_to_postgres_glue.py) | Loads Gold/Silver data into PostgreSQL |
| [reporting_ddl.sql](file:///home/tushar-katyal/Documents/FInal%20project%20main/omniroute/sql/reporting_ddl.sql) | PostgreSQL table definitions |
| [postgresql_setup_guide.md](file:///home/tushar-katyal/Documents/FInal%20project%20main/omniroute/docs/postgresql_setup_guide.md) | EC2 PostgreSQL setup + Glue connection guide |

## Files Modified

| File | Change |
|------|--------|
| [transform_maintenance_schedules_glue.py](file:///home/tushar-katyal/Documents/FInal%20project%20main/omniroute/glue_jobs/transform_maintenance_schedules_glue.py) | `fact_maintenance` → `dim_maintenance` (all references) |
| [s3_paths.json](file:///home/tushar-katyal/Documents/FInal%20project%20main/omniroute/config/s3_paths.json) | Added Gold paths, dim_date, renamed maintenance |
| [omniroute_daily_batch.py](file:///home/tushar-katyal/Documents/FInal%20project%20main/omniroute/dags/omniroute_daily_batch.py) | Uncommented Gold + Reporting task groups |

---

## Key Design Decisions

### maintenance_schedules is a DIMENSION (not fact)
- No numeric measures — it's a lookup table
- Used only to exclude dates from fuel efficiency calculations
- BRD calls it "mandatory downtime" — a reference schedule

### Fuel Efficiency Audit Logic (BRD 3.3.2)
```
distance = current_odometer - previous_odometer (per VIN)
km_per_liter = distance / fuel_liters
variance_pct = ((baseline - km_per_liter) / baseline) * 100
status = 'FLAGGED' if variance > 12% else 'OK'

Exclusions: weekends + maintenance days
```

### Reporting Layer Architecture (BRD 5.2)
```
Gold (S3 Delta) → Glue JDBC → PostgreSQL → BI / SQL / CSV
```

---

## Pipeline Flow

```
Bronze → DQ Gate → Silver → Gold → Reporting (PostgreSQL)
                      │
                      ├── dim_vehicle
                      ├── dim_vehicle_assignment_scd2
                      ├── dim_maintenance
                      ├── fact_fuel
                      ├── dim_date
                      │
                      └──→ Gold:
                            ├── fuel_efficiency_audit
                            ├── active_fleet_snapshot
                            └──→ PostgreSQL (5 tables)
```

---

## Next Steps

1. **Set up PostgreSQL** on EC2 using [postgresql_setup_guide.md](file:///home/tushar-katyal/Documents/FInal%20project%20main/omniroute/docs/postgresql_setup_guide.md)
2. **Run the DDL** to create reporting tables
3. **Upload Gold scripts** to S3
4. **Test** the fuel efficiency audit with real Silver data
