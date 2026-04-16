## Objective

- Track vehicle-driver history using SCD Type 2
- Detect abnormal fuel consumption (>12% deviation)
- Monitor real-time safety violations (speeding, restricted zones)
- Apply penalty system based on safety strikes
- Generate daily and monthly reports

# Data

### 1. Vehicle Registry

- Master vehicle dimension table
- CSV
- Daily full snapshot → Full load

| Column | Type |
| --- | --- |
| `vin` | String |
| `model` | String |
| `mfg_year` | Integer |
| `fuel_type` | String |

### 2. Vehicle Assignment

Most important source, drives **Asset history/ SCD Type 2 logic**. 

- CSV
- Daily incremental data
- 

| Column | Type |
| --- | --- |
| `vin` | String |
| `driver_id` | String |
| `start_timestamp` | UNIX timestamp |
| `end_timestamp` | UNIX/NULL |
| `daily_rate` | Float |
| `region` | String |
- A new row can mean driver swap, rate change, or a corrected record.
- Duplicate records for same VIN and timeframe to be resolved by keeping the highest daily rate.
- When new assignment arrives, previous active record must be closed with an end_date and marked ARCHIVED

### 3. Maintenance Logs

- Yearly on 1st Jan
- CSV
- Represents planned downtime or mandatory service dates

| Column | Type |
| --- | --- |
| `vin` | String |
| `service_date` | Date |
| `service_type` | String |

### 4. Fuel Transactions

- Daily incremental
- CSV

| Column | Type |
| --- | --- |
| `transaction_id` | String |
| `vin` | String |
| `fuel_liters` | Float |
| `odometer_reading` | Float |
| `timestamp` | UTC |

### 5. Telemetry Stream

- Real-time Kafka stream
- JSON
- Stateful stream, must be joined with asset-history data to know which driver is associated with which VIN at the time of event
- Used to detect overspeeding(over 110 kmph) and restricted zone breaches

| Column | Type |
| --- | --- |
| `vin` | String |
| `driver_id` | String |
| `lat` | Float |
| `long` | Float |
| `event_timestamp` | Kafka timestamp |

### 6. Restricted Zones

- Static JSON reference file

| Column | Type |
| --- | --- |
| `zone_name` | String |
| `min_lat` | Float |
| `max_lat` | Float |
| `min_long` | Float |
| `max_long` | Float |

```
| Source Name            | Type        | Format | Frequency                | Schema (Key Fields)                                                                 | Description |
|-----------------------|------------|--------|--------------------------|--------------------------------------------------------------------------------------|-------------|
| Vehicle Registry      | Batch (S3) | CSV    | Daily (Full Load)        | vin, model, mfg_year, fuel_type                                                     | Master list of all vehicles |
| Vehicle Assignment    | Batch (S3) | CSV    | Daily (Incremental)      | vin, driver_id, start_timestamp, end_timestamp, daily_rate, region                 | Tracks driver-vehicle assignments |
| Maintenance Logs      | Batch (S3) | CSV    | Yearly (Jan 1st)         | vin, service_date, service_type                                                     | Scheduled maintenance dates |
| Fuel Transactions     | Batch (S3) | CSV    | Daily (~05:00–07:00 UTC) | transaction_id, vin, fuel_liters, odometer_reading, timestamp                      | Fuel usage and distance tracking |
| Telemetry Stream      | Streaming  | JSON   | Real-time (Kafka)        | vin, driver_id, speed, lat, long, event_timestamp                                  | Live vehicle telemetry data |
| Restricted Zones      | Reference  | JSON   | Static / Ad-hoc          | zone_name, min_lat, max_lat, min_long, max_long                                     | Defines geofenced restricted areas |
```

## Important Notes

### 1. Full vs Incremental Data

- Vehicle Registry is a full snapshot (replace entire dataset daily)
- Vehicle Assignment is incremental (append new records daily)

### 2. Timestamp Handling

- Vehicle Assignment timestamps are Unix format → must be converted
- Fuel Transactions use UTC timestamps

### 3. Data Dependencies

- Telemetry stream must be joined with Vehicle Assignment (for driver mapping)
- Fuel audit depends on:
    - Fuel Transactions
    - Maintenance Logs (to exclude certain days)

### 4. Data Quality Considerations

- Duplicate assignment records may exist → resolve using highest daily_rate
- Missing end_timestamp indicates active assignment
- Telemetry data may contain noisy or high-frequency events

### 5. Special Constraints

- Fuel efficiency excludes:
    - Weekends
    - Maintenance days
- Safety violations:
    - Speed > 110 km/h
    - Restricted zone breach

## Data Relationships

- `vin` is the primary key across all datasets
- Vehicle Assignment links `vin` ↔ `driver_id`
- Telemetry uses `vin` → must join with assignment to identify driver
- Fuel Transactions use `vin` → used for efficiency analysis
- Maintenance Logs affect fuel analysis (exclude days)
- Restricted Zones used in streaming for geofencing