# Telemetry Consumer Hardening Walkthrough

This document summarizes the architectural upgrades made to `telemetry_consumer_v2.py` to make it a robust, idempotent streaming engine ready for Airflow and production scaling.

## 1. Idempotent Target File Naming
Previously, files were stamped with just the system time (e.g., `part-170445.parquet`). If the consumer failed and restarted, it would re-process the exact same Kafka messages and write them to a *new* file, duplicating your data.
> [!NOTE] 
> The consumer now reads the precise start and end offsets for every Kafka batch. 
> Output files are strictly named: `telemetry_offset_0_to_500.parquet`. If a batch fails and replays, it will just overwrite the exact same file, completely eliminating data duplication.

## 2. Airflow-Safe Atomic File Writes
When you attach Airflow to collect these files and move them to S3, you might run into an issue where Airflow attempts to move a `.parquet` file while Python is literally still in the middle of saving it, causing corrupt Parquet files.
> [!TIP]
> **Atomic writes** are now used. The script writes to a dummy `file.tmp` in the background, and only renames it to `.parquet` at the very last millisecond when the file is 100% physically mapped to disk.

## 3. Event-Time Cooldown Execution
Since data pipelines can experience lag, evaluating "did the month change?" strictly on your computer's local time is a fatal error.
> [!IMPORTANT]
> The engine no longer uses `datetime.utcnow()`. Instead, it reads the exact `event_timestamp` natively from the payload `(pd.to_datetime(event["event_timestamp"]).timestamp())` to determine the monthly cooldown boundaries, making it 100% resilient to network delays and re-processing historical data.

## 4. Dead Letter Queue Integration
Sensors occasionally drop data or send malformed JSON. Instead of crashing or silently deleting the data, we now capture it for data engineering teams.
> [!NOTE]
> Any event missing `vin` or `event_timestamp` is immediately buffered into the `dlq_batch` array and flushed securely to `data/raw/dlq/dlq_12345.json` on disk for forensics.

## How it works & How to Run:
**Everything remains fully automated**. You don't need to change any Airflow DAG or configuration variables. The `DLQ_DIR` was internally mapped based on your existing config constants.

To start it exactly how you used to, simply run:
```bash
python omniroute/telemetry_consumer_v2.py
```
*(Make sure Zookeeper and Kafka are running first in your EC2 tmux sessions!)*
