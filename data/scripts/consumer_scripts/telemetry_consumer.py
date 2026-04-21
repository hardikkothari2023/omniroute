import sys
import os

CURRENT_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))

if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

import json
import time
import pandas as pd
from kafka import KafkaConsumer

from config import TELEMETRY_CONFIG, TELEMETRY_RAW_DIR

# ================================
# CONFIG
# ================================

KAFKA_TOPIC = TELEMETRY_CONFIG["KAFKA_TOPIC"]
KAFKA_SERVER = TELEMETRY_CONFIG["KAFKA_SERVER"]

BATCH_SIZE = 500

# ================================
# CREATE CONSUMER (MANUAL COMMIT)
# ================================

def create_consumer():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        enable_auto_commit=False,
        auto_offset_reset="latest",
        group_id="omniroute_consumer_group"
    )
    return consumer


# ================================
# PARTITION PATH BUILDER
# ================================

def get_partition_path(event_timestamp):
    dt = pd.to_datetime(event_timestamp)

    year = dt.year
    month = str(dt.month).zfill(2)
    day = str(dt.day).zfill(2)

    path = os.path.join(
        TELEMETRY_RAW_DIR,
        f"year={year}",
        f"month={month}",
        f"day={day}"
    )

    os.makedirs(path, exist_ok=True)

    return path


# ================================
# WRITE PARQUET (BATCH)
# ================================

def write_parquet(batch):

    if not batch:
        return

    df = pd.DataFrame(batch)

    # Deduplication (IDEMPOTENCY)
    df.drop_duplicates(subset=["vin", "event_timestamp"], inplace=True)

    # Partition by date
    event_time = df["event_timestamp"].iloc[0]
    output_dir = get_partition_path(event_time)

    file_name = f"part-{int(time.time() * 1000)}.parquet"
    full_path = os.path.join(output_dir, file_name)

    df.to_parquet(full_path, index=False)

    print(f"Wrote {len(df)} records  {full_path}")


# ================================
# MAIN CONSUMER LOOP
# ================================

def run_consumer():

    print("Starting Kafka Consumer")

    consumer = create_consumer()

    print(f"Subscribed to topic: {KAFKA_TOPIC}")
    print("Waiting for messages...\n")

    batch = []

    try:
        for message in consumer:

            event = message.value

            # Basic validation (fail-safe)
            if "vin" not in event or "event_timestamp" not in event:
                continue

            batch.append(event)

            if len(batch) >= BATCH_SIZE:

                write_parquet(batch)

                # Commit offset ONLY after successful write
                consumer.commit()

                print("Offset committed\n")

                batch.clear()

    except Exception as e:
        print("ERROR in consumer")
        raise e

    finally:
        consumer.close()
        print("Consumer closed")


# ================================
# ENTRY POINT
# ================================

if __name__ == "__main__":
    run_consumer()