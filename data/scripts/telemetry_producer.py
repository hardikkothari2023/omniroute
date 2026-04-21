import sys
import os

CURRENT_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))

if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

import json
import time
import random
import csv
from datetime import datetime
from kafka import KafkaProducer

from config import (
    VEHICLE_REGISTRY_FILE,
    VEHICLE_ASSIGNMENT_FILE,
    TELEMETRY_CONFIG,
    RESTRICTED_ZONES_FILE
)

# ================================
# CONFIG (FROM CONFIG)
# ================================

REGISTRY_FILE = VEHICLE_REGISTRY_FILE
ASSIGNMENT_FILE = VEHICLE_ASSIGNMENT_FILE

KAFKA_TOPIC = TELEMETRY_CONFIG["KAFKA_TOPIC"]
KAFKA_SERVER = TELEMETRY_CONFIG["KAFKA_SERVER"]
EVENT_DELAY = TELEMETRY_CONFIG["EVENT_DELAY"]

LAT_RANGE = TELEMETRY_CONFIG["LAT_RANGE"]
LONG_RANGE = TELEMETRY_CONFIG["LONG_RANGE"]

NORMAL_SPEED = TELEMETRY_CONFIG["NORMAL_SPEED"]
HIGH_SPEED = TELEMETRY_CONFIG["HIGH_SPEED"]
EXTREME_SPEED = TELEMETRY_CONFIG["EXTREME_SPEED"]

# ================================
# LOAD DATA
# ================================

def load_vins():
    vins = []
    with open(REGISTRY_FILE, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            vins.append(row["vin"])
    return vins


_assignment_cache = {
    "data": {},
    "last_mtime": 0
}

def load_active_assignments(force_refresh=False):
    global _assignment_cache

    if not os.path.exists(ASSIGNMENT_FILE):
        return {}

    mtime = os.path.getmtime(ASSIGNMENT_FILE)
    if not force_refresh and mtime <= _assignment_cache["last_mtime"]:
        return _assignment_cache["data"]

    mapping = {}
    current_time = int(time.time())

    with open(ASSIGNMENT_FILE, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            end_ts = row["end_timestamp"]
            if end_ts == "" or (end_ts.isdigit() and int(end_ts) > current_time):
                mapping[row["vin"]] = row["driver_id"]

    _assignment_cache["data"] = mapping
    _assignment_cache["last_mtime"] = mtime
    return mapping


def load_restricted_zones():
    zones = []
    if os.path.exists(RESTRICTED_ZONES_FILE):
        with open(RESTRICTED_ZONES_FILE, "r") as f:
            data = json.load(f)
            for z in data:
                if "min_lat" in z:
                    zones.append(z)
    return zones


# ================================
# GENERATE TELEMETRY EVENT
# ================================

def generate_event(vin, driver_id, zones):

    r = random.random()

    if r < 0.80:
        speed = random.randint(NORMAL_SPEED[0], NORMAL_SPEED[1])
        force_intrusion = False
    elif r < 0.95:
        speed = random.randint(HIGH_SPEED[0], HIGH_SPEED[1])
        force_intrusion = (random.random() < 0.5) and len(zones) > 0
    else:
        speed = random.randint(EXTREME_SPEED[0], EXTREME_SPEED[1])
        force_intrusion = len(zones) > 0

    if force_intrusion and zones:
        zone = random.choice(zones)
        lat = round(random.uniform(zone["min_lat"], zone["max_lat"]), 6)
        long = round(random.uniform(zone["min_long"], zone["max_long"]), 6)
    else:
        lat = round(random.uniform(LAT_RANGE[0], LAT_RANGE[1]), 6)
        long = round(random.uniform(LONG_RANGE[0], LONG_RANGE[1]), 6)

    event = {
        "vin": vin,
        "driver_id": driver_id,
        "speed": speed,
        "lat": lat,
        "long": long,
        "event_timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    }

    return event


# ================================
# MAIN PRODUCER
# ================================

def run_producer():

    print("Connecting to Kafka...")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print("Loading data...")

    vins = load_vins()
    assignments = load_active_assignments(force_refresh=True)
    zones = load_restricted_zones()

    print(f"Loaded {len(vins)} vehicles")
    print(f"Loaded {len(assignments)} active assignments")
    print(f"Loaded {len(zones)} restricted zones")

    print("Starting telemetry stream in batches... Press Ctrl+C to stop.\n")
    BATCH_SIZE = 50
    last_cache_check = time.time()

    try:
        while True:
            # Refresh assignments every 60 seconds
            if time.time() - last_cache_check > 60:
                assignments = load_active_assignments()
                last_cache_check = time.time()

            batch_events = []
            for _ in range(BATCH_SIZE):
                vin = random.choice(vins)
                driver_id = assignments.get(vin, "DRV_UNKNOWN")

                event = generate_event(vin, driver_id, zones)
                # Assign partition key to guarantee ordered processing downstream
                producer.send(KAFKA_TOPIC, key=vin.encode("utf-8"), value=event)
                batch_events.append(event)

            print(f"Sent batch of {BATCH_SIZE} events. Example: {batch_events[0]}")
            time.sleep(EVENT_DELAY)

    except KeyboardInterrupt:
        print("\nPipeline stopping gracefully. Flushing events to Kafka...")
    finally:
        producer.flush()
        producer.close()
        print("Kafka Producer closed safely.")


# ================================
# ENTRY POINT
# ================================

if __name__ == "__main__":
    run_producer()