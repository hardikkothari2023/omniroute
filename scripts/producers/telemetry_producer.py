import sys
import os

CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))
PARENT_DIR  = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)
if PARENT_DIR not in sys.path:
    sys.path.insert(0, PARENT_DIR)

import json
import time
import random
import csv
from datetime import datetime, timedelta
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


def inject_random_edge_case(producer, topic_name, zones):
    """
    Randomly picks ONE edge case and injects it into the main stream.
    Called probabilistically during the main loop to simulate real-world anomalies.
    """
    zone = zones[0] if zones else {"min_lat": 10, "max_lat": 11, "min_long": 10, "max_long": 11}

    # Increased probability distribution for different edge cases
    edge_case = random.choices(
        ["double_violation", "suspension", "late_data", "anonymous", "dlq_bad", "impossible_speed"],
        weights=[20, 20, 20, 15, 15, 10]
    )[0]

    if edge_case == "double_violation":
        # BRD: Speed > 110 AND inside restricted zone simultaneously -> violation_type = 'BOTH'
        event = {
            "vin": "VIN-DOUBLE-VIOLATION",
            "driver_id": "DRV_DOUBLE",
            "speed": random.randint(115, 140),
            "lat": zone["min_lat"] + 0.0001,
            "long": zone["min_long"] + 0.0001,
            "event_timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        }
        producer.send(topic_name, key=b"VIN-DOUBLE-VIOLATION", value=event)
        print(f"[EDGE CASE INJECTED] double_violation → speed={event['speed']}, inside zone")

    elif edge_case == "suspension":
        # One strike towards suspension (DRV-SUSPENSION accumulates over time)
        event = {
            "vin": "VIN-SUSPENSION",
            "driver_id": "DRV-SUSPENSION",
            "speed": random.randint(120, 150),
            "lat": 0, "long": 0,
            "event_timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        }
        producer.send(topic_name, key=b"VIN-SUSPENSION", value=event)
        print(f"[EDGE CASE INJECTED] suspension_strike → DRV-SUSPENSION speed={event['speed']}")

    elif edge_case == "late_data":
        # Simulates a telemetry event that arrives 5-25 minutes late (Watermark testing)
        # Realistic: network delays cause minutes of latency, not days.
        # Tests Gold's 30-minute watermark without creating old date partitions.
        minutes_late = random.randint(5, 25)
        event = {
            "vin": f"VIN-LATE-{random.randint(1, 100)}",
            "driver_id": "DRV_LATE",
            "speed": random.randint(60, 90),
            "lat": 0, "long": 0,
            "event_timestamp": (datetime.utcnow() - timedelta(minutes=minutes_late)).strftime("%Y-%m-%d %H:%M:%S")
        }
        producer.send(topic_name, key=event["vin"].encode('utf-8'), value=event)
        print(f"[EDGE CASE INJECTED] late_data → event_timestamp is {minutes_late} minutes old")

    elif edge_case == "anonymous":
        # Missing driver_id → forces stream-static join to resolve from assignments
        event = {
            "vin": f"VIN-ANON-{random.randint(100, 999)}",
            "driver_id": "",  # Intentionally blank or null
            "speed": random.randint(110, 130),
            "lat": 0, "long": 0,
            "event_timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        }
        producer.send(topic_name, key=event["vin"].encode('utf-8'), value=event)
        print(f"[EDGE CASE INJECTED] anonymous_truck → driver_id missing, vin={event['vin']}")

    elif edge_case == "dlq_bad":
        # Missing VIN or event_timestamp → should be caught by Bronze DLQ
        bad_types = [
            {"driver_id": "DRV_BAD", "speed": 100, "lat": 0, "long": 0, "event_timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")}, # Missing VIN
            {"vin": "VIN-BAD-TS", "driver_id": "DRV_BAD", "speed": 100, "lat": 0, "long": 0}, # Missing Timestamp
            {"vin": "VIN-MALFORMED", "driver_id": "DRV_BAD", "speed": "WAY TOO FAST", "lat": "N/A", "long": "N/A", "event_timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")} # Bad types
        ]
        event = random.choice(bad_types)
        producer.send(topic_name, key=b"DLQ_TEST", value=event)
        print(f"[EDGE CASE INJECTED] dlq_bad → Malformed payload sent to DLQ")

    elif edge_case == "impossible_speed":
        # Negative or impossible speed (500+ km/h) -> should be caught by Bronze validation
        speed = random.choice([-50, 500, 800])
        event = {
            "vin": f"VIN-IMP-{random.randint(10, 99)}",
            "driver_id": "DRV_IMP",
            "speed": speed,
            "lat": 0, "long": 0,
            "event_timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        }
        producer.send(topic_name, key=event["vin"].encode('utf-8'), value=event)
        print(f"[EDGE CASE INJECTED] impossible_speed → speed={speed} km/h")

    producer.flush()

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

    # Inject static BRD edge cases before the infinite loop
    print("Injecting initial edge cases into Kafka stream...")
    inject_random_edge_case(producer, KAFKA_TOPIC, zones)
    inject_random_edge_case(producer, KAFKA_TOPIC, zones)

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
                # 15% chance to inject an edge case instead of a normal event
                if random.random() < 0.15:
                    inject_random_edge_case(producer, KAFKA_TOPIC, zones)
                else:
                    vin = random.choice(vins)
                    driver_id = assignments.get(vin, "DRV_UNKNOWN")

                    event = generate_event(vin, driver_id, zones)
                    # Assign partition key to guarantee ordered processing downstream
                    producer.send(KAFKA_TOPIC, key=vin.encode("utf-8"), value=event)
                    batch_events.append(event)

            if batch_events:
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