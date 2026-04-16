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
    TELEMETRY_CONFIG
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


def load_active_assignments():
    mapping = {}

    with open(ASSIGNMENT_FILE, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["end_timestamp"] == "":
                mapping[row["vin"]] = row["driver_id"]

    return mapping


# ================================
# GENERATE TELEMETRY EVENT
# ================================

def generate_event(vin, driver_id):

    r = random.random()

    if r < 0.80:
        speed = random.randint(NORMAL_SPEED[0], NORMAL_SPEED[1])
    elif r < 0.95:
        speed = random.randint(HIGH_SPEED[0], HIGH_SPEED[1])
    else:
        speed = random.randint(EXTREME_SPEED[0], EXTREME_SPEED[1])

    lat = round(random.uniform(LAT_RANGE[0], LAT_RANGE[1]), 6)
    long = round(random.uniform(LONG_RANGE[0], LONG_RANGE[1]), 6)

    event = {
        "vin": vin,
        "driver_id": driver_id,
        "speed": speed,
        "lat": lat,
        "long": long,
        "event_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
    assignments = load_active_assignments()

    print(f"Loaded {len(vins)} vehicles")
    print(f"Loaded {len(assignments)} active assignments")

    print("Starting telemetry stream...\n")

    while True:

        vin = random.choice(vins)

        driver_id = assignments.get(vin, "DRV_UNKNOWN")

        event = generate_event(vin, driver_id)

        producer.send(KAFKA_TOPIC, value=event)

        print(f"Sent: {event}")

        time.sleep(EVENT_DELAY)


# ================================
# ENTRY POINT
# ================================

if __name__ == "__main__":
    run_producer()