import json
import time
import random
import csv
import os
from datetime import datetime
from kafka import KafkaProducer

# ================================
# CONFIG
# ================================

BASE_DIR = os.path.dirname(__file__)

REGISTRY_FILE = os.path.join(BASE_DIR, "../data/vehicle_registry.csv")
ASSIGNMENT_FILE = os.path.join(BASE_DIR, "../data/vehicle_assignment.csv")

KAFKA_TOPIC = "vehicle_telemetry_topic"
KAFKA_SERVER = "localhost:9092"

EVENT_DELAY = 1   # seconds between events

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
    """
    Load only ACTIVE drivers (end_timestamp = NULL)
    """
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
    """
    Generate realistic telemetry event
    """

    # Speed distribution
    r = random.random()

    if r < 0.80:
        speed = random.randint(40, 100)   # normal
    elif r < 0.95:
        speed = random.randint(110, 130)  # violation
    else:
        speed = random.randint(130, 160)  # extreme

    # Delhi/NCR coordinates
    lat = round(random.uniform(28.4, 28.9), 6)
    long = round(random.uniform(76.8, 77.5), 6)

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

        # Get driver (fallback if missing)
        driver_id = assignments.get(vin, f"DRV_UNKNOWN")

        event = generate_event(vin, driver_id)

        # Send to Kafka
        producer.send(KAFKA_TOPIC, value=event)

        print(f"Sent: {event}")

        time.sleep(EVENT_DELAY)


# ================================
# ENTRY POINT
# ================================

if __name__ == "__main__":
    run_producer()