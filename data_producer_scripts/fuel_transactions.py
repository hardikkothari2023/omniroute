import sys
import os

CURRENT_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))

if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

import csv
import random
from datetime import datetime, timedelta

from config import (
    VEHICLE_REGISTRY_FILE,
    FUEL_FILE,
    FUEL_CONFIG,
    DATA_DIR
)

# ================================
# CONFIG (FROM CONFIG)
# ================================

os.makedirs(DATA_DIR, exist_ok=True)

REGISTRY_FILE = VEHICLE_REGISTRY_FILE
OUTPUT_FILE = FUEL_FILE

NUM_RECORDS = FUEL_CONFIG["NUM_RECORDS"]
MIN_FUEL = FUEL_CONFIG["MIN_FUEL"]
MAX_FUEL = FUEL_CONFIG["MAX_FUEL"]
MIN_DISTANCE = FUEL_CONFIG["MIN_DISTANCE"]
MAX_DISTANCE = FUEL_CONFIG["MAX_DISTANCE"]

# ================================
# LOAD VINs
# ================================

def load_vins():
    vins = []

    if not os.path.exists(REGISTRY_FILE):
        raise FileNotFoundError(f"Registry file not found at: {REGISTRY_FILE}")

    with open(REGISTRY_FILE, "r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            vins.append(row["vin"])

    print(f"Loaded {len(vins)} VINs")
    return vins


# ================================
# GENERATE DATA
# ================================

def generate_data(vins):
    data = []
    txn_counter = 1

    base_date = datetime(2026, 1, 1)

    odometer_map = {vin: random.randint(10000, 50000) for vin in vins}

    for _ in range(NUM_RECORDS):

        vin = random.choice(vins)

        distance = random.randint(MIN_DISTANCE, MAX_DISTANCE)
        odometer_map[vin] += distance

        fuel_liters = round(random.uniform(MIN_FUEL, MAX_FUEL), 2)

        timestamp = base_date + timedelta(
            days=random.randint(0, 120),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )

        record = {
            "transaction_id": f"TXN_{txn_counter}",
            "vin": vin,
            "fuel_liters": fuel_liters,
            "odometer_reading": odometer_map[vin],
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S")
        }

        txn_counter += 1
        data.append(record)

    return data


# ================================
# EDGE CASES
# ================================

def add_edge_cases(data, vins):

    total = len(data)

    for _ in range(int(total * 0.01)):
        data.append(random.choice(data).copy())

    for _ in range(int(total * 0.005)):
        row = random.choice(data).copy()
        row["fuel_liters"] = random.choice([0, -10])
        data.append(row)

    for _ in range(int(total * 0.005)):
        row = random.choice(data).copy()
        row["odometer_reading"] -= random.randint(100, 500)
        data.append(row)

    for _ in range(int(total * 0.005)):
        data.append({
            "transaction_id": f"TXN_BAD_{random.randint(1000,9999)}",
            "vin": "INVALID_VIN",
            "fuel_liters": 50,
            "odometer_reading": 20000,
            "timestamp": "2026-04-10 10:00:00"
        })

    for _ in range(int(total * 0.005)):
        row = random.choice(data).copy()
        row["fuel_liters"] = ""
        data.append(row)

    return data


# ================================
# WRITE CSV
# ================================

def write_csv(data):

    fields = ["transaction_id", "vin", "fuel_liters", "odometer_reading", "timestamp"]

    with open(OUTPUT_FILE, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(data)

    print(f"Generated {len(data)} rows  {OUTPUT_FILE}")


# ================================
# MAIN
# ================================

if __name__ == "__main__":

    print("Loading VINs...")
    vins = load_vins()

    print("Generating fuel transactions...")
    data = generate_data(vins)

    print("Adding edge cases...")
    data = add_edge_cases(data, vins)

    random.shuffle(data)

    write_csv(data)

    print("Done!")