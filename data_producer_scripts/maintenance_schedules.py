import csv
import random
import os
from datetime import datetime, timedelta

# ================================
# CONFIG (PRODUCTION READY)
# ================================

BASE_DIR = os.path.dirname(__file__)

# 🔥 Central data directory
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "../data"))

# 🔥 Ensure folder exists (VERY IMPORTANT)
os.makedirs(DATA_DIR, exist_ok=True)

# File paths
REGISTRY_FILE = os.path.join(DATA_DIR, "vehicle_registry.csv")
OUTPUT_FILE = os.path.join(DATA_DIR, "maintenance_schedules.csv")

NUM_RECORDS = 5000

SERVICE_TYPES = [
    "Engine Overhaul",
    "Tire Rotation",
    "Oil Change",
    "Brake Inspection",
    "Battery Replacement",
    "Full Service"
]

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

    for _ in range(NUM_RECORDS):

        vin = random.choice(vins)

        # Random date in 2026
        start_date = datetime(2026, 1, 1)
        random_days = random.randint(0, 364)
        service_date = start_date + timedelta(days=random_days)

        record = {
            "vin": vin,
            "service_date": service_date.strftime("%Y-%m-%d"),
            "service_type": random.choice(SERVICE_TYPES)
        }

        data.append(record)

    return data


# ================================
# EDGE CASES
# ================================

def add_edge_cases(data, vins):

    total = len(data)

    # 1. Duplicate (~1%)
    for _ in range(int(total * 0.01)):
        data.append(random.choice(data).copy())

    # 2. Same VIN + same date conflict (~1%)
    for _ in range(int(total * 0.01)):
        vin = random.choice(vins)
        service_date = "2026-06-15"

        data.append({
            "vin": vin,
            "service_date": service_date,
            "service_type": "Engine Overhaul"
        })

        data.append({
            "vin": vin,
            "service_date": service_date,
            "service_type": "Tire Rotation"
        })

    # 3. Invalid VIN (~0.5%)
    for _ in range(int(total * 0.005)):
        data.append({
            "vin": "INVALID_" + str(random.randint(1000, 9999)),
            "service_date": "2026-05-10",
            "service_type": "Oil Change"
        })

    # 4. Missing service_type (~0.5%)
    for _ in range(int(total * 0.005)):
        data.append({
            "vin": random.choice(vins),
            "service_date": "2026-07-20",
            "service_type": ""
        })

    # 5. Invalid date (~0.5%)
    for _ in range(int(total * 0.005)):
        data.append({
            "vin": random.choice(vins),
            "service_date": "INVALID_DATE",
            "service_type": "Brake Inspection"
        })

    return data


# ================================
# WRITE CSV
# ================================

def write_csv(data):

    fields = ["vin", "service_date", "service_type"]

    with open(OUTPUT_FILE, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(data)

    print(f"Generated {len(data)} records → {OUTPUT_FILE}")


# ================================
# MAIN
# ================================

if __name__ == "__main__":

    print("Loading VINs...")
    vins = load_vins()

    print("Generating maintenance data...")
    data = generate_data(vins)

    print("Adding edge cases...")
    data = add_edge_cases(data, vins)

    random.shuffle(data)

    write_csv(data)

    print("Done!")