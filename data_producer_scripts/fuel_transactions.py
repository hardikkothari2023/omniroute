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
OUTPUT_FILE = os.path.join(DATA_DIR, "fuel_transactions.csv")

NUM_RECORDS = 5000

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

    # 🔥 Track odometer per vehicle (VERY IMPORTANT)
    odometer_map = {vin: random.randint(10000, 50000) for vin in vins}

    for _ in range(NUM_RECORDS):

        vin = random.choice(vins)

        # Increment odometer (time-series behavior)
        distance = random.randint(100, 500)
        odometer_map[vin] += distance

        fuel_liters = round(random.uniform(20, 150), 2)

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

    # 1. Duplicate (~1%)
    for _ in range(int(total * 0.01)):
        data.append(random.choice(data).copy())

    # 2. Invalid fuel (~0.5%)
    for _ in range(int(total * 0.005)):
        row = random.choice(data).copy()
        row["fuel_liters"] = random.choice([0, -10])
        data.append(row)

    # 3. Odometer rollback (~0.5%)
    for _ in range(int(total * 0.005)):
        row = random.choice(data).copy()
        row["odometer_reading"] -= random.randint(100, 500)
        data.append(row)

    # 4. Invalid VIN (~0.5%)
    for _ in range(int(total * 0.005)):
        data.append({
            "transaction_id": f"TXN_BAD_{random.randint(1000,9999)}",
            "vin": "INVALID_VIN",
            "fuel_liters": 50,
            "odometer_reading": 20000,
            "timestamp": "2026-04-10 10:00:00"
        })

    # 5. Missing fuel (~0.5%)
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

    print(f"Generated {len(data)} rows → {OUTPUT_FILE}")


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