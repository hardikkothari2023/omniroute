import csv
import random
from datetime import datetime, timedelta
import os

# ================================
# CONFIG (PRODUCTION READY)
# ================================

BASE_DIR = os.path.dirname(__file__)

# 🔥 Centralized data directory
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "../data"))

# 🔥 Ensure data folder exists (VERY IMPORTANT)
os.makedirs(DATA_DIR, exist_ok=True)

# File paths
REGISTRY_FILE = os.path.join(DATA_DIR, "vehicle_registry.csv")
OUTPUT_FILE = os.path.join(DATA_DIR, "vehicle_assignment.csv")

NUM_ROWS = 20000

REGIONS = ["North", "South", "East", "West", "Central", "NCR", "Mumbai", "Jaipur"]

# ================================
# LOAD VINs FROM REGISTRY
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
# HELPER FUNCTIONS
# ================================

def generate_driver_id(num):
    return f"DRV_{str(num).zfill(5)}"


def to_unix(dt):
    return int(dt.timestamp())


# ================================
# MAIN GENERATION LOGIC
# ================================

def generate_assignments(vins):
    data = []
    driver_counter = 1

    base_date = datetime(2025, 1, 1)

    for vin in vins:

        num_assignments = random.randint(1, 4)

        start_date = base_date + timedelta(days=random.randint(0, 200))

        for i in range(num_assignments):

            driver_id = generate_driver_id(driver_counter)
            driver_counter += 1

            daily_rate = round(random.uniform(300, 1200), 2)
            region = random.choice(REGIONS)

            duration = random.randint(10, 60)
            end_date = start_date + timedelta(days=duration)

            if i == num_assignments - 1:
                end_timestamp = ""
            else:
                end_timestamp = to_unix(end_date)

            record = {
                "vin": vin,
                "driver_id": driver_id,
                "start_timestamp": to_unix(start_date),
                "end_timestamp": end_timestamp,
                "daily_rate": daily_rate,
                "region": region
            }

            data.append(record)

            start_date = end_date + timedelta(days=random.randint(1, 5))

    return data


# ================================
# EDGE CASES
# ================================

def add_edge_cases(data, vins):

    if len(data) > 3 and len(vins) > 3:

        # Duplicate with different rate
        sample = data[0].copy()
        sample["daily_rate"] = 400
        data.append(sample)

        dup = sample.copy()
        dup["daily_rate"] = 600
        data.append(dup)

        # Overlapping assignment
        data.append({
            "vin": vins[1],
            "driver_id": "DRV_OVER1",
            "start_timestamp": to_unix(datetime(2026, 4, 1)),
            "end_timestamp": to_unix(datetime(2026, 4, 20)),
            "daily_rate": 500,
            "region": "North"
        })

        data.append({
            "vin": vins[1],
            "driver_id": "DRV_OVER2",
            "start_timestamp": to_unix(datetime(2026, 4, 10)),
            "end_timestamp": "",
            "daily_rate": 550,
            "region": "North"
        })

        # Invalid VIN
        data.append({
            "vin": "INVALID123",
            "driver_id": "DRV_BAD",
            "start_timestamp": to_unix(datetime(2026, 5, 1)),
            "end_timestamp": "",
            "daily_rate": 500,
            "region": "South"
        })

    return data


# ================================
# WRITE CSV
# ================================

def write_csv(data):

    fields = ["vin", "driver_id", "start_timestamp", "end_timestamp", "daily_rate", "region"]

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

    print("Generating assignments...")
    data = generate_assignments(vins)

    print("Adding edge cases...")
    data = add_edge_cases(data, vins)

    random.shuffle(data)

    write_csv(data)

    print("Done! Ready for pipeline.")