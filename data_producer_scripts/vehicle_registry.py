import csv
import random
import string
from datetime import datetime
import os

# ================================
# CONFIGURATION (PRODUCTION READY)
# ================================

BASE_DIR = os.path.dirname(__file__)

# 🔥 Central data directory
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "../data"))

# 🔥 Ensure folder exists (VERY IMPORTANT for EC2)
os.makedirs(DATA_DIR, exist_ok=True)

# File path
OUTPUT_FILE = os.path.join(DATA_DIR, "vehicle_registry.csv")

NUM_RECORDS = 256482

MODELS = [
    "Freightliner M2",
    "Volvo VNL",
    "Isuzu N-Series",
    "Tata Ultra",
    "Ashok Leyland Dost"
]

FUEL_TYPES = ["Diesel", "LNG", "CNG", "Electric"]

# ================================
# HELPER FUNCTIONS
# ================================

def generate_vin(existing_vins):
    """
    Generate a unique VIN (8 chars alphanumeric)
    """
    while True:
        vin = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
        if vin not in existing_vins:
            return vin


def generate_mfg_year():
    return random.randint(2005, 2025)


def generate_model():
    return random.choice(MODELS)


def generate_fuel_type():
    return random.choice(FUEL_TYPES)


# ================================
# MAIN DATA GENERATION
# ================================

def generate_vehicle_registry():
    existing_vins = set()
    data = []

    for _ in range(NUM_RECORDS):

        vin = generate_vin(existing_vins)
        existing_vins.add(vin)

        record = {
            "vin": vin,
            "model": generate_model(),
            "mfg_year": generate_mfg_year(),
            "fuel_type": generate_fuel_type()
        }

        data.append(record)

    return data


# ================================
# EDGE CASES
# ================================

def add_edge_cases(data):

    if len(data) > 2:

        # 1. Duplicate VIN
        duplicate = data[0].copy()
        data.append(duplicate)

        # 2. Missing mfg_year
        bad_record_1 = data[1].copy()
        bad_record_1["mfg_year"] = ""
        data.append(bad_record_1)

        # 3. Invalid fuel_type
        bad_record_2 = data[2].copy()
        bad_record_2["fuel_type"] = "INVALID_FUEL"
        data.append(bad_record_2)

    return data


# ================================
# WRITE TO CSV
# ================================

def write_to_csv(data):

    fieldnames = ["vin", "model", "mfg_year", "fuel_type"]

    with open(OUTPUT_FILE, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(data)

    print(f"CSV file generated: {OUTPUT_FILE}")
    print(f"Total records: {len(data)}")


# ================================
# MAIN EXECUTION
# ================================

if __name__ == "__main__":

    print("Generating Vehicle Registry Data...")

    data = generate_vehicle_registry()

    data = add_edge_cases(data)

    write_to_csv(data)

    print("Done! Ready for pipeline.")