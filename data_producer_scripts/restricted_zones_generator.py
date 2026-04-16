import json
import os
import random

# ================================
# CONFIG (PRODUCTION READY)
# ================================

BASE_DIR = os.path.dirname(__file__)

# 🔥 Central data directory
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "../data"))

# 🔥 Ensure folder exists (VERY IMPORTANT)
os.makedirs(DATA_DIR, exist_ok=True)

# File path
OUTPUT_FILE = os.path.join(DATA_DIR, "restricted_zones.json")

NUM_ZONES = 8  # recommended 5–10

# ================================
# REALISTIC ZONE NAMES
# ================================

ZONE_NAMES = [
    "IGI Airport Zone",
    "Delhi Cantonment",
    "No Entry CP",
    "Industrial Restricted Zone",
    "Military Base Area",
    "VIP Movement Zone",
    "Warehouse Restricted Area",
    "High Security Zone"
]

# ================================
# GENERATE VALID ZONES
# ================================

def generate_zones():

    zones = []

    for _ in range(NUM_ZONES):

        center_lat = random.uniform(28.5, 28.8)
        center_long = random.uniform(77.0, 77.4)

        lat_offset = random.uniform(0.01, 0.03)
        long_offset = random.uniform(0.01, 0.03)

        zone = {
            "zone_name": random.choice(ZONE_NAMES),
            "min_lat": round(center_lat - lat_offset, 6),
            "max_lat": round(center_lat + lat_offset, 6),
            "min_long": round(center_long - long_offset, 6),
            "max_long": round(center_long + long_offset, 6)
        }

        zones.append(zone)

    return zones


# ================================
# EDGE CASES
# ================================

def add_edge_cases(zones):

    # 1. Overlapping zones
    zones.append({
        "zone_name": "Overlap Zone",
        "min_lat": 28.55,
        "max_lat": 28.60,
        "min_long": 77.08,
        "max_long": 77.12
    })

    zones.append({
        "zone_name": "Overlap Zone 2",
        "min_lat": 28.57,
        "max_lat": 28.62,
        "min_long": 77.10,
        "max_long": 77.15
    })

    # 2. Invalid coordinates
    zones.append({
        "zone_name": "Invalid Zone",
        "min_lat": 200,
        "max_lat": 300,
        "min_long": 500,
        "max_long": 600
    })

    # 3. Empty zone
    zones.append({})

    # 4. Too large zone
    zones.append({
        "zone_name": "Too Large Zone",
        "min_lat": 28.0,
        "max_lat": 29.0,
        "min_long": 76.0,
        "max_long": 78.0
    })

    return zones


# ================================
# WRITE JSON
# ================================

def write_json(data):

    with open(OUTPUT_FILE, "w") as f:
        json.dump(data, f, indent=4)

    print(f"Generated {len(data)} zones → {OUTPUT_FILE}")


# ================================
# MAIN
# ================================

if __name__ == "__main__":

    print("Generating restricted zones...")

    zones = generate_zones()
    zones = add_edge_cases(zones)

    write_json(zones)

    print("Done!")