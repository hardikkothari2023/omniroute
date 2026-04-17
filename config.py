import os
import sys

# ================================
# PROJECT ROOT
# ================================

BASE_DIR = os.path.dirname(__file__)

# FIXED: config.py is already in root
PROJECT_ROOT = BASE_DIR

# ================================
# DATA DIRECTORY
# ================================

DATA_DIR = os.path.join(PROJECT_ROOT, "data")

os.makedirs(DATA_DIR, exist_ok=True)

# ================================
# FILE PATHS
# ================================

VEHICLE_REGISTRY_FILE = os.path.join(DATA_DIR, "vehicle_registry.csv")
VEHICLE_ASSIGNMENT_FILE = os.path.join(DATA_DIR, "vehicle_assignment.csv")
MAINTENANCE_FILE = os.path.join(DATA_DIR, "maintenance_schedules.csv")
FUEL_FILE = os.path.join(DATA_DIR, "fuel_transactions.csv")
RESTRICTED_ZONES_FILE = os.path.join(DATA_DIR, "restricted_zones.json")

# ================================
# SCRIPT PATHS
# ================================

DATA_SCRIPTS_DIR = os.path.join(PROJECT_ROOT, "data_producer_scripts")
STREAMING_SCRIPTS_DIR = os.path.join(PROJECT_ROOT, "streaming_data_producer")

# ================================
# EXECUTION CONFIG
# ================================

PYTHON_EXEC = sys.executable

# ================================
# VEHICLE REGISTRY CONFIG
# ================================

VEHICLE_REGISTRY_CONFIG = {
    "NUM_RECORDS": 250000,
    "MODELS": [
        "Freightliner M2",
        "Volvo VNL",
        "Isuzu N-Series",
        "Tata Ultra",
        "Ashok Leyland Dost"
    ],
    "FUEL_TYPES": ["Diesel", "LNG", "CNG", "Electric"]
}

# ================================
# VEHICLE ASSIGNMENT CONFIG
# ================================

VEHICLE_ASSIGNMENT_CONFIG = {
    "NUM_ROWS": 20000,
    "REGIONS": ["North", "South", "East", "West", "Central", "NCR", "Mumbai", "Jaipur"],
    "MIN_RATE": 300,
    "MAX_RATE": 1200
}

# ================================
# MAINTENANCE CONFIG
# ================================

MAINTENANCE_CONFIG = {
    "NUM_RECORDS": 5000,
    "SERVICE_TYPES": [
        "Engine Overhaul",
        "Tire Rotation",
        "Oil Change",
        "Brake Inspection",
        "Battery Replacement",
        "Full Service"
    ]
}

# ================================
# FUEL TRANSACTION CONFIG
# ================================

FUEL_CONFIG = {
    "NUM_RECORDS": 5000,
    "MIN_FUEL": 20,
    "MAX_FUEL": 150,
    "MIN_DISTANCE": 100,
    "MAX_DISTANCE": 500
}

# ================================
# RESTRICTED ZONES CONFIG
# ================================

ZONES_CONFIG = {
    "NUM_ZONES": 8,
    "LAT_RANGE": (28.5, 28.8),
    "LONG_RANGE": (77.0, 77.4),
    "OFFSET_RANGE": (0.01, 0.03)
}

# ================================
# TELEMETRY CONFIG
# ================================

TELEMETRY_CONFIG = {
    "KAFKA_TOPIC": "vehicle_telemetry_topic",
    "KAFKA_SERVER": "localhost:9092",
    "EVENT_DELAY": 3,
    "LAT_RANGE": (28.4, 28.9),
    "LONG_RANGE": (76.8, 77.5),
    "NORMAL_SPEED": (40, 100),
    "HIGH_SPEED": (110, 130),
    "EXTREME_SPEED": (130, 160)
}

# ================================
# PIPELINE CONTROL
# ================================

PIPELINE_CONFIG = {
    "RUN_TELEMETRY": True,
    "ENABLE_EDGE_CASES": True
}