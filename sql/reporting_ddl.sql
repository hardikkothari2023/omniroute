-- ================================================================
-- OmniRoute Reporting Layer — PostgreSQL DDL
-- ================================================================
-- Run this after setting up PostgreSQL per postgresql_setup_guide.md
-- Connect: psql -h localhost -U omniroute_user -d omniroute_reporting
-- ================================================================

-- Create reporting schema
CREATE SCHEMA IF NOT EXISTS report;

-- ──────────────────────────────────────────────────────────────
-- 1. Fleet Assignment History (from Silver dim_vehicle_assignment_scd2)
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS report.fleet_assignment_history (
    assignment_sk   VARCHAR(64) PRIMARY KEY,
    vehicle_sk      VARCHAR(64),
    driver_sk       VARCHAR(64),
    vin             VARCHAR(20),
    driver_id       VARCHAR(20),
    region          VARCHAR(50),
    daily_rate      DECIMAL(10, 2),
    start_date      DATE,
    end_date        DATE,
    is_current      BOOLEAN,
    status          VARCHAR(20),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
);

-- ──────────────────────────────────────────────────────────────
-- 2. Fuel Efficiency Audit (from Gold fuel_efficiency_audit)
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS report.fuel_efficiency_audit (
    vin             VARCHAR(20),
    model           VARCHAR(100),
    audit_date      DATE,
    km_per_liter    REAL,
    baseline_kmpl   REAL,
    variance_pct    REAL,
    status          VARCHAR(10),
    created_at      TIMESTAMP,
    PRIMARY KEY (vin, audit_date)
);

-- ──────────────────────────────────────────────────────────────
-- 3. Active Fleet Snapshot (from Gold active_fleet_snapshot)
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS report.active_fleet_snapshot (
    snapshot_date           DATE,
    model                   VARCHAR(100),
    active_vehicle_count    INTEGER,
    snapshot_ts             TIMESTAMP,
    created_at              TIMESTAMP,
    PRIMARY KEY (snapshot_date, model)
);

-- ──────────────────────────────────────────────────────────────
-- 4. Dim Vehicle (from Silver dim_vehicle)
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS report.dim_vehicle (
    vehicle_sk          VARCHAR(64) PRIMARY KEY,
    vin                 VARCHAR(20) UNIQUE,
    model               VARCHAR(100),
    fuel_type           VARCHAR(20),
    mfg_year            INTEGER,
    baseline_efficiency REAL,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,
    is_active           BOOLEAN
);

-- ──────────────────────────────────────────────────────────────
-- 5. Dim Date (from Silver dim_date)
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS report.dim_date (
    date_id         INTEGER PRIMARY KEY,
    full_date       DATE UNIQUE,
    day             INTEGER,
    month           INTEGER,
    quarter         INTEGER,
    year            INTEGER,
    day_of_week     VARCHAR(10),
    is_weekend      BOOLEAN
);

-- ──────────────────────────────────────────────────────────────
-- 6. Driver Safety Status (from Gold driver_safety_status)
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS report.driver_safety_status (
    driver_id VARCHAR(100) PRIMARY KEY,
    base_rate NUMERIC(10, 2),
    strike_count INT,
    current_adjusted_rate NUMERIC(10, 2),
    status VARCHAR(50),
    month DATE
);

-- ──────────────────────────────────────────────────────────────
-- 7. Driver Strike Fact (from Gold driver_strike_fact)
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS report.fact_driver_strike (
    id SERIAL PRIMARY KEY,
    driver_id VARCHAR(100),
    timestamp TIMESTAMP,
    violation_type VARCHAR(50)
);

-- ──────────────────────────────────────────────────────────────
-- 8. Restricted Zones Table
-- ──────────────────────────────────────────────────────────────
CREATE TABLE restricted_zones (
    zone_name VARCHAR(255) PRIMARY KEY,
    min_lat NUMERIC,
    max_lat NUMERIC,
    min_long NUMERIC,
    max_long NUMERIC
);
INSERT INTO restricted_zones (zone_name, min_lat, max_lat, min_long, max_long) 
VALUES
    ('Military Base Area', 28.725258, 28.784815, 77.024, 77.053353),
    ('No Entry CP', 28.638583, 28.690644, 77.169048, 77.226127),
    ('Industrial Restricted Zone', 28.633124, 28.67933, 77.111065, 77.161576),
    ('High Security Zone', 28.583557, 28.623886, 77.003558, 77.031564),
    ('No Entry CP', 28.735291, 28.778898, 77.026815, 77.079106),
    ('Warehouse Restricted Area', 28.498484, 28.553537, 77.053797, 77.112799),
    ('High Security Zone', 28.620568, 28.649548, 77.192971, 77.221646),
    ('VIP Movement Zone', 28.533174, 28.566724, 77.284107, 77.318825),
    ('Overlap Zone', 28.55, 28.6, 77.08, 77.12),
    ('Overlap Zone 2', 28.57, 28.62, 77.1, 77.15);
-- ──────────────────────────────────────────────────────────────
-- Indexes for common queries
-- ──────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_fuel_audit_status ON report.fuel_efficiency_audit(status);
CREATE INDEX IF NOT EXISTS idx_fuel_audit_date ON report.fuel_efficiency_audit(audit_date);
CREATE INDEX IF NOT EXISTS idx_fleet_snapshot_date ON report.active_fleet_snapshot(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_assignment_vin ON report.fleet_assignment_history(vin);
CREATE INDEX IF NOT EXISTS idx_assignment_status ON report.fleet_assignment_history(status);

-- ──────────────────────────────────────────────────────────────
-- Grant permissions
-- ──────────────────────────────────────────────────────────────
GRANT USAGE ON SCHEMA report TO omniroute_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA report TO omniroute_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA report GRANT ALL ON TABLES TO omniroute_user;

-- ================================================================
-- Done! Verify with: \dt report.*
-- ================================================================
