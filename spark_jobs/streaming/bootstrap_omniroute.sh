#!/bin/bash
# ──────────────────────────────────────────────────────────────
# OmniRoute EMR Bootstrap Action
# Runs on every EMR node (master + workers) BEFORE Spark starts.
# Installs Python packages that EMR 7.x (Amazon Linux 2023) doesn't have.
#
# IMPORTANT:
#   - NO "set -e" — we never let a single pip failure kill the cluster.
#   - NO strict version pins — avoids conflicts with EMR's pre-installed libs.
#   - boto3, pandas are already on EMR — we don't reinstall them.
# ──────────────────────────────────────────────────────────────

echo "[BOOTSTRAP] Starting OmniRoute dependency installation..."
echo "[BOOTSTRAP] Python: $(python3 --version)"
echo "[BOOTSTRAP] pip:    $(pip3 --version)"

# psycopg2-binary — PostgreSQL adapter, not on EMR by default
echo "[BOOTSTRAP] Installing psycopg2-binary..."
pip3 install psycopg2-binary 2>&1 || \
    sudo pip3 install psycopg2-binary 2>&1 || \
    echo "[BOOTSTRAP] WARN: psycopg2-binary install failed"

# pyarrow — used for reading Parquet files in Gold loader
# EMR may already have it; just ensure it's present (upgrade if needed)
echo "[BOOTSTRAP] Installing pyarrow..."
pip3 install "pyarrow>=12.0.0" 2>&1 || \
    sudo pip3 install "pyarrow>=12.0.0" 2>&1 || \
    echo "[BOOTSTRAP] WARN: pyarrow install failed"

# ── Verify imports ──
echo "[BOOTSTRAP] Verifying installed packages..."
python3 -c "import psycopg2; print('[OK] psycopg2:', psycopg2.__version__)" 2>&1 || echo "[WARN] psycopg2 not importable"
python3 -c "import pyarrow;  print('[OK] pyarrow:', pyarrow.__version__)"  2>&1 || echo "[WARN] pyarrow not importable"
python3 -c "import boto3;    print('[OK] boto3:', boto3.__version__)"       2>&1 || echo "[WARN] boto3 not importable"
python3 -c "import pandas;   print('[OK] pandas:', pandas.__version__)"    2>&1 || echo "[WARN] pandas not importable"

echo "[BOOTSTRAP] Done."
