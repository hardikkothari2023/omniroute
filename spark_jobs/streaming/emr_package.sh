#!/bin/bash
# ──────────────────────────────────────────────────────────────
# OmniRoute EMR Package & Upload Script
# EC2 project structure:
#   ~/omniroute/
#     s3_paths.json
#     scripts/
#       config.py
#       ec2_streaming/
#         bronze_streaming.py
#         silver_streaming.py
#         gold_streaming.py
#         bootstrap_omniroute.sh   ← this script's location too
#       producers/
#
# HOW TO RUN (from EC2):
#   cd ~/omniroute
#   bash scripts/ec2_streaming/emr_package.sh
# ──────────────────────────────────────────────────────────────
set -e

BRONZE_BUCKET="ttn-de-bootcamp-bronze-us-east-1"
EMR_S3_PREFIX="poc-bootcamp-group5-bronze/emr"

# Always run from the project root ~/omniroute
cd ~/omniroute
echo "=== OmniRoute EMR Package & Upload ==="
echo "Working from: $(pwd)"
echo ""

# ── 1. Bootstrap script ──
# Installs psycopg2, pyarrow, pandas on all EMR nodes before Spark starts
echo "[1/7] Uploading bootstrap_omniroute.sh..."
aws s3 cp scripts/ec2_streaming/bootstrap_omniroute.sh \
    s3://${BRONZE_BUCKET}/${EMR_S3_PREFIX}/bootstrap_omniroute.sh
echo "      DONE"

# ── 2. s3_paths.json ──
# Scripts read this from S3 on EMR (local disk paths don't exist on EMR workers)
echo "[2/7] Uploading s3_paths.json..."
aws s3 cp s3_paths.json \
    s3://${BRONZE_BUCKET}/${EMR_S3_PREFIX}/s3_paths.json
echo "      DONE"

# ── 3. omniroute_libs.zip ──
# Contains scripts/config.py so EMR workers can do: from scripts.config import POSTGRES_CONFIG
# Using Python's built-in zipfile (zip command not always installed on EC2)
echo "[3/7] Creating omniroute_libs.zip from scripts/config.py..."
python3 -c "
import zipfile
with zipfile.ZipFile('/tmp/omniroute_libs.zip', 'w', zipfile.ZIP_DEFLATED) as zf:
    zf.write('scripts/config.py', 'scripts/config.py')
print('      Created /tmp/omniroute_libs.zip')
"
echo "      Uploading..."
aws s3 cp /tmp/omniroute_libs.zip \
    s3://${BRONZE_BUCKET}/${EMR_S3_PREFIX}/omniroute_libs.zip
echo "      DONE"

# ── 4. Bronze streaming ──
echo "[4/7] Uploading bronze_streaming.py..."
aws s3 cp scripts/ec2_streaming/bronze_streaming.py \
    s3://${BRONZE_BUCKET}/${EMR_S3_PREFIX}/bronze_streaming.py
echo "      DONE"

# ── 5. Silver streaming ──
echo "[5/7] Uploading silver_streaming.py..."
aws s3 cp scripts/ec2_streaming/silver_streaming.py \
    s3://${BRONZE_BUCKET}/${EMR_S3_PREFIX}/silver_streaming.py
echo "      DONE"

# ── 6. Gold streaming ──
echo "[6/7] Uploading gold_streaming.py..."
aws s3 cp scripts/ec2_streaming/gold_streaming.py \
    s3://${BRONZE_BUCKET}/${EMR_S3_PREFIX}/gold_streaming.py
echo "      DONE"

# ── 7. run_all.sh ──
# Single entry point that launches Bronze → Silver → Gold concurrently on EMR
echo "[7/7] Uploading run_all.sh..."
aws s3 cp scripts/ec2_streaming/run_all.sh \
    s3://${BRONZE_BUCKET}/${EMR_S3_PREFIX}/run_all.sh
echo "      DONE"

echo "=== All files uploaded to S3 ==="
echo ""
echo "Verifying upload:"
aws s3 ls s3://${BRONZE_BUCKET}/${EMR_S3_PREFIX}/
echo ""
echo "Next step: Go to EMR console → Clone cluster → add steps."
echo "Only re-run this script when you change code."
