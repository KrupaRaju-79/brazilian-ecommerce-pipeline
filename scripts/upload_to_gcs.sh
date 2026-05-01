#!/bin/bash
# =============================================================================
# upload_to_gcs.sh
# Uploads all Olist CSV files to the GCS raw/ landing zone
# Usage: bash scripts/upload_to_gcs.sh
# =============================================================================

set -e   # Exit immediately on any error

# --------------------------------------------------------------------------
# Config — override these with environment variables or edit here
# --------------------------------------------------------------------------
PROJECT_ID="${PROJECT_ID:-brazilian-ecommerce-pipeline}"
BUCKET_NAME="${BUCKET_NAME:-${PROJECT_ID}-data-lake}"
DATA_DIR="${DATA_DIR:-./data}"

echo "======================================================"
echo "  Brazilian E-Commerce Pipeline — GCS Upload Script"
echo "======================================================"
echo "Project:  $PROJECT_ID"
echo "Bucket:   gs://$BUCKET_NAME"
echo "Data dir: $DATA_DIR"
echo ""

# --------------------------------------------------------------------------
# Check: data directory exists
# --------------------------------------------------------------------------
if [ ! -d "$DATA_DIR" ]; then
  echo "ERROR: Data directory '$DATA_DIR' not found."
  echo "  Create it and place your CSV files inside, or set DATA_DIR:"
  echo "  DATA_DIR=/path/to/csvs bash scripts/upload_to_gcs.sh"
  exit 1
fi

# --------------------------------------------------------------------------
# Check: all required files exist
# --------------------------------------------------------------------------
REQUIRED_FILES=(
  "olist_orders_dataset.csv"
  "olist_order_items_dataset.csv"
  "olist_order_payments_dataset.csv"
  "olist_order_reviews_dataset.csv"
  "olist_customers_dataset.csv"
  "olist_products_dataset.csv"
  "olist_sellers_dataset.csv"
  "olist_geolocation_dataset.csv"
  "product_category_name_translation.csv"
)

echo "Checking required files..."
MISSING=0
for FILE in "${REQUIRED_FILES[@]}"; do
  if [ ! -f "${DATA_DIR}/${FILE}" ]; then
    echo "  MISSING: ${FILE}"
    MISSING=1
  else
    echo "  OK: ${FILE}"
  fi
done

if [ "$MISSING" -eq 1 ]; then
  echo ""
  echo "ERROR: Some files are missing. Place all CSV files in $DATA_DIR and retry."
  exit 1
fi

echo ""
echo "All files present. Starting upload..."
echo ""

# --------------------------------------------------------------------------
# Upload each file with progress
# --------------------------------------------------------------------------
UPLOAD_COUNT=0
TOTAL=${#REQUIRED_FILES[@]}

for FILE in "${REQUIRED_FILES[@]}"; do
  UPLOAD_COUNT=$((UPLOAD_COUNT + 1))
  LOCAL_PATH="${DATA_DIR}/${FILE}"
  GCS_PATH="gs://${BUCKET_NAME}/raw/${FILE}"

  echo "[$UPLOAD_COUNT/$TOTAL] Uploading $FILE..."
  gcloud storage cp "$LOCAL_PATH" "$GCS_PATH"

  if [ $? -eq 0 ]; then
    echo "  Uploaded to $GCS_PATH"
  else
    echo "  ERROR uploading $FILE"
    exit 1
  fi
done

# --------------------------------------------------------------------------
# Verify: list uploaded files
# --------------------------------------------------------------------------
echo ""
echo "======================================================"
echo "Upload complete. Verifying files in GCS..."
echo "======================================================"
gcloud storage ls "gs://${BUCKET_NAME}/raw/"

echo ""
echo "File sizes:"
gcloud storage du "gs://${BUCKET_NAME}/raw/" --human-readable

echo ""
echo "Done! All 9 files are in gs://${BUCKET_NAME}/raw/"
echo "Next step: Run Day 2 — BigQuery schema load"
