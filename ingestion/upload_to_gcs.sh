#!/bin/bash
# upload_to_gcs.sh — Upload all source CSVs to the raw GCS bucket
# Run from the project root directory

set -e

# Load env vars (or export them before running)
PROJECT_ID="${PROJECT_ID:?ERROR: PROJECT_ID env var not set}"
BUCKET_RAW="${BUCKET_RAW:-${PROJECT_ID}-raw}"
DATA_DIR="${DATA_DIR:-./data}"

echo "🚀 Uploading CSVs to gs://${BUCKET_RAW}/"
echo "   Source directory: ${DATA_DIR}"

declare -A FILE_MAP=(
  ["olist_orders_dataset.csv"]="orders/"
  ["olist_customers_dataset.csv"]="customers/"
  ["olist_order_items_dataset.csv"]="order_items/"
  ["olist_order_payments_dataset.csv"]="payments/"
  ["olist_order_reviews_dataset.csv"]="reviews/"
  ["olist_products_dataset.csv"]="products/"
  ["olist_sellers_dataset.csv"]="sellers/"
  ["olist_geolocation_dataset.csv"]="geolocation/"
  ["product_category_name_translation.csv"]="reference/"
)

for FILE in "${!FILE_MAP[@]}"; do
  DEST_FOLDER="${FILE_MAP[$FILE]}"
  SRC="${DATA_DIR}/${FILE}"

  if [[ ! -f "$SRC" ]]; then
    echo "  ⚠️  MISSING: ${SRC} — skipping"
    continue
  fi

  echo "  Uploading ${FILE} → gs://${BUCKET_RAW}/${DEST_FOLDER}"
  gsutil cp "$SRC" "gs://${BUCKET_RAW}/${DEST_FOLDER}"
done

echo ""
echo "✅ Upload complete."
echo ""
echo "Files in bucket:"
gsutil ls -r "gs://${BUCKET_RAW}/"
echo ""
echo "Total size:"
gsutil du -sh "gs://${BUCKET_RAW}/"
