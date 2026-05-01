#!/bin/bash
# =============================================================================
# scripts/load_to_bq.sh
# Loads all 9 CSV files from GCS raw/ into BigQuery raw.* tables
# Run AFTER upload_to_gcs.sh
# Usage: bash scripts/load_to_bq.sh
# =============================================================================

set -e

# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------
PROJECT_ID="${PROJECT_ID:-brazilian-ecommerce-pipeline}"
BUCKET_NAME="${BUCKET_NAME:-${PROJECT_ID}-data-lake}"
REGION="${REGION:-us-central1}"
DATASET="raw"
SCHEMA_DIR="./ingestion/bq_schemas"

echo "======================================================"
echo "  Brazilian E-Commerce Pipeline — BigQuery Load"
echo "======================================================"
echo "Project:  $PROJECT_ID"
echo "Dataset:  $DATASET"
echo "Source:   gs://$BUCKET_NAME/raw/"
echo ""

# Helper function — load one table
# FIX: All flags MUST come before the 3 positional args (table, gcs_uri, schema)
# bq load <flags...> TABLE GCS_URI SCHEMA
load_table() {
  local TABLE_NAME=$1
  local GCS_FILE=$2
  local SCHEMA_FILE=$3
  local PARTITION_FIELD=$4
  local CLUSTER_FIELDS=$5
  local MAX_BAD_RECORDS=${6:-0}   # default 0 = fail on ANY bad row

  echo "Loading $TABLE_NAME..."

  local FLAGS=(
    --project_id="$PROJECT_ID"
    --location="$REGION"
    --source_format=CSV
    --skip_leading_rows=1
    --allow_quoted_newlines
    --allow_jagged_rows
    --replace
    --max_bad_records="$MAX_BAD_RECORDS"
  )

  if [ -n "$PARTITION_FIELD" ]; then
    FLAGS+=(--time_partitioning_field="$PARTITION_FIELD" --time_partitioning_type=MONTH)
  fi

  if [ -n "$CLUSTER_FIELDS" ]; then
    FLAGS+=(--clustering_fields="$CLUSTER_FIELDS")
  fi

  bq load \
    "${FLAGS[@]}" \
    "${DATASET}.${TABLE_NAME}" \
    "gs://${BUCKET_NAME}/raw/${GCS_FILE}" \
    "${SCHEMA_DIR}/${SCHEMA_FILE}"

  if [ $? -eq 0 ]; then
    ROW_COUNT=$(bq query \
      --project_id="$PROJECT_ID" \
      --use_legacy_sql=false \
      --format=csv \
      --quiet \
      "SELECT COUNT(*) as cnt FROM \`${PROJECT_ID}.${DATASET}.${TABLE_NAME}\`" \
      | tail -1)
    echo "  ✓ $TABLE_NAME — ${ROW_COUNT} rows"
  else
    echo "  ✗ ERROR: Failed to load $TABLE_NAME"
    exit 1
  fi
}

# --------------------------------------------------------------------------
# Load all 9 tables
# --------------------------------------------------------------------------
echo "Step 1/9: Loading orders..."
load_table \
  "orders" \
  "olist_orders_dataset.csv" \
  "orders_schema.json" \
  "order_purchase_timestamp" \
  "order_status"

echo "Step 2/9: Loading order_items..."
load_table \
  "order_items" \
  "olist_order_items_dataset.csv" \
  "order_items_schema.json" \
  "" \
  "seller_id,product_id"

echo "Step 3/9: Loading payments..."
load_table \
  "payments" \
  "olist_order_payments_dataset.csv" \
  "payments_schema.json" \
  "" \
  "payment_type"

echo "Step 4/9: Loading reviews..."
# reviews has unescaped commas inside comment_message that shift columns.
# Fix: load all columns as STRING (no type enforcement), then cast + clean in SQL.
# max_bad_records=500 handles the small % of rows too malformed to parse even as strings.
load_table \
  "reviews" \
  "olist_order_reviews_dataset.csv" \
  "reviews_schema.json" \
  "" \
  "" \
  "500"

echo "Step 5/9: Loading customers..."
load_table \
  "customers" \
  "olist_customers_dataset.csv" \
  "customers_schema.json" \
  "" \
  "customer_state"

echo "Step 6/9: Loading products..."
load_table \
  "products" \
  "olist_products_dataset.csv" \
  "products_schema.json" \
  "" \
  "product_category_name"

echo "Step 7/9: Loading sellers..."
load_table \
  "sellers" \
  "olist_sellers_dataset.csv" \
  "sellers_schema.json" \
  "" \
  "seller_state"

echo "Step 8/9: Loading geolocation..."
load_table \
  "geolocation" \
  "olist_geolocation_dataset.csv" \
  "geolocation_schema.json" \
  "" \
  "geolocation_state"

echo "Step 9/9: Loading category_translation..."
load_table \
  "category_translation" \
  "product_category_name_translation.csv" \
  "category_translation_schema.json" \
  "" \
  ""

# --------------------------------------------------------------------------
# Final validation — row counts for all tables
# --------------------------------------------------------------------------
echo ""
echo "======================================================"
echo "  Row Count Validation"
echo "======================================================"

bq query \
  --project_id=$PROJECT_ID \
  --use_legacy_sql=false \
  --format=pretty \
"
SELECT 'orders'              AS table_name, COUNT(*) AS row_count FROM \`${PROJECT_ID}.raw.orders\`
UNION ALL
SELECT 'order_items',                        COUNT(*) FROM \`${PROJECT_ID}.raw.order_items\`
UNION ALL
SELECT 'payments',                           COUNT(*) FROM \`${PROJECT_ID}.raw.payments\`
UNION ALL
SELECT 'reviews',                            COUNT(*) FROM \`${PROJECT_ID}.raw.reviews\`
UNION ALL
SELECT 'customers',                          COUNT(*) FROM \`${PROJECT_ID}.raw.customers\`
UNION ALL
SELECT 'products',                           COUNT(*) FROM \`${PROJECT_ID}.raw.products\`
UNION ALL
SELECT 'sellers',                            COUNT(*) FROM \`${PROJECT_ID}.raw.sellers\`
UNION ALL
SELECT 'geolocation',                        COUNT(*) FROM \`${PROJECT_ID}.raw.geolocation\`
UNION ALL
SELECT 'category_translation',               COUNT(*) FROM \`${PROJECT_ID}.raw.category_translation\`
ORDER BY row_count DESC
"

echo ""
echo "All tables loaded into BigQuery raw.*"
echo "Expected total: ~1.5M rows"
echo "Next step: Run Day 3 — Dataflow Apache Beam pipeline"
