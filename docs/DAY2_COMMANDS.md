# Day 2 — All Commands Reference
# BigQuery: Datasets → Schema Load → Validation → Partitioning

> Copy-paste ready. Every command for Day 2 in order.
> Assumes Day 1 is complete: CSVs are in gs://YOUR_BUCKET/raw/

---

## 0. Re-export Variables (run this every new terminal session)

```bash
export PROJECT_ID="brazilian-ecommerce-pipeline"
export REGION="us-central1"
export BUCKET_NAME="${PROJECT_ID}-data-lake"
export DATASET_RAW="raw"
export DATASET_STAGING="staging"
export DATASET_MART="mart"
export SCHEMA_DIR="./ingestion/bq_schemas"
export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/credentials/pipeline-sa-key.json"
```

---

## 1. Create BigQuery Datasets

```bash
# Create raw dataset (source data, typed but untransformed)
bq mk \
  --project_id=$PROJECT_ID \
  --location=$REGION \
  --dataset \
  --description="Source CSV data loaded from GCS — typed but not transformed" \
  ${PROJECT_ID}:${DATASET_RAW}

# Create staging dataset (Dataflow + dbt stg_ output — Day 3 & 4)
bq mk \
  --project_id=$PROJECT_ID \
  --location=$REGION \
  --dataset \
  --description="Cleaned and enriched data from Dataflow and dbt staging models" \
  ${PROJECT_ID}:${DATASET_STAGING}

# Create mart dataset (business-ready dbt mart_ models — Day 4)
bq mk \
  --project_id=$PROJECT_ID \
  --location=$REGION \
  --dataset \
  --description="Business-ready aggregated tables from dbt mart models" \
  ${PROJECT_ID}:${DATASET_MART}

# Verify all 3 datasets exist
bq ls --project_id=$PROJECT_ID
```

---

## 2. Load All 9 Tables — Using the Script

```bash
# The automated way (recommended)
bash scripts/load_to_bq.sh

# This script:
#   1. Loads each CSV from GCS raw/ into BigQuery raw.*
#   2. Uses typed schemas from ingestion/bq_schemas/
#   3. Adds partitioning and clustering where appropriate
#   4. Prints row count after each table loads
```

---

## 3. Load Tables Manually (if you prefer step by step)

```bash
# --- Table 1: orders (partitioned by month, clustered by status) ---
bq load \
  --project_id=$PROJECT_ID \
  --location=$REGION \
  --source_format=CSV \
  --skip_leading_rows=1 \
  --allow_quoted_newlines \
  --allow_jagged_rows \
  --replace \
  --time_partitioning_field=order_purchase_timestamp \
  --time_partitioning_type=MONTH \
  --clustering_fields=order_status \
  ${DATASET_RAW}.orders \
  gs://${BUCKET_NAME}/raw/olist_orders_dataset.csv \
  ${SCHEMA_DIR}/orders_schema.json

# --- Table 2: order_items (clustered by seller_id, product_id) ---
bq load \
  --project_id=$PROJECT_ID \
  --location=$REGION \
  --source_format=CSV \
  --skip_leading_rows=1 \
  --allow_quoted_newlines \
  --replace \
  --clustering_fields=seller_id,product_id \
  ${DATASET_RAW}.order_items \
  gs://${BUCKET_NAME}/raw/olist_order_items_dataset.csv \
  ${SCHEMA_DIR}/order_items_schema.json

# --- Table 3: payments (clustered by payment_type) ---
bq load \
  --project_id=$PROJECT_ID \
  --location=$REGION \
  --source_format=CSV \
  --skip_leading_rows=1 \
  --replace \
  --clustering_fields=payment_type \
  ${DATASET_RAW}.payments \
  gs://${BUCKET_NAME}/raw/olist_order_payments_dataset.csv \
  ${SCHEMA_DIR}/payments_schema.json

# --- Table 4: reviews (partitioned by creation date, clustered by score) ---
bq load \
  --project_id=$PROJECT_ID \
  --location=$REGION \
  --source_format=CSV \
  --skip_leading_rows=1 \
  --allow_quoted_newlines \
  --replace \
  --time_partitioning_field=review_creation_date \
  --time_partitioning_type=MONTH \
  --clustering_fields=review_score \
  ${DATASET_RAW}.reviews \
  gs://${BUCKET_NAME}/raw/olist_order_reviews_dataset.csv \
  ${SCHEMA_DIR}/reviews_schema.json

# --- Table 5: customers (clustered by state) ---
bq load \
  --project_id=$PROJECT_ID \
  --location=$REGION \
  --source_format=CSV \
  --skip_leading_rows=1 \
  --replace \
  --clustering_fields=customer_state \
  ${DATASET_RAW}.customers \
  gs://${BUCKET_NAME}/raw/olist_customers_dataset.csv \
  ${SCHEMA_DIR}/customers_schema.json

# --- Table 6: products (clustered by category) ---
bq load \
  --project_id=$PROJECT_ID \
  --location=$REGION \
  --source_format=CSV \
  --skip_leading_rows=1 \
  --allow_quoted_newlines \
  --replace \
  --clustering_fields=product_category_name \
  ${DATASET_RAW}.products \
  gs://${BUCKET_NAME}/raw/olist_products_dataset.csv \
  ${SCHEMA_DIR}/products_schema.json

# --- Table 7: sellers (clustered by state) ---
bq load \
  --project_id=$PROJECT_ID \
  --location=$REGION \
  --source_format=CSV \
  --skip_leading_rows=1 \
  --replace \
  --clustering_fields=seller_state \
  ${DATASET_RAW}.sellers \
  gs://${BUCKET_NAME}/raw/olist_sellers_dataset.csv \
  ${SCHEMA_DIR}/sellers_schema.json

# --- Table 8: geolocation (clustered by state) ---
bq load \
  --project_id=$PROJECT_ID \
  --location=$REGION \
  --source_format=CSV \
  --skip_leading_rows=1 \
  --replace \
  --clustering_fields=geolocation_state \
  ${DATASET_RAW}.geolocation \
  gs://${BUCKET_NAME}/raw/olist_geolocation_dataset.csv \
  ${SCHEMA_DIR}/geolocation_schema.json

# --- Table 9: category_translation ---
bq load \
  --project_id=$PROJECT_ID \
  --location=$REGION \
  --source_format=CSV \
  --skip_leading_rows=1 \
  --replace \
  ${DATASET_RAW}.category_translation \
  gs://${BUCKET_NAME}/raw/product_category_name_translation.csv \
  ${SCHEMA_DIR}/category_translation_schema.json
```

---

## 4. Verify Row Counts

```bash
bq query \
  --project_id=$PROJECT_ID \
  --use_legacy_sql=false \
  --format=pretty \
"
SELECT table_name, row_count
FROM \`${PROJECT_ID}.raw.INFORMATION_SCHEMA.PARTITIONS\`
GROUP BY table_name, row_count
ORDER BY row_count DESC
"
```

Expected output:
```
+----------------------+-----------+
| table_name           | row_count |
+----------------------+-----------+
| geolocation          | 1000163   |
| reviews              |  104715   |
| payments             |  103886   |
| order_items          |  112650   |
| orders               |   99441   |
| customers            |   99441   |
| products             |   32951   |
| sellers              |    3095   |
| category_translation |      71   |
+----------------------+-----------+
```

---

## 5. Run Data Quality Validation Queries

```bash
# Run all 10 validation checks at once
bq query \
  --project_id=$PROJECT_ID \
  --use_legacy_sql=false \
  < ingestion/bq_validation_queries.sql

# OR run Python validation script (produces a full report)
pip install google-cloud-bigquery pandas tabulate
python ingestion/validate_raw_tables.py \
  --project_id=$PROJECT_ID \
  --dataset=raw
```

---

## 6. Check Table Schemas in BigQuery

```bash
# Inspect a specific table schema
bq show --schema --format=prettyjson ${PROJECT_ID}:raw.orders

# Show all tables in raw dataset
bq ls ${PROJECT_ID}:raw

# Show table info including partition and cluster details
bq show ${PROJECT_ID}:raw.orders
```

---

## 7. Explore Partitions (verify partitioning worked)

```bash
# Check partition count on orders table
bq query \
  --project_id=$PROJECT_ID \
  --use_legacy_sql=false \
"
SELECT
  partition_id,
  total_rows,
  total_logical_bytes
FROM \`${PROJECT_ID}.raw.INFORMATION_SCHEMA.PARTITIONS\`
WHERE table_name = 'orders'
ORDER BY partition_id
"

# Run a partition-pruned query (should say "This query will process 0 B" in BQ console)
bq query \
  --project_id=$PROJECT_ID \
  --use_legacy_sql=false \
  --dry_run \
"
SELECT order_id, order_status
FROM \`${PROJECT_ID}.raw.orders\`
WHERE order_purchase_timestamp BETWEEN '2018-01-01' AND '2018-03-31'
"
```

---

## 8. Git Commit Day 2

```bash
cd gcp-brazilian-ecommerce-pipeline

git add .
git status   # review what's being committed

git commit -m "feat: Day 2 — BigQuery raw layer load, schemas, validation queries"

git push origin main
```

---

## Common Errors and Fixes

| Error | Cause | Fix |
|-------|-------|-----|
| `Dataset already exists` | Dataset was created via Terraform on Day 1 | That's fine — `bq load` will still work |
| `Table not found` after `bq load` | Load failed silently | Check `bq ls PROJECT:raw` and rerun |
| `Could not coerce value to type TIMESTAMP` | CSV has bad date format | Add `--allow_jagged_rows --allow_quoted_newlines` |
| `Schema mismatch` | Old table has different schema | Add `--replace` flag to drop and recreate |
| `Quota exceeded` | Too many concurrent load jobs | Wait 60 sec and retry |
| `Access Denied` | SA missing bigquery.dataEditor | Re-run Day 1 IAM role grant |
