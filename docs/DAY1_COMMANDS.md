# Day 1 — All Commands Reference

> Copy-paste ready. Every command you run on Day 1, in order.
> Replace `YOUR_PROJECT_ID`, `YOUR_REGION`, `YOUR_BUCKET_NAME` with your actual values.

---

## 0. Variables — set these once in your terminal

```bash
export PROJECT_ID="brazilian-ecommerce-pipeline"     # your GCP project ID
export REGION="us-central1"                           # GCP region
export ZONE="us-central1-a"
export BUCKET_NAME="${PROJECT_ID}-data-lake"          # GCS bucket name
export BQ_DATASET_RAW="raw"
export BQ_DATASET_STAGING="staging"
export BQ_DATASET_MART="mart"
export SA_NAME="pipeline-sa"                          # service account name
export DATA_DIR="./data"                              # local folder with your CSVs
```

---

## 1. Install Google Cloud SDK

```bash
# macOS (Homebrew)
brew install --cask google-cloud-sdk

# Ubuntu/Debian
curl https://sdk.cloud.google.com | bash
exec -l $SHELL

# Verify install
gcloud version
```

---

## 2. Authenticate with GCP

```bash
# Login with your Google account
gcloud auth login

# Set application default credentials (needed for Python SDK / Terraform)
gcloud auth application-default login

# Verify you're logged in
gcloud auth list
```

---

## 3. Create GCP Project

```bash
# Create the project
gcloud projects create $PROJECT_ID --name="Brazilian Ecommerce Pipeline"

# Set it as the active project
gcloud config set project $PROJECT_ID

# Verify
gcloud config get-value project

# Link billing account (REQUIRED — Dataflow and Composer need billing enabled)
# List your billing accounts first
gcloud billing accounts list

# Link billing (replace BILLING_ACCOUNT_ID with your actual ID e.g. 01ABCD-123456-789XYZ)
gcloud billing projects link $PROJECT_ID --billing-account=BILLING_ACCOUNT_ID
```

---

## 4. Enable Required APIs

```bash
gcloud services enable \
  storage.googleapis.com \
  bigquery.googleapis.com \
  dataflow.googleapis.com \
  pubsub.googleapis.com \
  composer.googleapis.com \
  cloudresourcemanager.googleapis.com \
  iam.googleapis.com \
  compute.googleapis.com \
  monitoring.googleapis.com \
  logging.googleapis.com

# Verify all APIs are enabled
gcloud services list --enabled --filter="name:(storage OR bigquery OR dataflow OR pubsub OR composer)"
```

---

## 5. Create Service Account + Roles

```bash
# Create service account
gcloud iam service-accounts create $SA_NAME \
  --display-name="Pipeline Service Account" \
  --description="Used by Dataflow, Composer, and dbt"

# Store full service account email
export SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

# Grant required roles
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/storage.objectAdmin"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/dataflow.worker"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/pubsub.editor"

gcloud auth application-default login

gcloud auth application-default print-access-token
Expected output:

ya29.a0AfH6SM...


echo "Service account created: $SA_EMAIL"

```

---

## 6. Create Cloud Storage Buckets

```bash
# Create main data lake bucket
gcloud storage buckets create gs://${BUCKET_NAME} \
  --project=$PROJECT_ID \
  --location=$REGION \
  --uniform-bucket-level-access \
  --default-storage-class=STANDARD

# Create Dataflow temp bucket (Dataflow requires its own bucket)
gcloud storage buckets create gs://${BUCKET_NAME}-dataflow-temp \
  --project=$PROJECT_ID \
  --location=$REGION \
  --uniform-bucket-level-access

# Create folder structure (GCS uses prefixes, not real folders)
echo "placeholder" | gcloud storage cp - gs://${BUCKET_NAME}/raw/.keep
echo "placeholder" | gcloud storage cp - gs://${BUCKET_NAME}/staging/.keep
echo "placeholder" | gcloud storage cp - gs://${BUCKET_NAME}/processed/.keep

# Grant service account access to buckets
gcloud storage buckets add-iam-policy-binding gs://${BUCKET_NAME} \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/storage.objectAdmin"

# Verify buckets exist
gcloud storage ls
```

---

## 7. Upload CSV Files to GCS (raw/)

```bash
# Option A: Upload all CSVs at once (recommended)
gcloud storage cp ${DATA_DIR}/*.csv gs://${BUCKET_NAME}/raw/

# Option B: Upload each file individually (if you want to verify each)
gcloud storage cp ${DATA_DIR}/olist_orders_dataset.csv          gs://${BUCKET_NAME}/raw/
gcloud storage cp ${DATA_DIR}/olist_order_items_dataset.csv     gs://${BUCKET_NAME}/raw/
gcloud storage cp ${DATA_DIR}/olist_order_payments_dataset.csv  gs://${BUCKET_NAME}/raw/
gcloud storage cp ${DATA_DIR}/olist_order_reviews_dataset.csv   gs://${BUCKET_NAME}/raw/
gcloud storage cp ${DATA_DIR}/olist_customers_dataset.csv       gs://${BUCKET_NAME}/raw/
gcloud storage cp ${DATA_DIR}/olist_products_dataset.csv        gs://${BUCKET_NAME}/raw/
gcloud storage cp ${DATA_DIR}/olist_sellers_dataset.csv         gs://${BUCKET_NAME}/raw/
gcloud storage cp ${DATA_DIR}/olist_geolocation_dataset.csv     gs://${BUCKET_NAME}/raw/
gcloud storage cp ${DATA_DIR}/product_category_name_translation.csv gs://${BUCKET_NAME}/raw/

# Verify all files uploaded
gcloud storage ls gs://${BUCKET_NAME}/raw/

# Check file sizes to confirm nothing is truncated
gcloud storage du gs://${BUCKET_NAME}/raw/ --human-readable
```

---

## 8. Set Lifecycle Policy on Bucket (cost saving)

```bash
# Apply lifecycle policy — moves files to Nearline after 30 days, Coldline after 90
gcloud storage buckets update gs://${BUCKET_NAME} \
  --lifecycle-file=./infrastructure/gcs_lifecycle.json

# Verify policy applied
gcloud storage buckets describe gs://${BUCKET_NAME} --format="value(lifecycle)"
```

---

## 9. Set Up Git + GitHub

```bash
# Inside the project folder
cd gcp-brazilian-ecommerce-pipeline

git init
git add .

# IMPORTANT: Make sure credentials/ is in .gitignore before committing
grep "credentials/" .gitignore   # should print "credentials/"

git commit -m "feat: Day 1 — GCP project setup, IAM, Cloud Storage, raw CSV upload"

# Create repo on GitHub (if you have GitHub CLI)
gh repo create gcp-brazilian-ecommerce-pipeline --public --source=. --remote=origin --push

# Or manually: go to github.com → New Repository → then:
git remote add origin https://github.com/YOUR_USERNAME/gcp-brazilian-ecommerce-pipeline.git
git branch -M main
git push -u origin main
```

---

## 10. Verify Everything

```bash
# Check project
gcloud config get-value project

# Check APIs enabled
gcloud services list --enabled | grep -E "(storage|bigquery|dataflow|pubsub|composer)"

# Check service account exists
gcloud iam service-accounts list

# Check buckets
gcloud storage ls

# Check all 9 files in raw/
gcloud storage ls gs://${BUCKET_NAME}/raw/ | wc -l   # should print 11 (9 CSVs + 2 .keep files)

# Check IAM bindings on project
gcloud projects get-iam-policy $PROJECT_ID \
  --flatten="bindings[].members" \
  --filter="bindings.members:${SA_EMAIL}" \
  --format="table(bindings.role)"
```

---

## Common Errors and Fixes

| Error | Cause | Fix |
|-------|-------|-----|
| `BILLING_NOT_ENABLED` | Project has no billing account | Run step 3 billing link command |
| `permission denied on bucket` | SA missing storage role | Re-run step 5 role grant |
| `API not enabled` | API wasn't activated | Re-run step 4 enable command |
| `gsutil: command not found` | Old SDK — use `gcloud storage` instead | Update gcloud SDK |
| `credentials not found` | GOOGLE_APPLICATION_CREDENTIALS not set | Export the variable again |
