# Day 1 — Process Journal

**Date:** [Fill in your date]
**Goal:** GCP project setup, IAM configuration, Cloud Storage buckets, raw CSV upload
**Status:** ✅ Complete

---

## What I Did Today (Step by Step)

### Step 1: Installed Google Cloud SDK
Installed `gcloud` CLI on my machine. This is the main tool for managing all GCP resources from the terminal. Verified with `gcloud version`.

**Why this matters:** Every GCP action in this project can be done via the CLI — this makes it scriptable, repeatable, and GitOps-friendly. Real data engineering teams never click through the GCP console to manage resources.

---

### Step 2: Authenticated and Created GCP Project
- Ran `gcloud auth login` to authenticate with my Google account
- Created a new GCP project: `brazilian-ecommerce-pipeline`
- Linked it to a billing account (required for Dataflow, Composer, and sustained use)

**Why this matters:** Every GCP resource lives inside a project. Billing must be enabled even if you're using free tier — many APIs gate on it.

**Cost note:** Day 1 costs are essentially zero. Cloud Storage is $0.02/GB/month and our dataset is ~200MB. BigQuery and Dataflow are not touched today.

---

### Step 3: Enabled 10 APIs
```
storage, bigquery, dataflow, pubsub, composer,
cloudresourcemanager, iam, compute, monitoring, logging
```

**Why enable all upfront?** API enablement can take 1–2 minutes each and some tools (e.g., Composer) require compute and monitoring to already be enabled. Doing this on Day 1 avoids blockers on Days 3 and 6.

---

### Step 4: Created a Service Account
Created `pipeline-sa` — a non-human identity that the pipeline code uses to authenticate with GCP.

**Roles granted:**
| Role | Why |
|------|-----|
| `roles/bigquery.dataEditor` | Read and write BQ tables |
| `roles/bigquery.jobUser` | Run BQ queries and load jobs |
| `roles/storage.objectAdmin` | Read/write/delete GCS objects |
| `roles/dataflow.worker` | Dataflow pipeline execution |
| `roles/pubsub.editor` | Publish and subscribe to topics |

**Key learning — Principle of Least Privilege:** Never use `roles/owner` or `roles/editor` for a pipeline service account. If the key is ever leaked, blast radius is limited to only what the pipeline actually needs.

Downloaded the JSON key to `credentials/pipeline-sa-key.json` and immediately added `credentials/` to `.gitignore`.

⚠️ **NEVER commit the JSON key to git.** This is the most common security mistake in GCP projects.

---

### Step 5: Created Cloud Storage Buckets

Created two buckets:
- `brazilian-ecommerce-pipeline-data-lake` — main data lake
- `brazilian-ecommerce-pipeline-dataflow-temp` — Dataflow staging (required by Dataflow runner)

**Bucket structure (prefixes = virtual folders):**
```
gs://brazilian-ecommerce-pipeline-data-lake/
├── raw/          ← Source CSV files land here
├── staging/      ← Dataflow output goes here (Day 3)
└── processed/    ← Archived files after BQ load
```

**Settings used:**
- `--uniform-bucket-level-access`: Disables per-object ACLs. All access managed via IAM. Required for security best practice.
- `--default-storage-class=STANDARD`: Hot storage for active pipeline data.
- `--location=us-central1`: Same region as Dataflow and Composer to avoid egress costs.

**Key learning — Region matters for cost:** Dataflow reads from GCS. If your bucket is in a different region than your Dataflow job, you pay inter-region egress. Always co-locate compute and storage.

---

### Step 6: Applied Lifecycle Policy
Added a lifecycle rule so files in `raw/` automatically transition to:
- **Nearline storage** after 30 days ($0.01/GB/month vs $0.02)
- **Coldline storage** after 90 days ($0.004/GB/month)

This is real cost engineering — on large datasets this can save 80% on storage costs.

---

### Step 7: Uploaded All 9 CSV Files

Uploaded all CSVs to `gs://.../raw/` using `gcloud storage cp`.

**File sizes uploaded:**
```
olist_customers_dataset.csv          →  6.8 MB
olist_geolocation_dataset.csv        → 65.1 MB
olist_order_items_dataset.csv        →  7.5 MB
olist_order_payments_dataset.csv     →  5.3 MB
olist_order_reviews_dataset.csv      → 19.2 MB
olist_orders_dataset.csv             →  6.8 MB
olist_products_dataset.csv           →  3.4 MB
olist_sellers_dataset.csv            →  0.3 MB
product_category_name_translation.csv → 0.004 MB
Total:                               ~ 114 MB
```

**Verified with:** `gcloud storage ls gs://.../raw/` and `gcloud storage du` for sizes.

---

### Step 8: Initialized Git Repository + Pushed to GitHub

```bash
git init
git add .
git commit -m "feat: Day 1 — GCP project setup, IAM, Cloud Storage, raw CSV upload"
git push -u origin main
```

**Files committed:**
- `README.md` — full project description and architecture
- `docs/DAY1_COMMANDS.md` — every command run today
- `docs/DAY1_PROCESS.md` — this file
- `docs/DATA_DICTIONARY.md` — column definitions
- `scripts/upload_to_gcs.sh` — automated upload script
- `infrastructure/gcs_lifecycle.json` — bucket lifecycle policy
- `.gitignore` — excludes credentials, Python venvs, etc.

---

## What I Learned Today

1. **GCP project setup is not trivial** — billing, APIs, IAM, and service accounts all need to be wired correctly before any pipeline code runs. Getting this right on Day 1 prevents wasted hours on Days 3–6.

2. **Service accounts are the identity layer** — the pipeline never runs "as you". It runs as a service account with scoped permissions. This is the correct pattern in every production GCP environment.

3. **GCS buckets are globally unique** — bucket names must be unique across all of GCP, not just your project. Use your project ID as a prefix to guarantee uniqueness.

4. **`gcloud storage` vs `gsutil`** — `gsutil` is the old tool, `gcloud storage` is the new recommended CLI. Both work but `gcloud storage` is faster for parallel uploads.

5. **`.gitignore` is a security control** — adding `credentials/` to `.gitignore` before the first commit is critical. If you commit a service account key even once, you should rotate it immediately.

---

## Blockers and How I Resolved Them

| Blocker | Resolution |
|---------|-----------|
| API not enabled error | Ran the bulk API enable command again, waited 2 min |
| Bucket name already taken | Added project ID as prefix to guarantee uniqueness |
| Billing not linked | Found billing account ID in GCP console → Billing section |

---

## Day 2 Preview

Tomorrow I will:
1. Create BigQuery datasets: `raw`, `staging`, `mart`
2. Define typed schemas for all 8 tables in JSON
3. Run `bq load` to load all 9 CSVs from GCS into `raw.*` tables
4. Add partitioning on `order_purchase_timestamp` and clustering on `customer_state`
5. Run basic SQL validation queries to verify row counts and data quality

**Key concept for Day 2:** BigQuery partitioning cuts query costs dramatically. A query scanning 1 year of data can be 12x cheaper if the table is partitioned by month.
