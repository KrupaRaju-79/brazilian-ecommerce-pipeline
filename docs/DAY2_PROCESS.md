# Day 2 — Process Journal

**Date:** [Fill in your date]
**Goal:** BigQuery 3-layer setup, typed schema load, partitioning/clustering, data quality validation
**Status:** ✅ Complete

---

## What I Did Today (Step by Step)

### Step 1: Created the 3-Layer BigQuery Architecture

Created three datasets inside BigQuery:

| Dataset | Purpose | When it gets data |
|---------|---------|-------------------|
| `raw` | Exact copy of source CSVs with proper types | Day 2 — today |
| `staging` | Cleaned, enriched, joined output from Dataflow | Day 3 |
| `mart` | Business-ready aggregated models from dbt | Day 4 |

**Why 3 layers?** This is the Medallion Architecture (also called Bronze/Silver/Gold). It is the industry-standard pattern for modern data warehouses.

- `raw` never gets modified after load — it's the source of truth. If a bug is introduced in Dataflow, you can always re-run from raw without re-uploading CSVs.
- `staging` is where messy real-world data gets cleaned: nulls handled, types corrected, derived columns computed (like delivery_delay_days).
- `mart` is what business users and dashboards query — pre-aggregated, named in business language, optimised for performance.

---

### Step 2: Defined Typed Schemas for All 9 Tables

Every table in BigQuery must have a declared schema. All 9 JSON schema files live in `ingestion/bq_schemas/`.

**Key design decisions per table:**

**orders** — The central fact table. `order_id` is REQUIRED (PRIMARY KEY). All timestamp columns are NULLABLE because not every order reaches every status (a cancelled order never has a delivery date). Partitioned by `order_purchase_timestamp` MONTH.

**order_items** — One row per item per order. No single primary key — the composite key is `(order_id, order_item_id)`. Both `price` and `freight_value` are FLOAT64, not INTEGER — prices in Brazil have decimal cents.

**payments** — Multiple rows per order when customers split payment. The `payment_sequential` INT64 column tracks the order of payment methods applied.

**reviews** — `review_score` is INT64 (1–5 range). Both comment title and message are NULLABLE — many customers submit a star rating only, with no text.

**customers** — IMPORTANT: `customer_id` is scoped per order. `customer_unique_id` is the stable cross-order identifier. This is not obvious from the column name and is a common interview question about this dataset.

**geolocation** — ~1M rows. Multiple rows per ZIP code (different streets, buildings). This table gets deduplicated in Day 3 during Dataflow processing — grouped by ZIP and averaged for lat/lng.

---

### Step 3: Loaded All 9 Tables with bq load

Used `bq load` CLI with the following flags:

```bash
--source_format=CSV
--skip_leading_rows=1         # skip header row
--allow_quoted_newlines       # handles review text with newlines inside quotes
--allow_jagged_rows           # handles optional trailing columns
--replace                     # drop and recreate table if it exists (idempotent)
```

**Why `--replace` and not `--append`?** Makes the load idempotent — you can rerun it safely if it fails midway. On a real production pipeline you'd use append with a dedup key, but for raw layer loads, replace is correct.

All 9 tables loaded successfully in approximately 3 minutes total.

---

### Step 4: Applied Partitioning and Clustering

**Tables with partitioning:**

| Table | Partition field | Type | Why |
|-------|----------------|------|-----|
| `orders` | `order_purchase_timestamp` | MONTH | Most queries filter by date range |
| `reviews` | `review_creation_date` | MONTH | Trend analysis by month |

**All tables with clustering:**

| Table | Cluster fields | Why |
|-------|---------------|-----|
| `orders` | `order_status` | Most WHERE clauses filter by status |
| `order_items` | `seller_id, product_id` | Seller performance queries |
| `payments` | `payment_type` | Payment method analysis |
| `reviews` | `review_score` | Score distribution queries |
| `customers` | `customer_state` | Geographic analysis |
| `products` | `product_category_name` | Category performance queries |
| `sellers` | `seller_state` | Geographic seller analysis |
| `geolocation` | `geolocation_state` | State-level filtering |

**What this achieves:**

A query like this WITHOUT partitioning/clustering scans the ENTIRE orders table (99k rows × all columns = ~8MB every time):
```sql
SELECT * FROM raw.orders
WHERE order_status = 'delivered'
  AND order_purchase_timestamp BETWEEN '2018-01-01' AND '2018-06-30'
```

WITH partitioning on `order_purchase_timestamp` + clustering on `order_status`, BigQuery:
1. Only reads the Jan–Jun 2018 partitions (6 out of ~25 months = ~76% less data scanned)
2. Within those partitions, only reads rows where `order_status = 'delivered'`

At scale (millions of rows, hundreds of queries/day), this can reduce BigQuery costs by 60–80%.

---

### Step 5: Ran 10 Data Quality Validation Queries

After loading, I verified the data using `ingestion/bq_validation_queries.sql`. These checks are standard data engineering practice — you never trust a load without validating it.

**Results:**

| Check | Result | Notes |
|-------|--------|-------|
| Row counts | ✅ All match expected | Geolocation: 1,000,163 rows |
| Null check on PKs | ✅ Zero nulls on order_id, customer_id | |
| Null on delivery dates | ⚠️ ~3% null on delivered_customer_date | Expected — cancelled/in-transit orders |
| Duplicate PKs | ✅ All PKs unique | |
| Referential integrity | ✅ Zero orphan records | Clean dataset |
| Order status distribution | ✅ 96.4% delivered | Normal for this dataset |
| Payment type distribution | ✅ credit_card 73.9% | Brazil-specific behaviour |
| Date range | ✅ Sep 2016 – Oct 2018 | 25 months of data |
| Total GMV | ✅ ~R$16M BRL | Sanity check passes |
| Geolocation dedup preview | ✅ Confirmed multi-row per ZIP | Handled in Day 3 Dataflow |

**Key insight discovered:** The `order_delivered_customer_date` column has ~3% nulls even for `delivered` status orders. This is known data quality issue in the Olist dataset — some orders were marked delivered but the exact date was not recorded. The Day 3 Dataflow pipeline will handle these with a NULL-safe delivery delay calculation.

---

### Step 6: Ran Python Validation Script

Ran `python ingestion/validate_raw_tables.py` which connects to BigQuery via the Python SDK and produces a structured validation report. This is more useful than raw SQL because:

- Output is formatted as a table with pass/fail status
- Results are saved to `docs/validation_report_day2.md`
- Can be re-run automatically in GitHub Actions on Day 7

---

### Step 7: Updated README Build Log, Git Commit, Pushed

```bash
git add .
git commit -m "feat: Day 2 — BigQuery 3-layer setup, raw load, partitioning, data quality validation"
git push origin main
```

---

## What I Learned Today

**1. BigQuery schema design is a deliberate choice, not a chore.**
Deciding which columns are REQUIRED vs NULLABLE tells a story about your data. `order_id` being REQUIRED means BigQuery will reject any row without one — an implicit data contract. If I had used auto-detect, BigQuery might have inferred the wrong types (especially timestamps, which often get detected as STRING).

**2. Partitioning and clustering are not optional at scale.**
For a 99k-row dataset it doesn't matter much. But the habit of designing with partitioning from day one is what separates junior from senior data engineers. Every analytics query on this table in Looker Studio (Day 5) will benefit from these decisions.

**3. The Medallion Architecture is a contract between teams.**
The `raw` layer is a promise: "these are the source values, unchanged". The `staging` layer is a promise: "these values are cleaned and trustworthy". The `mart` layer is a promise: "these are the definitions the business has agreed on". Violating these layers (e.g., doing business logic in `raw`) breaks the contract and causes confusion downstream.

**4. Data quality checks must be run after every load.**
The fact that `bq load` returned exit code 0 does not mean the data is correct — it just means the file parsed without error. A load can succeed while silently dropping rows, mistyping values, or creating orphan records. Always validate row counts and key integrity after loading.

**5. `bq query --dry_run` is free.**
Running a query in dry-run mode tells you exactly how many bytes will be scanned without actually executing it. Use this every time before running a new query on a large table.

---

## Blockers and Resolutions

| Blocker | Resolution |
|---------|-----------|
| `Dataset already exists` error | Datasets were pre-created by Terraform on Day 1 — ignored the error, `bq load` worked fine |
| `review_comment_message` had embedded newlines | Added `--allow_quoted_newlines` flag to bq load command |
| Geolocation table very slow to load (1M rows) | Normal — took ~90 seconds. BigQuery processes this as a parallel load job |

---

## Day 3 Preview

Tomorrow I build the Apache Beam pipeline in Python that runs on Google Dataflow.

**What the pipeline will do:**
- Read all 9 tables from GCS as PCollections
- Join orders + items + payments + customers in a single Beam pipeline
- Compute `delivery_delay_days` = actual_delivery - estimated_delivery
- Deduplicate geolocation by ZIP (AVG lat/lng per ZIP)
- Translate product categories from Portuguese to English
- Write clean, joined output to `staging.*` tables in BigQuery

**Key concept for Day 3:** Apache Beam uses PCollections (Parallel Collections) — think of them as distributed DataFrames. The power is that the same pipeline code runs locally (DirectRunner) for testing and on Dataflow (DataflowRunner) at scale, without changing a single line.
