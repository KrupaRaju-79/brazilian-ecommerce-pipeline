# 🇧🇷 Brazilian E-Commerce — GCP End-to-End Data Pipeline

[![dbt CI](https://github.com/YOUR_USERNAME/gcp-brazilian-ecommerce-pipeline/actions/workflows/dbt_ci.yml/badge.svg)](https://github.com/YOUR_USERNAME/gcp-brazilian-ecommerce-pipeline/actions)
[![Terraform](https://github.com/YOUR_USERNAME/gcp-brazilian-ecommerce-pipeline/actions/workflows/terraform_plan.yml/badge.svg)](https://github.com/YOUR_USERNAME/gcp-brazilian-ecommerce-pipeline/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> **End-to-end production-grade data pipeline on Google Cloud Platform**  
> Ingests 1.5M+ rows across 8 relational tables from Brazil's largest e-commerce marketplace (Olist), transforms via Apache Beam / Dataflow, models with dbt, orchestrates with Apache Airflow on Cloud Composer, and surfaces insights through a live Looker Studio dashboard.

---

## 📐 Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SOURCE LAYER                                 │
│   9 CSV files · 99k+ orders · 8 relational tables · 1.5M+ rows     │
└───────────────────────────┬─────────────────────────────────────────┘
                            │ gsutil cp / gcloud storage
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    CLOUD STORAGE (GCS)                              │
│   gs://[project]-raw/      → source CSVs (immutable)               │
│   gs://[project]-staging/  → Dataflow intermediate output           │
│   gs://[project]-processed/→ archived after load                   │
└──────────────┬──────────────────────────────┬───────────────────────┘
               │ GCS notification             │ bq load (Day 2)
               ▼                              ▼
┌──────────────────────┐      ┌───────────────────────────────────────┐
│    CLOUD PUB/SUB     │      │         BIGQUERY — RAW LAYER          │
│  Topic: new-file     │      │   dataset: raw                        │
│  Subscription:       │      │   8 tables · typed schemas            │
│  dataflow-trigger    │      │   partitioned + clustered             │
└──────────┬───────────┘      └───────────────────────────────────────┘
           │ trigger                        ▲
           ▼                               │ write
┌─────────────────────────────────────────┐│
│    DATAFLOW (Apache Beam Python SDK)    ││
│                                         ││
│  ┌──────────┐  ┌──────────┐  ┌───────┐ ││
│  │ Read GCS │→ │Transform │→ │ Write ├─┘│
│  └──────────┘  │ & Enrich │  │  BQ   │  │
│                └──────────┘  └───────┘  │
│  · Null handling & type casting         │
│  · Delivery delay computation           │
│  · Price enrichment & join              │
│  · Category translation (EN)            │
└─────────────────────────────────────────┘
                            │ models read from staging.*
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   dbt — STAGING + MART LAYER                        │
│                                                                     │
│  staging.*          →      mart.*                                   │
│  stg_orders              mart_order_kpis                            │
│  stg_customers           mart_seller_performance                    │
│  stg_payments            mart_customer_ltv                          │
│  stg_order_items         mart_delivery_performance                  │
│  stg_reviews             mart_geo_sales                             │
│  stg_products                                                       │
│  stg_sellers                                                        │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ connects to mart.*
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    LOOKER STUDIO DASHBOARD                          │
│   BI Engine accelerated · 4 report pages · live refresh             │
└─────────────────────────────────────────────────────────────────────┘

        ALL ABOVE ORCHESTRATED BY CLOUD COMPOSER (Apache Airflow 2)
        ALL INFRASTRUCTURE PROVISIONED BY TERRAFORM
        ALL DEPLOYMENTS AUTOMATED BY GITHUB ACTIONS CI/CD
```

---

## 🗂️ Repository Structure

```
gcp-brazilian-ecommerce-pipeline/
│
├── README.md                          ← You are here
├── COMMANDS.md                        ← Every CLI command used in this project
├── PROCESS_LOG.md                     ← Day-by-day build journal
├── LICENSE
│
├── docs/
│   ├── architecture.md                ← Detailed architecture decisions
│   ├── data_dictionary.md             ← All tables, columns, types, descriptions
│   ├── kpi_definitions.md             ← 5 KPIs: business question + SQL
│   └── setup_guide.md                 ← Step-by-step GCP setup for new devs
│
├── infrastructure/
│   └── terraform/
│       ├── main.tf                    ← GCS buckets, BQ datasets, Composer env
│       ├── variables.tf
│       ├── outputs.tf
│       └── terraform.tfvars.example
│
├── ingestion/
│   ├── upload_to_gcs.sh               ← Shell script to upload CSVs to GCS
│   └── bq_schemas/                    ← JSON schema files for all 8 BQ tables
│       ├── orders_schema.json
│       ├── customers_schema.json
│       ├── order_items_schema.json
│       ├── payments_schema.json
│       ├── reviews_schema.json
│       ├── products_schema.json
│       ├── sellers_schema.json
│       └── geolocation_schema.json
│
├── transformation/
│   ├── dataflow/
│   │   ├── pipeline.py                ← Apache Beam pipeline (main)
│   │   ├── transforms.py              ← Custom transform functions
│   │   └── requirements.txt
│   └── dbt/
│       ├── dbt_project.yml
│       ├── profiles.yml.example
│       ├── packages.yml
│       ├── models/
│       │   ├── staging/
│       │   │   ├── _staging.yml       ← Sources + tests
│       │   │   ├── stg_orders.sql
│       │   │   ├── stg_customers.sql
│       │   │   ├── stg_order_items.sql
│       │   │   ├── stg_payments.sql
│       │   │   ├── stg_reviews.sql
│       │   │   ├── stg_products.sql
│       │   │   └── stg_sellers.sql
│       │   └── mart/
│       │       ├── _mart.yml          ← Mart model documentation
│       │       ├── mart_order_kpis.sql
│       │       ├── mart_seller_performance.sql
│       │       ├── mart_customer_ltv.sql
│       │       ├── mart_delivery_performance.sql
│       │       └── mart_geo_sales.sql
│       ├── tests/
│       │   └── assert_positive_prices.sql
│       └── macros/
│           └── generate_schema_name.sql
│
├── orchestration/
│   └── dags/
│       └── brazilian_ecommerce_pipeline.py   ← Airflow DAG
│
├── dashboard/
│   └── looker_studio_setup.md         ← Dashboard config guide
│
├── scripts/
│   ├── create_pubsub.sh               ← Create Pub/Sub topic + subscription
│   ├── enable_apis.sh                 ← Enable all required GCP APIs
│   └── run_dataflow_local.sh          ← Test pipeline locally with DirectRunner
│
└── .github/
    └── workflows/
        ├── dbt_ci.yml                 ← Run dbt test on every PR
        └── terraform_plan.yml         ← Terraform plan on every PR
```

---

## 📦 Dataset

| Table | Rows | Description |
|---|---|---|
| olist_orders_dataset | 99,441 | Master order records with status and timestamps |
| olist_customers_dataset | 99,441 | Customer location and unique IDs |
| olist_order_items_dataset | 112,650 | Line items: product, seller, price, freight |
| olist_order_payments_dataset | 103,886 | Payment type, installments, value |
| olist_order_reviews_dataset | 104,715 | Review scores and comments |
| olist_products_dataset | 32,951 | Product metadata and dimensions |
| olist_sellers_dataset | 3,095 | Seller location |
| olist_geolocation_dataset | 1,000,163 | Zip code → lat/lng mapping |

**Source:** [Olist Brazilian E-Commerce on Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

---

## 🎯 KPIs Implemented

| # | KPI | Business Question |
|---|---|---|
| 1 | Total Revenue by Product Category | Which categories drive the most revenue? |
| 2 | Average Review Score by Seller State | Which regions have the best seller performance? |
| 3 | On-Time Delivery Rate | What % of orders arrive on or before estimated date? |
| 4 | Average Order Value by Payment Type | Which payment methods correlate with higher spend? |
| 5 | Monthly Active Customers (MAC) | How is customer engagement trending month-over-month? |

---

## 🛠️ Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Infrastructure | Terraform | Provision all GCP resources as code |
| Storage | Google Cloud Storage | Data lake (raw / staging / processed) |
| Messaging | Google Cloud Pub/Sub | Event-driven pipeline trigger |
| Processing | Apache Beam + Dataflow | Distributed data transformation |
| Warehouse | Google BigQuery | Columnar analytical store |
| Modelling | dbt (data build tool) | SQL transformation + testing + docs |
| Orchestration | Cloud Composer (Airflow 2) | Pipeline scheduling and monitoring |
| BI | Looker Studio + BI Engine | Live interactive dashboards |
| CI/CD | GitHub Actions | Automated testing and deployment |
| Language | Python 3.11, SQL | Pipeline + model code |

---

## 🚀 Quick Start

### Prerequisites
- GCP account with billing enabled
- `gcloud` CLI installed and authenticated
- Python 3.11+
- Terraform 1.6+
- dbt-bigquery

### 1. Clone and configure
```bash
git clone https://github.com/YOUR_USERNAME/gcp-brazilian-ecommerce-pipeline.git
cd gcp-brazilian-ecommerce-pipeline
export PROJECT_ID="your-gcp-project-id"
export REGION="us-central1"
```

### 2. Enable APIs and create infrastructure
```bash
bash scripts/enable_apis.sh
cd infrastructure/terraform
cp terraform.tfvars.example terraform.tfvars   # edit with your values
terraform init && terraform apply
```

### 3. Upload raw data to GCS
```bash
bash ingestion/upload_to_gcs.sh
```

### 4. Run Dataflow transformation
```bash
cd transformation/dataflow
pip install -r requirements.txt
python pipeline.py --runner DataflowRunner \
  --project $PROJECT_ID \
  --region $REGION \
  --temp_location gs://${PROJECT_ID}-staging/temp
```

### 5. Run dbt models
```bash
cd transformation/dbt
cp profiles.yml.example ~/.dbt/profiles.yml   # edit with your project
dbt deps && dbt build
```

---

## 📊 Dashboard

Live Looker Studio dashboard: **[Add your link after Day 5]**

Report pages:
- **Order trends** — monthly revenue, order volume, status breakdown
- **Seller KPIs** — performance by state, review scores, fulfilment rate
- **Customer map** — geographic distribution of customers by state
- **Review analysis** — score distribution, delivery correlation

---

## 📅 Build Journal

See [PROCESS_LOG.md](PROCESS_LOG.md) for a detailed day-by-day account of decisions made, issues encountered, and lessons learned throughout the build.

---

## 👤 Author

**[Your Name]**  
Data Engineer  
[LinkedIn](https://linkedin.com/in/yourprofile) · [GitHub](https://github.com/YOUR_USERNAME)

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.
