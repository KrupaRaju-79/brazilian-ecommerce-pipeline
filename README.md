# 🇧🇷 Brazilian E-Commerce — End-to-End GCP Data Pipeline

> **Production-grade data pipeline** built on Google Cloud Platform, processing 99,000+ orders across 8 relational datasets from Brazil's largest e-commerce marketplace (Olist).

---

## 📐 Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SOURCE DATA (9 CSV files)                    │
│          Olist Brazilian E-Commerce Dataset — ~1.5M rows            │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ gsutil cp / upload
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│               GOOGLE CLOUD STORAGE (GCS)                            │
│   gs://[project]-raw/        gs://[project]-staging/               │
│   └── olist_orders.csv       └── transformed parquet files         │
│   └── olist_customers.csv                                           │
│   └── ... (9 files)          gs://[project]-processed/             │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ Pub/Sub event trigger
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│               CLOUD PUB/SUB (Event Trigger)                         │
│   Topic: new-file-uploaded                                          │
│   Subscription: dataflow-trigger-sub                                │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ Apache Beam pipeline
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│               GOOGLE CLOUD DATAFLOW                                 │
│   Apache Beam Python SDK                                            │
│   • Null handling & type casting                                    │
│   • Multi-table joins (order_id, product_id, seller_id)            │
│   • Delivery delay computation                                      │
│   • Category name translation (PT → EN)                            │
│   • Geo enrichment from zip_code_prefix                            │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ Write to BigQuery
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│               GOOGLE BIGQUERY (3-Layer Warehouse)                   │
│                                                                     │
│   raw.*          staging.*         mart.*                           │
│   ─────────      ───────────       ──────────                      │
│   raw loads  →   Beam output  →    dbt models                      │
│   typed CSV      cleaned rows      business KPIs                   │
│                                                                     │
│   Partitioned: order_purchase_timestamp                             │
│   Clustered: customer_state, product_category                      │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ dbt transformation
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│               DBT CLOUD (Data Modelling)                            │
│   staging models → mart models → documented & tested               │
│   mart_order_kpis | mart_seller_performance                        │
│   mart_customer_ltv | mart_geo_sales | mart_delivery               │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ BI Engine connection
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│               LOOKER STUDIO (Dashboard)                             │
│   4 report pages: Order Trends | Seller KPIs | Geo Map | Reviews   │
└─────────────────────────────────────────────────────────────────────┘

                    ↕  Orchestrated by  ↕
┌─────────────────────────────────────────────────────────────────────┐
│   CLOUD COMPOSER 2 (Apache Airflow 2)                               │
│   DAG: gcs_sensor → dataflow_job → dbt_run → notify                │
└─────────────────────────────────────────────────────────────────────┘

                    ↕  Provisioned by  ↕
┌─────────────────────────────────────────────────────────────────────┐
│   TERRAFORM + GITHUB ACTIONS (IaC & CI/CD)                         │
│   terraform apply  |  dbt test on PR  |  lint on push              │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Repository Structure

```
gcp-brazilian-pipeline/
│
├── README.md                          ← You are here
├── docs/
│   ├── DAY1_PROCESS.md               ← Day 1 detailed process log
│   ├── DAY2_PROCESS.md               ← Day 2 detailed process log
│   ├── ... (one per day)
│   ├── ALL_COMMANDS.md               ← Every command used in the project
│   └── DATA_SCHEMA.md                ← Full schema documentation
│
├── src/
│   ├── beam/                         ← Apache Beam / Dataflow pipelines
│   │   ├── pipeline.py
│   │   └── transforms/
│   ├── dbt/                          ← dbt models (staging + mart)
│   │   ├── models/
│   │   ├── tests/
│   │   └── dbt_project.yml
│   ├── airflow/                      ← Cloud Composer DAGs
│   │   └── dags/
│   └── terraform/                   ← Infrastructure as Code
│       ├── main.tf
│       ├── variables.tf
│       └── outputs.tf
│
├── scripts/
│   └── setup_gcp.sh                 ← Day 1 setup script
│
└── .github/
    └── workflows/
        ├── dbt_test.yml             ← Run dbt tests on PR
        └── terraform_plan.yml      ← Terraform plan on push
```

---

## 📊 Dataset Overview

| Table | Rows | Key Column | Description |
|-------|------|-----------|-------------|
| olist_orders_dataset | 99,441 | order_id (PK) | All orders with status and timestamps |
| olist_customers_dataset | 99,441 | customer_id (PK) | Customer location data |
| olist_order_items_dataset | 112,650 | order_id (FK) | Line items, price, freight |
| olist_order_payments_dataset | 103,886 | order_id (FK) | Payment type and installments |
| olist_order_reviews_dataset | 104,715 | order_id (FK) | Customer review scores |
| olist_products_dataset | 32,951 | product_id (PK) | Product categories and dimensions |
| olist_sellers_dataset | 3,095 | seller_id (PK) | Seller location |
| olist_geolocation_dataset | 1,000,163 | zip_code_prefix | Lat/lng for every zip prefix |

---

## 🛠️ Tech Stack

| Layer | Tool | Purpose |
|-------|------|---------|
| Cloud Platform | Google Cloud Platform | All infrastructure |
| Object Storage | Google Cloud Storage | Data lake (raw/staging/processed) |
| Messaging | Google Cloud Pub/Sub | Event-driven file triggers |
| Processing | Apache Beam + Dataflow | Distributed transformations |
| Warehouse | BigQuery | Analytical data warehouse |
| Modelling | dbt (data build tool) | SQL transformations & testing |
| Orchestration | Cloud Composer 2 / Airflow 2 | Pipeline scheduling |
| BI | Looker Studio + BI Engine | Dashboards |
| IaC | Terraform | GCP resource provisioning |
| CI/CD | GitHub Actions | Automated testing & deployment |
| Language | Python 3.11 | Beam pipelines, DAGs, scripts |

---

## 🚀 Quick Start

### Prerequisites
- GCP account with billing enabled
- `gcloud` CLI installed and authenticated
- Python 3.11+
- Terraform >= 1.5
- dbt-bigquery

### 1. Clone and configure
```bash
git clone https://github.com/YOUR_USERNAME/gcp-brazilian-pipeline.git
cd gcp-brazilian-pipeline
cp .env.example .env
# Edit .env with your GCP project ID
```

### 2. Run Day 1 setup
```bash
chmod +x scripts/setup_gcp.sh
./scripts/setup_gcp.sh
```

### 3. Follow the day-by-day guides
See `docs/DAY1_PROCESS.md` through `docs/DAY7_PROCESS.md`

---

## 📈 KPIs Delivered

- **Order Volume Trends** — daily/weekly/monthly order counts
- **Revenue by State** — geographic revenue distribution across Brazil
- **Seller Performance** — GMV, avg review score, on-time delivery rate per seller
- **Delivery Performance** — actual vs estimated delivery time by region and category
- **Customer Lifetime Value** — repeat purchase rate, avg order value per customer
- **Payment Analysis** — credit card vs boleto vs voucher split, installment distribution
- **Review Score Analysis** — score distribution, correlation with delivery delay

---

## 👨‍💻 Author

Built as a portfolio data engineering project demonstrating end-to-end GCP pipeline design.

**Skills demonstrated:** GCP · BigQuery · Apache Beam · Dataflow · dbt · Apache Airflow · Cloud Composer · Terraform · GitHub Actions · Python · SQL · Looker Studio
