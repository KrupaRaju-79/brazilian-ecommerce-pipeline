# Architecture Documentation

## Pipeline Flow

```
DATA SOURCES
9 CSV files · 1.5M rows · Olist Brazilian E-Commerce
      |
      v
GOOGLE CLOUD STORAGE (Data Lake)
  gs://project-data-lake/
  ├── raw/          ← CSV files land here (Day 1)
  ├── staging/      ← Dataflow output (Day 3)
  └── processed/    ← Archived after BQ load
  Lifecycle: raw/ → Nearline(30d) → Coldline(90d)
      |
      v (GCS Object notification)
CLOUD PUB/SUB (Event bus)
  Topic: new-file-uploaded
  Pattern: At-least-once delivery
      |
      v
DATAFLOW (Apache Beam — Python SDK)
  · Read 9 CSVs from GCS
  · Type casting and null handling
  · Join orders + items + payments
  · Compute delivery_delay_days
  · Translate product categories PT → EN
  · Write to BigQuery staging layer
      |
      v
BIGQUERY (3-Layer Architecture)
  raw.*     ← Typed loads from GCS
  staging.* ← Cleaned tables from Dataflow
  mart.*    ← Business models from dbt
  
  mart_order_kpis | mart_seller_performance
  mart_customer_lifetime_value | mart_geo_sales
  mart_delivery_performance
  
  Partitioned on order_purchase_timestamp
  Clustered on customer_state, product_category
      |
      v
LOOKER STUDIO (Dashboard)
  Page 1: Order trends (GMV, AOV, count by month)
  Page 2: Seller KPIs (revenue, reviews, fulfilment)
  Page 3: Customer geo map (by Brazilian state)
  Page 4: Review scores and delivery performance

ORCHESTRATION: Cloud Composer (Apache Airflow 2)
  DAG: brazilian_ecommerce_pipeline
  Tasks: gcs_sensor → dataflow_job → dbt_run → notify

INFRASTRUCTURE: Terraform + GitHub Actions CI/CD
```
