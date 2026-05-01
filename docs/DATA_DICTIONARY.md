# Data Dictionary

> Column-level definitions for all 9 source tables in the Olist Brazilian E-Commerce dataset.

---

## olist_orders_dataset

Central fact table. One row per order.

| Column | Type | Description |
|--------|------|-------------|
| `order_id` | STRING (PK) | Unique identifier for each order |
| `customer_id` | STRING (FK) | Links to olist_customers_dataset |
| `order_status` | STRING | delivered / shipped / canceled / invoiced / processing / unavailable / created / approved |
| `order_purchase_timestamp` | TIMESTAMP | When customer placed the order |
| `order_approved_at` | TIMESTAMP | When payment was approved |
| `order_delivered_carrier_date` | TIMESTAMP | When seller handed off to logistics carrier |
| `order_delivered_customer_date` | TIMESTAMP | Actual delivery date to customer |
| `order_estimated_delivery_date` | TIMESTAMP | Estimated delivery date shown at purchase |

**Derived metrics possible:**
- `delivery_delay_days` = `order_delivered_customer_date` - `order_estimated_delivery_date`
- `processing_time_hours` = `order_approved_at` - `order_purchase_timestamp`

---

## olist_order_items_dataset

Line-item level. One row per item within an order (orders can have multiple items).

| Column | Type | Description |
|--------|------|-------------|
| `order_id` | STRING (FK) | Links to olist_orders_dataset |
| `order_item_id` | INTEGER | Sequential item number within an order (1, 2, 3...) |
| `product_id` | STRING (FK) | Links to olist_products_dataset |
| `seller_id` | STRING (FK) | Links to olist_sellers_dataset |
| `shipping_limit_date` | TIMESTAMP | Deadline for seller to hand off to carrier |
| `price` | FLOAT | Item price paid by customer (BRL) |
| `freight_value` | FLOAT | Shipping cost for this item (BRL) |

**Derived metrics possible:**
- `total_order_value` = SUM(price + freight_value) GROUP BY order_id

---

## olist_order_payments_dataset

Payment records. Multiple rows per order if customer split payment across methods.

| Column | Type | Description |
|--------|------|-------------|
| `order_id` | STRING (FK) | Links to olist_orders_dataset |
| `payment_sequential` | INTEGER | Sequence when multiple payment methods used |
| `payment_type` | STRING | credit_card / boleto / voucher / debit_card |
| `payment_installments` | INTEGER | Number of installments (Brazil supports up to 24x) |
| `payment_value` | FLOAT | Amount paid via this payment method (BRL) |

**Key note:** Brazil has a unique payment culture — `boleto` is a bank slip, very common. Installments up to 24x are standard for larger purchases. This directly impacts revenue recognition timing.

---

## olist_order_reviews_dataset

Customer reviews. Not all orders have reviews.

| Column | Type | Description |
|--------|------|-------------|
| `review_id` | STRING (PK) | Unique review identifier |
| `order_id` | STRING (FK) | Links to olist_orders_dataset |
| `review_score` | INTEGER | 1–5 star rating |
| `review_comment_title` | STRING | Review headline (in Portuguese) |
| `review_comment_message` | STRING | Full review text (in Portuguese) |
| `review_creation_date` | TIMESTAMP | When the review form was sent to customer |
| `review_answer_timestamp` | TIMESTAMP | When customer submitted their review |

---

## olist_customers_dataset

Customer dimension. One row per customer_id (note: customer_id changes per order, customer_unique_id stays constant).

| Column | Type | Description |
|--------|------|-------------|
| `customer_id` | STRING (PK) | Order-scoped customer ID (changes each order) |
| `customer_unique_id` | STRING | True unique customer identifier across orders |
| `customer_zip_code_prefix` | STRING (FK) | 5-digit ZIP, links to olist_geolocation_dataset |
| `customer_city` | STRING | Customer city name |
| `customer_state` | STRING | 2-letter Brazilian state code (SP, RJ, MG...) |

**Key gotcha:** `customer_id` is NOT a stable customer identifier — it is scoped to a single order. Use `customer_unique_id` to track returning customers.

---

## olist_products_dataset

Product dimension. One row per product.

| Column | Type | Description |
|--------|------|-------------|
| `product_id` | STRING (PK) | Unique product identifier |
| `product_category_name` | STRING (FK) | Portuguese category name, join to translation table |
| `product_name_lenght` | INTEGER | Character count of product name (note: typo in source) |
| `product_description_lenght` | INTEGER | Character count of description (note: typo in source) |
| `product_photos_qty` | INTEGER | Number of product photos |
| `product_weight_g` | FLOAT | Weight in grams |
| `product_length_cm` | FLOAT | Length in centimetres |
| `product_height_cm` | FLOAT | Height in centimetres |
| `product_width_cm` | FLOAT | Width in centimetres |

---

## olist_sellers_dataset

Seller dimension. One row per seller.

| Column | Type | Description |
|--------|------|-------------|
| `seller_id` | STRING (PK) | Unique seller identifier |
| `seller_zip_code_prefix` | STRING (FK) | 5-digit ZIP, links to olist_geolocation_dataset |
| `seller_city` | STRING | Seller city name |
| `seller_state` | STRING | 2-letter Brazilian state code |

---

## olist_geolocation_dataset

Geographic dimension. Maps ZIP codes to lat/lng. Multiple rows per ZIP (averages needed).

| Column | Type | Description |
|--------|------|-------------|
| `geolocation_zip_code_prefix` | STRING (PK) | 5-digit ZIP code prefix |
| `geolocation_lat` | FLOAT | Latitude |
| `geolocation_lng` | FLOAT | Longitude |
| `geolocation_city` | STRING | City name |
| `geolocation_state` | STRING | 2-letter state code |

**Key note:** This table has ~1M rows because each ZIP has multiple lat/lng entries (different streets). To join cleanly, use `AVG(lat)` and `AVG(lng)` GROUP BY zip_code_prefix.

---

## product_category_name_translation

Lookup table. Maps Portuguese category names to English.

| Column | Type | Description |
|--------|------|-------------|
| `product_category_name` | STRING (PK) | Portuguese category name |
| `product_category_name_english` | STRING | English translation |

---

## Entity Relationship Summary

```
olist_orders  ←→  olist_order_reviews     (order_id)
olist_orders  ←→  olist_order_payments    (order_id)
olist_orders  ←→  olist_order_items       (order_id)
olist_orders  ←→  olist_customers         (customer_id)

olist_order_items  ←→  olist_products     (product_id)
olist_order_items  ←→  olist_sellers      (seller_id)

olist_customers  ←→  olist_geolocation   (zip_code_prefix)
olist_sellers    ←→  olist_geolocation   (zip_code_prefix)

olist_products  ←→  product_category_name_translation  (product_category_name)
```
