-- =============================================================================
-- models/staging/stg_orders.sql
-- Cleans and types the orders_enriched table from staging layer
-- Source: staging.orders_enriched (written by Beam pipeline)
-- =============================================================================

with source as (

    select * from {{ source('staging', 'orders_enriched') }}

),

cleaned as (

    select
        -- keys
        order_id,
        customer_id,
        customer_unique_id,

        -- status
        order_status,

        -- timestamps — cast to proper TIMESTAMP
        cast(order_purchase_timestamp      as timestamp) as order_purchase_timestamp,
        cast(order_approved_at             as timestamp) as order_approved_at,
        cast(order_delivered_carrier_date  as timestamp) as order_delivered_carrier_date,
        cast(order_delivered_customer_date as timestamp) as order_delivered_customer_date,
        cast(order_estimated_delivery_date as timestamp) as order_estimated_delivery_date,

        -- derived delivery metrics (computed by Beam)
        delivery_delay_days,
        is_late_delivery,

        -- customer geography (joined by Beam)
        customer_state,
        customer_city,
        customer_zip_code_prefix,

        -- derived date parts for easier grouping in marts
        date(cast(order_purchase_timestamp as timestamp))               as order_date,
        format_timestamp('%Y-%m', cast(order_purchase_timestamp as timestamp)) as order_month,
        extract(year  from cast(order_purchase_timestamp as timestamp)) as order_year,
        extract(month from cast(order_purchase_timestamp as timestamp)) as order_month_num

    from source
    where
        order_id     is not null
        and customer_id is not null
        -- only keep valid statuses
        and order_status in (
            'delivered', 'shipped', 'canceled',
            'invoiced', 'processing', 'unavailable',
            'created', 'approved'
        )

)

select * from cleaned
