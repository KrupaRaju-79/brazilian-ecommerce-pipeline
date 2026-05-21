-- =============================================================================
-- models/staging/stg_order_items.sql
-- Cleans order_items_enriched — adds total_item_value derived column
-- Source: staging.order_items_enriched (written by Beam pipeline)
-- =============================================================================

with source as (

    select * from {{ source('staging', 'order_items_enriched') }}

),

cleaned as (

    select
        -- keys
        order_id,
        order_item_id,
        product_id,
        seller_id,

        -- timestamps
        cast(shipping_limit_date as timestamp) as shipping_limit_date,

        -- financials
        price,
        freight_value,

        -- derived: total cost of this line item
        round(coalesce(price, 0) + coalesce(freight_value, 0), 2) as total_item_value,

        -- seller geography (joined by Beam)
        seller_state,
        seller_city,

        -- product category (joined by Beam)
        product_category_name,
        product_category_name_english,

        -- use English name if available, else Portuguese
        coalesce(
            nullif(product_category_name_english, ''),
            nullif(product_category_name, ''),
            'unknown'
        ) as category_display

    from source
    where
        order_id   is not null
        and product_id is not null
        and seller_id  is not null
        and price      is not null

)

select * from cleaned
