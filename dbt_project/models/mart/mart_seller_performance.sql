-- =============================================================================
-- models/mart/mart_seller_performance.sql
-- One row per seller with revenue, order count, avg review score
-- Powers: Looker Studio Page 2 (Seller KPIs)
-- =============================================================================

with orders as (

    select * from {{ ref('stg_orders') }}
    where order_status = 'delivered'

),

items as (

    select * from {{ ref('stg_order_items') }}

),

-- join orders to items to get seller + order context
order_items_joined as (

    select
        i.seller_id,
        i.seller_state,
        i.seller_city,
        i.order_id,
        i.price,
        i.freight_value,
        i.total_item_value,
        i.category_display,
        o.delivery_delay_days,
        o.is_late_delivery,
        o.order_month

    from items i
    inner join orders o using (order_id)

),

-- aggregate per seller
seller_agg as (

    select
        seller_id,
        seller_state,
        seller_city,

        -- volume
        count(distinct order_id)              as total_orders,
        count(*)                              as total_items_sold,

        -- revenue
        round(sum(price), 2)                  as total_revenue_brl,
        round(avg(price), 2)                  as avg_item_price_brl,
        round(sum(total_item_value), 2)       as total_gmv_brl,

        -- delivery performance
        round(avg(delivery_delay_days), 1)    as avg_delivery_delay_days,
        countif(is_late_delivery = true)      as late_deliveries,
        round(
            safe_divide(
                countif(is_late_delivery = true),
                count(distinct order_id)
            ) * 100, 1
        )                                     as late_delivery_pct,

        -- top category (most revenue)
        ( select category_display
          from unnest(array_agg(
              struct(total_item_value as val, category_display as cat)
              order by val desc limit 1))
        )                                     as top_category,

        -- active months
        count(distinct order_month)           as active_months

    from order_items_joined
    group by seller_id, seller_state, seller_city

)

select
    seller_id,
    seller_state,
    seller_city,
    total_orders,
    total_items_sold,
    total_revenue_brl,
    avg_item_price_brl,
    total_gmv_brl,
    avg_delivery_delay_days,
    late_deliveries,
    late_delivery_pct,
    top_category,
    active_months,

    -- performance tier based on revenue
    case
        when total_revenue_brl >= 50000 then 'Platinum'
        when total_revenue_brl >= 10000 then 'Gold'
        when total_revenue_brl >= 1000  then 'Silver'
        else 'Bronze'
    end as seller_tier

from seller_agg
order by total_revenue_brl desc
