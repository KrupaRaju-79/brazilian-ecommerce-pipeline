-- =============================================================================
-- models/mart/mart_seller_performance.sql
-- One row per seller with revenue, order count, delivery stats, performance tier
-- Powers: Looker Studio Page 2 (Seller KPIs)
-- =============================================================================

with orders as (

    select * from {{ ref('stg_orders') }}
    where order_status = 'delivered'

),

items as (

    select * from {{ ref('stg_order_items') }}

),

-- join orders to items
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

-- get top category per seller separately
top_category_per_seller as (

    select
        seller_id,
        category_display as top_category
    from (
        select
            seller_id,
            category_display,
            count(*) as item_count,
            row_number() over (
                partition by seller_id
                order by count(*) desc
            ) as rn
        from order_items_joined
        group by seller_id, category_display
    )
    where rn = 1

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

        -- active months
        count(distinct order_month)           as active_months

    from order_items_joined
    group by seller_id, seller_state, seller_city

)

select
    s.seller_id,
    s.seller_state,
    s.seller_city,
    s.total_orders,
    s.total_items_sold,
    s.total_revenue_brl,
    s.avg_item_price_brl,
    s.total_gmv_brl,
    s.avg_delivery_delay_days,
    s.late_deliveries,
    s.late_delivery_pct,
    t.top_category,
    s.active_months,

    -- performance tier based on revenue
    case
        when s.total_revenue_brl >= 50000 then 'Platinum'
        when s.total_revenue_brl >= 10000 then 'Gold'
        when s.total_revenue_brl >= 1000  then 'Silver'
        else 'Bronze'
    end as seller_tier

from seller_agg s
left join top_category_per_seller t using (seller_id)
order by s.total_revenue_brl desc