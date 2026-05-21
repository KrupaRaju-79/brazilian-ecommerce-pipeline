-- =============================================================================
-- models/mart/mart_order_kpis.sql
-- Monthly order KPIs — the primary dashboard source
-- Powers: Looker Studio Page 1 (Order trends)
-- =============================================================================

with orders as (

    select * from {{ ref('stg_orders') }}

),

items as (

    select * from {{ ref('stg_order_items') }}

),

payments as (

    select * from {{ ref('stg_payments') }}

),

-- total value per order from items
order_values as (

    select
        order_id,
        round(sum(price), 2)              as total_revenue,
        round(sum(freight_value), 2)      as total_freight,
        round(sum(total_item_value), 2)   as total_order_value,
        count(distinct product_id)        as distinct_products,
        count(*)                          as total_items

    from items
    group by order_id

),

-- total payment per order
order_payments as (

    select
        order_id,
        round(sum(payment_value), 2)      as total_payment_value,
        count(distinct payment_type)      as payment_methods_used,
        max(payment_installments)         as max_installments,
        round(avg(payment_installments), 1) as avg_installments

    from payments
    group by order_id

),

-- join everything together at order grain first
order_grain as (

    select
        o.order_id,
        o.order_month,
        o.order_year,
        o.order_month_num,
        o.order_date,
        o.order_status,
        o.customer_state,
        o.delivery_delay_days,
        o.is_late_delivery,

        coalesce(ov.total_revenue,      0) as total_revenue,
        coalesce(ov.total_freight,      0) as total_freight,
        coalesce(ov.total_order_value,  0) as total_order_value,
        coalesce(ov.distinct_products,  0) as distinct_products,
        coalesce(ov.total_items,        0) as total_items,

        coalesce(op.total_payment_value,  0) as total_payment_value,
        coalesce(op.payment_methods_used, 1) as payment_methods_used,
        coalesce(op.max_installments,     1) as max_installments,
        coalesce(op.avg_installments,     1) as avg_installments

    from orders o
    left join order_values   ov on o.order_id = ov.order_id
    left join order_payments op on o.order_id = op.order_id
    where o.order_status = 'delivered'

),

-- aggregate to monthly grain
monthly as (

    select
        order_month,
        order_year,
        order_month_num,

        -- volume
        count(distinct order_id)                          as total_orders,
        count(distinct customer_state)                    as states_served,

        -- revenue
        round(sum(total_revenue), 2)                      as gmv_brl,
        round(sum(total_freight), 2)                      as total_freight_brl,
        round(avg(total_order_value), 2)                  as aov_brl,
        round(sum(total_items), 0)                        as total_items_sold,

        -- delivery performance
        countif(is_late_delivery = true)                  as late_orders,
        countif(is_late_delivery = false)                 as on_time_orders,
        round(avg(delivery_delay_days), 1)                as avg_delivery_delay_days,
        round(
            safe_divide(
                countif(is_late_delivery = true),
                count(distinct order_id)
            ) * 100, 1
        )                                                 as late_delivery_pct,

        -- payment behaviour
        round(avg(avg_installments), 1)                   as avg_installments,
        round(avg(max_installments), 1)                   as avg_max_installments

    from order_grain
    group by order_month, order_year, order_month_num
    order by order_year, order_month_num

)

select * from monthly
