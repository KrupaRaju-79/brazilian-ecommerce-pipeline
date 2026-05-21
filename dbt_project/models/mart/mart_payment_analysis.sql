-- =============================================================================
-- models/mart/mart_payment_analysis.sql
-- Payment method breakdown — GMV, instalment behaviour, order counts
-- Powers: Looker Studio Page 1 (Payment method filter)
-- =============================================================================

with payments as (

    select * from {{ ref('stg_payments') }}

),

orders as (

    select order_id, order_month, order_year, customer_state
    from {{ ref('stg_orders') }}
    where order_status = 'delivered'

),

joined as (

    select
        p.order_id,
        p.payment_type,
        p.payment_value,
        p.payment_installments,
        p.is_instalment_purchase,
        o.order_month,
        o.order_year,
        o.customer_state

    from payments p
    inner join orders o using (order_id)

)

select
    payment_type,
    order_month,
    order_year,

    -- volume
    count(distinct order_id)                  as total_orders,

    -- revenue
    round(sum(payment_value), 2)              as total_payment_value_brl,
    round(avg(payment_value), 2)              as avg_payment_value_brl,

    -- instalment behaviour (Brazil-specific insight)
    countif(is_instalment_purchase = true)    as instalment_orders,
    round(avg(payment_installments), 1)       as avg_installments,
    max(payment_installments)                 as max_installments,

    -- share of total (window function)
    round(
        safe_divide(
            count(distinct order_id),
            sum(count(distinct order_id)) over (partition by order_month)
        ) * 100, 1
    )                                         as pct_of_monthly_orders

from joined
group by payment_type, order_month, order_year
order by order_year, order_month, total_orders desc
