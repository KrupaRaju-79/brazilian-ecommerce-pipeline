-- =============================================================================
-- models/mart/mart_delivery_performance.sql
-- Delivery performance broken down by customer state
-- Powers: Looker Studio Page 4 (Delivery performance)
-- =============================================================================

with orders as (

    select * from {{ ref('stg_orders') }}
    where
        order_status = 'delivered'
        and delivery_delay_days is not null

)

select
    customer_state,

    -- volume
    count(distinct order_id)              as total_delivered_orders,

    -- delay distribution
    countif(delivery_delay_days < 0)      as early_deliveries,
    countif(delivery_delay_days = 0)      as on_time_deliveries,
    countif(delivery_delay_days > 0)      as late_deliveries,
    countif(delivery_delay_days > 7)      as very_late_deliveries,   -- more than 1 week late

    -- percentages
    round(
        safe_divide(countif(delivery_delay_days > 0), count(*)) * 100, 1
    )                                     as late_pct,
    round(
        safe_divide(countif(delivery_delay_days < 0), count(*)) * 100, 1
    )                                     as early_pct,

    -- delay stats
    round(avg(delivery_delay_days), 1)    as avg_delay_days,
    min(delivery_delay_days)              as best_delivery_days,
    max(delivery_delay_days)              as worst_delivery_days,

    -- performance grade
    case
        when round(safe_divide(countif(delivery_delay_days > 0), count(*)) * 100, 1) <= 5
            then 'Excellent'
        when round(safe_divide(countif(delivery_delay_days > 0), count(*)) * 100, 1) <= 15
            then 'Good'
        when round(safe_divide(countif(delivery_delay_days > 0), count(*)) * 100, 1) <= 25
            then 'Needs Improvement'
        else 'Poor'
    end                                   as delivery_grade

from orders
group by customer_state
order by late_pct desc
