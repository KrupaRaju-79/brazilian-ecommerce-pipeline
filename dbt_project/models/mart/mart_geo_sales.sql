-- =============================================================================
-- models/mart/mart_geo_sales.sql
-- Revenue and order volume by Brazilian state — feeds the geo map in Looker Studio
-- Powers: Looker Studio Page 3 (Customer geo map)
-- =============================================================================

with orders as (

    select * from {{ ref('stg_orders') }}
    where order_status in ('delivered', 'shipped')

),

items as (

    select * from {{ ref('stg_order_items') }}

),

geo as (

    select * from {{ ref('stg_geolocation') }}

),

-- order-level totals
order_totals as (

    select
        o.order_id,
        o.customer_state,
        o.customer_zip_code_prefix,
        sum(i.total_item_value) as order_value

    from orders o
    left join items i using (order_id)
    group by o.order_id, o.customer_state, o.customer_zip_code_prefix

),

-- state-level aggregation
state_agg as (

    select
        customer_state                        as state,
        count(distinct order_id)              as total_orders,
        round(sum(order_value), 2)            as total_gmv_brl,
        round(avg(order_value), 2)            as avg_order_value_brl,
        count(distinct customer_zip_code_prefix) as distinct_zips

    from order_totals
    group by customer_state

),

-- get avg lat/lng per state from geolocation
state_geo as (

    select
        state,
        round(avg(latitude),  4) as state_lat,
        round(avg(longitude), 4) as state_lng

    from geo
    group by state

)

select
    s.state,
    s.total_orders,
    s.total_gmv_brl,
    s.avg_order_value_brl,
    s.distinct_zips,
    g.state_lat,
    g.state_lng,

    -- rank by GMV for dashboard sorting
    rank() over (order by s.total_gmv_brl desc) as gmv_rank

from state_agg s
left join state_geo g on s.state = g.state
order by s.total_gmv_brl desc
