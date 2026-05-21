-- =============================================================================
-- models/staging/stg_payments.sql
-- Cleans payments table — one row per payment method per order
-- Source: staging.payments (written by Beam pipeline)
-- =============================================================================

with source as (

    select * from {{ source('staging', 'payments') }}

),

cleaned as (

    select
        order_id,
        payment_sequential,

        -- normalise payment type labels
        case payment_type
            when 'credit_card' then 'Credit Card'
            when 'boleto'      then 'Boleto'
            when 'voucher'     then 'Voucher'
            when 'debit_card'  then 'Debit Card'
            else coalesce(payment_type, 'Unknown')
        end as payment_type,

        payment_installments,
        payment_value,

        -- flag instalment purchases (common in Brazil — up to 24x)
        payment_installments > 1 as is_instalment_purchase

    from source
    where
        order_id      is not null
        and payment_value is not null
        and payment_value > 0

)

select * from cleaned
