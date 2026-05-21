-- =============================================================================
-- models/staging/stg_geolocation.sql
-- Already deduplicated by Beam (1M rows → ~19k unique ZIPs)
-- Source: staging.geolocation_deduped (written by Beam pipeline)
-- =============================================================================

with source as (

    select * from {{ source('staging', 'geolocation_deduped') }}

),

cleaned as (

    select
        zip_code_prefix,
        avg_lat          as latitude,
        avg_lng          as longitude,
        geolocation_city as city,
        geolocation_state as state

    from source
    where
        zip_code_prefix is not null
        and avg_lat     is not null
        and avg_lng     is not null

)

select * from cleaned
