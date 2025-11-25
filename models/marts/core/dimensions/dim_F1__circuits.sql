{{ config(
    materialized = 'table'
) }}

with source as (
    select
        circuit_surrogate_key,
        circuit_id,
        circuit_referency as circuit_reference,
        circuit_name,
        city,
        country,
        latitude,
        longitude,
        altitude_meters
    from {{ ref('stg_F1__circuits') }}
),

business_enriched as (
    select
        circuit_surrogate_key as circuit_key,
        circuit_id,
        circuit_reference,
        circuit_name,
        circuit_name || ' (' || city || ', ' || country || ')' as circuit_display_name,
        city,
        country,
        latitude,
        longitude,
        altitude_meters,
        case
            when latitude >= 0 then 'NORTH'
            else 'SOUTH'
        end as hemisphere,

        case
            when altitude_meters is null then 'UNKNOWN'
            when altitude_meters < 200 then 'LOW'
            when altitude_meters between 200 and 800 then 'MEDIUM'
            else 'HIGH'
        end as altitude_category,

        case
            when altitude_meters >= 1000 then true
            else false
        end as is_high_altitude_circuit
    from source
)

select *
from business_enriched
