{{ config(
    materialized = 'view'
) }}

with source as (
    select
        circuitId,
        circuitRef,
        name,
        location,
        country,
        lat,
        lng,
        alt,
        ingestion_timestamp
    from {{ source('F1', 'circuits') }}
),

cleaned as (
    select
        {{ surrogate_key(['circuitId']) }}       as circuit_surrogate_key,
        circuitId                                as circuit_id,
        trim(circuitRef)                         as circuit_referency,
        trim(name)                               as circuit_name,
        trim(location)                           as city,
        upper(trim(country))                     as country,
        cast(lat as float)                       as latitude,
        cast(lng as float)                       as longitude,
        cast(alt as number(5,0))                 as altitude_meters,
        ingestion_timestamp
    from source
)

select * from cleaned
