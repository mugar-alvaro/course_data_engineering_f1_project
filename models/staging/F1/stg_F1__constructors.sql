{{ config(
    materialized = 'view'
) }}

with source as (
    select
        constructorId,
        constructorRef,
        name,
        nationality,
        ingestion_timestamp
    from {{ source('F1', 'constructors') }}
),

cleaned as (
    select
        {{ surrogate_key(['constructorId']) }}     as constructor_surrogate_key,
        constructorId                               as constructor_id,
        trim(constructorRef)                        as constructor_referency,
        trim(name)                                  as constructor_name,
        upper(trim(nationality))                    as nationality,
        ingestion_timestamp
    from source
)

select * from cleaned
