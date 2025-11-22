{{ config(
    materialized = 'view'
) }}

with source as (
    select
        constructorResultsId,
        raceId,
        constructorId,
        points,
        status,
        ingestion_timestamp
    from {{ source('F1', 'constructor_results') }}
),

cleaned as (
    select
        {{ surrogate_key(['constructorResultsId']) }}   as constructor_result_surrogate_key,
        constructorResultsId                            as constructor_result_id,
        {{ surrogate_key(['raceId']) }}                 as race_surrogate_key,
        raceId                                          as race_id,
        {{ surrogate_key(['constructorId']) }}          as constructor_surrogate_key,
        constructorId                                   as constructor_id,
        cast(points as number(3,1))                     as constructor_points,
        upper(trim(status))                             as status_code,
        ingestion_timestamp
    from source
)

select * from cleaned
