{{ config(
    materialized = 'table'
) }}

with source as (
    select
        year,
        url,
        ingestion_timestamp
    from {{ source('F1', 'seasons') }}
),

cleaned as (
    select
        {{ surrogate_key(['year']) }}   as season_surrogate_key,
        cast(year as number(4,0))       as season_year,
        trim(url)                       as season_url,
        ingestion_timestamp
    from source
)

select * from cleaned;
