{{ config(
    materialized = 'table'
) }}

with source as (
    select
        season_surrogate_key,
        season_year
    from {{ ref('stg_F1__seasons') }}
),

business_enriched as (
    select
        season_surrogate_key as season_key,
        season_year,
        'Season ' || season_year           as season_name,
        floor(season_year / 10) * 10       as season_decade,
        case
            when season_year < 1990 then 'CLASSIC'
            when season_year between 1990 and 2013 then 'MODERN'
            else 'HYBRID_ERA'
        end as season_era
    from source
)

select *
from business_enriched
