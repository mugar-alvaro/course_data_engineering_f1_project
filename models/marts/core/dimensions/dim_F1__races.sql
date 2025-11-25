{{ config(
    materialized = 'table'
) }}

with source as (
    select
        race_surrogate_key,
        race_id,
        season_year,
        round_number,
        race_name,
        circuit_id,
        race_date,
        race_time,
        race_datetime_utc
    from {{ ref('stg_F1__races') }}
),

business_enriched as (
    select
        race_surrogate_key as race_key,
        race_id,
        season_year,
        round_number,
        race_name,
        season_year || ' - ' || race_name              as race_display_name,
        circuit_id,
        race_date,
        race_time,
        race_datetime_utc,
        dayname(race_date)                             as race_weekday_name,
        month(race_date)                               as race_month_number,
        monthname(race_date)                           as race_month_name
    from source
)

select *
from business_enriched
