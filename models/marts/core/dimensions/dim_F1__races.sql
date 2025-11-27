{{ config(
    materialized = 'table',
    post_hook    = "{{ f1_log_model_run() }}"
) }}

with source as (

    select
        race_surrogate_key,
        race_id,
        race_year,
        race_round,
        race_name,
        circuit_surrogate_key,
        circuit_id,
        race_date,
        race_time_of_day,
        ingestion_timestamp
    from {{ ref('stg_F1__races') }}

),

business_enriched as (

    select
        race_surrogate_key                      as race_key,
        race_id,
        race_year,
        race_round                              as round_number,
        race_name,
        race_year || ' - ' || race_name         as race_display_name,
        circuit_surrogate_key,
        circuit_id,
        race_date,
        race_time_of_day,
        case
            when race_time_of_day is not null then
                to_timestamp_ntz(race_date || ' ' || race_time_of_day)
            else
                to_timestamp_ntz(race_date)
        end                                       as race_datetime_utc,

        dayname(race_date)                        as race_weekday_name,
        month(race_date)                          as race_month_number,
        monthname(race_date)                      as race_month_name

    from source
)

select *
from business_enriched
