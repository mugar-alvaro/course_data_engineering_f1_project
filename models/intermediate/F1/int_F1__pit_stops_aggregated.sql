{{ config(
    materialized      = 'incremental',
    unique_key        = ['race_surrogate_key', 'driver_surrogate_key'],
    on_schema_change  = 'append_new_columns',
    post_hook         = "{{ f1_log_model_run() }}"
) }}

with pit_stops as (
    select
        race_surrogate_key,
        race_id,
        driver_surrogate_key,
        driver_id,
        stop_number,
        lap_number,
        pit_stop_time_of_day,
        pit_stop_duration_milliseconds
    from {{ ref('stg_F1__pit_stops') }}
    {% if var('f1_use_incremental', true) and is_incremental() %}
        where {{ f1_incremental_filter('ingestion_timestamp') }}
    {% endif %}
),

aggregated_pit as (
    select
        race_surrogate_key,
        race_id,
        driver_surrogate_key,
        driver_id,
        count(*)                            as pit_stop_count,
        min(pit_stop_duration_milliseconds) as best_pit_stop_duration_milliseconds,
        max(pit_stop_duration_milliseconds) as worst_pit_stop_duration_milliseconds,
        avg(pit_stop_duration_milliseconds) as avg_pit_stop_duration_milliseconds,
        sum(pit_stop_duration_milliseconds) as total_pit_stop_duration_milliseconds,
        min(lap_number)                     as first_pit_lap_number,
        max(lap_number)                     as last_pit_lap_number
    from pit_stops
    group by 1,2,3,4
)

select
    {{ surrogate_key(['race_surrogate_key', 'driver_surrogate_key']) }} 
        as race_driver_surrogate_key,
    *
from aggregated_pit
