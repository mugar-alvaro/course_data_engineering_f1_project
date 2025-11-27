{{ config(
    materialized      = 'incremental',
    unique_key        = ['race_surrogate_key', 'driver_surrogate_key'],
    on_schema_change  = 'append_new_columns',
    post_hook         = "{{ f1_log_model_run() }}"
) }}

with pit_stops as (

    select
        s.race_surrogate_key,
        s.race_id,
        s.driver_surrogate_key,
        s.driver_id,
        s.stop_number,
        s.lap_number,
        s.pit_stop_time_of_day,
        s.pit_stop_duration_milliseconds,
        s.ingestion_timestamp
    from {{ ref('stg_F1__pit_stops') }} as s

    {% if var('f1_use_incremental', true) and is_incremental() %}

    join (
        select
            coalesce(max(ingestion_timestamp), '1900-01-01'::timestamp) as max_ingestion_timestamp
        from {{ this }}
    ) as last_run
        on 1 = 1

    where s.ingestion_timestamp > last_run.max_ingestion_timestamp

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
        max(lap_number)                     as last_pit_lap_number,
        max(ingestion_timestamp)            as ingestion_timestamp
    from pit_stops
    group by 1,2,3,4
)

select
    {{ surrogate_key(['race_surrogate_key', 'driver_surrogate_key']) }} 
        as race_driver_surrogate_key,
    *
from aggregated_pit
