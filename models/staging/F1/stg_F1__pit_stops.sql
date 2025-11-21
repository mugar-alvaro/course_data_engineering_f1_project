{{ config(
    materialized = 'incremental',
    unique_key   = 'pit_stop_surrogate_key',
    on_schema_change = 'append_new_columns',
    post_hook    = "{{ f1_log_model_run() }}"
) }}

with source as (
    select
        raceId,
        driverId,
        stop,
        lap,
        time,
        duration,
        milliseconds,
        ingestion_timestamp
    from {{ source('F1', 'pit_stops') }}
    {% if var('f1_use_incremental', true) and is_incremental() %}
        where {{ f1_incremental_filter('ingestion_timestamp') }}
    {% endif %}
),

cleaned as (
    select
        {{ surrogate_key(['raceId', 'driverId', 'stop']) }}  as pit_stop_surrogate_key,
        {{ surrogate_key(['raceId']) }}                      as race_surrogate_key,
        raceId                                               as race_id,
        {{ surrogate_key(['driverId']) }}                    as driver_surrogate_key,
        driverId                                             as driver_id,
        cast(stop as number(3,0))                            as stop_number,
        cast(lap as number(3,0))                             as lap_number,
        cast(time as time)                                   as pit_stop_time_of_day,
        cast(duration as number(10,3))                       as pit_stop_duration_seconds,
        cast(milliseconds as number(10,0))                   as pit_stop_duration_ms,
        ingestion_timestamp
    from source
)

select * from cleaned;
