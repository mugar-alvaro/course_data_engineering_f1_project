{{ config(
    materialized = 'incremental',
    unique_key   = 'lap_time_surrogate_key',
    on_schema_change = 'append_new_columns',
    post_hook    = "{{ f1_log_model_run() }}"
) }}

with source as (
    select
        raceId,
        driverId,
        lap,
        position,
        time,
        milliseconds,
        ingestion_timestamp
    from {{ source('F1', 'lap_times') }}
    {% if var('f1_use_incremental', true) and is_incremental() %}
        where {{ f1_incremental_filter('ingestion_timestamp') }}
    {% endif %}
),

cleaned as (
    select
        {{ surrogate_key(['raceId', 'driverId', 'lap']) }}  as lap_time_surrogate_key,
        {{ surrogate_key(['raceId']) }}                     as race_surrogate_key,
        raceId                                              as race_id,
        {{ surrogate_key(['driverId']) }}                   as driver_surrogate_key,
        driverId                                            as driver_id,
        cast(lap as number(3,0))                            as lap_number,
        cast(position as number(3,0))                       as lap_position,
        trim(time)                                          as lap_time_formatted,
        cast(milliseconds as number(10,0))                  as lap_time_milliseconds,
        ingestion_timestamp
    from source
)

select * from cleaned;
