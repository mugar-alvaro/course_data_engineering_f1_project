{{ config(
    materialized        = 'incremental',
    unique_key          = 'race_result_surrogate_key',
    on_schema_change    = 'append_new_columns',
    post_hook           = "{{ f1_log_model_run() }}"
) }}

with source as (
    select
        resultId,
        raceId,
        driverId,
        constructorId,
        number,
        grid,
        position,
        positionText,
        positionOrder,
        points,
        laps,
        milliseconds,
        fastestLap,
        rank,
        fastestLapTime,
        fastestLapSpeed,
        statusId,
        ingestion_timestamp
    from {{ source('F1', 'results') }}
    {% if var('f1_use_incremental', true) and is_incremental() %}
        where {{ f1_incremental_filter('ingestion_timestamp') }}
    {% endif %}
),

cleaned as (
    select
        {{ surrogate_key(['resultId']) }}                    as race_result_surrogate_key,
        resultId                                             as race_result_id,
        {{ surrogate_key(['raceId']) }}                      as race_surrogate_key,
        raceId                                               as race_id,
        {{ surrogate_key(['driverId']) }}                    as driver_surrogate_key,
        driverId                                             as driver_id,
        {{ surrogate_key(['constructorId']) }}               as constructor_surrogate_key,
        constructorId                                        as constructor_id,
        {{ surrogate_key(['statusId']) }}                    as race_status_surrogate_key,
        statusId                                             as race_status_id,
        cast(number as number(3,0))                          as car_number,
        cast(grid as number(3,0))                            as race_grid_position,
        cast(position as number(3,0))                        as race_position,
        trim(positionText)                                   as race_position_label,
        cast(positionOrder as number(3,0))                   as race_final_position_order,
        cast(points as number(3,1))                          as race_points,
        cast(laps as number(3,0))                            as race_laps_completed,
        cast(milliseconds as number(20,0))                   as race_duration_milliseconds,
        cast(fastestLap as number(3,0))                      as race_fastest_lap,
        cast(rank as number(3,0))                            as race_fastest_lap_rank,
        {{ f1_time_to_ms('fastestLapTime') }}::number(15,0)  as race_fastest_lap_time_milliseconds,
        cast(fastestLapSpeed as number(10,3))                as race_fastest_lap_top_speed,
        ingestion_timestamp
    from source
)

select * from cleaned