{{ config(
    materialized        = 'incremental',
    unique_key          = 'sprint_result_surrogate_key',
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
        fastestLapTime,
        statusId,
        ingestion_timestamp
    from {{ source('F1', 'sprint_results') }}
    {% if var('f1_use_incremental', true) and is_incremental() %}
        where {{ f1_incremental_filter('ingestion_timestamp') }}
    {% endif %}
),

cleaned as (
    select
        {{ surrogate_key(['resultId']) }}                    as sprint_result_surrogate_key,
        resultId                                             as sprint_result_id,
        {{ surrogate_key(['raceId']) }}                      as race_surrogate_key,
        raceId                                               as race_id,
        {{ surrogate_key(['driverId']) }}                    as driver_surrogate_key,
        driverId                                             as driver_id,
        {{ surrogate_key(['constructorId']) }}               as constructor_surrogate_key,
        constructorId                                        as constructor_id,
        {{ surrogate_key(['statusId']) }}                    as sprint_status_surrogate_key,
        statusId                                             as sprint_status_id,
        cast(number as number(3,0))                          as car_number,
        cast(grid as number(3,0))                            as sprint_grid_position,
        trim(position)                                       as sprint_position,
        trim(positionText)                                   as sprint_position_label,
        cast(positionOrder as number(3,0))                   as sprint_final_position_order,
        cast(points as number(3,1))                          as sprint_points,
        cast(laps as number(3,0))                            as sprint_laps_completed,
        cast(milliseconds as number(20,0))                   as sprint_duration_milliseconds,
        cast(fastestLap as number(3,0))                      as sprint_fastest_lap,
        {{ f1_time_to_ms('fastestLapTime') }}::number(15,0)  as sprint_fastest_lap_time,
        ingestion_timestamp
    from source
)

select * from cleaned
