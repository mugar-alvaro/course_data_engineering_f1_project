{{ config(
    materialized        = 'incremental',
    unique_key          = 'driver_standing_surrogate_key',
    on_schema_change    = 'append_new_columns',
    post_hook           = "{{ f1_log_model_run() }}"
) }}

with source as (
    select
        driverStandingsId,
        raceId,
        driverId,
        points,
        position,
        positionText,
        wins,
        ingestion_timestamp
    from {{ source('F1', 'drivers_standings') }}
    {% if var('f1_use_incremental', true) and is_incremental() %}
        where {{ f1_incremental_filter('ingestion_timestamp') }}
    {% endif %}
),

cleaned as (
    select
        {{ surrogate_key(['driverStandingsId']) }} as driver_standing_surrogate_key,
        driverStandingsId                          as driver_standing_id,
        {{ surrogate_key(['raceId']) }}            as race_surrogate_key,
        raceId                                     as race_id,
        {{ surrogate_key(['driverId']) }}          as driver_surrogate_key,
        driverId                                   as driver_id,
        cast(points as number(4,1))                as driver_points,
        cast(position as number(3,0))              as driver_position_number,
        trim(positionText)                         as driver_position_label,
        cast(wins as number(3,0))                  as driver_wins,
        ingestion_timestamp
    from source
)

select * from cleaned;
