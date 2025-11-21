{{ config(
    materialized = 'incremental',
    unique_key   = 'qualify_surrogate_key',
    post_hook    = "{{ f1_log_model_run() }}"
) }}

with source as (
    select
        qualifyId,
        raceId,
        driverId,
        constructorId,
        number,
        position,
        q1,
        q2,
        q3,
        ingestion_timestamp
    from {{ source('F1', 'qualifying') }}
    {% if var('f1_use_incremental', true) and is_incremental() %}
        where {{ f1_incremental_filter('ingestion_timestamp') }}
    {% endif %}
),

cleaned as (
    select
        {{ surrogate_key(['qualifyId']) }}          as qualify_surrogate_key,
        qualifyId                                   as qualify_id,
        {{ surrogate_key(['raceId']) }}             as race_surrogate_key,
        raceId                                      as race_id,
        {{ surrogate_key(['driverId']) }}           as driver_surrogate_key,
        driverId                                    as driver_id,
        {{ surrogate_key(['constructorId']) }}      as constructor_surrogate_key,
        constructorId                               as constructor_id,
        cast(number as number(3,0))                 as car_number,
        cast(position as number(3,0))               as qualifying_position,
        trim(q1)                                    as q1_time_text,
        trim(q2)                                    as q2_time_text,
        trim(q3)                                    as q3_time_text,
        ingestion_timestamp
    from source
)

select * from cleaned;
