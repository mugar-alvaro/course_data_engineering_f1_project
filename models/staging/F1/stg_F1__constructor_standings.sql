{{ config(
    materialized = 'incremental',
    unique_key   = 'constructor_standing_surrogate_key',
    on_schema_change = 'append_new_columns',
    post_hook    = "{{ f1_log_model_run() }}"
) }}

with source as (
    select
        constructorStandingsId,
        raceId,
        constructorId,
        points,
        position,
        positionText,
        wins,
        ingestion_timestamp
    from {{ source('F1', 'constructor_standings') }}
    {% if is_incremental() %}
        where {{ f1_incremental_filter('ingestion_timestamp') }}
    {% endif %}
),

cleaned as (
    select
        {{ surrogate_key(['constructorStandingsId']) }}   as constructor_standing_surrogate_key,
        constructorStandingsId                            as constructor_standing_id,
        {{ surrogate_key(['raceId']) }}                   as race_surrogate_key,
        raceId                                            as race_id,
        {{ surrogate_key(['constructorId']) }}            as constructor_surrogate_key,
        constructorId                                     as constructor_id,
        cast(points as number(4,1))                       as constructor_points,
        cast(position as number(3,0))                     as constructor_position,
        upper(trim(positionText))                         as constructor_position_text,
        cast(wins as number(3,0))                         as constructor_wins,
        ingestion_timestamp
    from source
)

select * from cleaned;
