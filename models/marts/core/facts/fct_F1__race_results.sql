{{ config(
    materialized     = 'incremental',
    unique_key       = 'race_result_key',
    on_schema_change = 'append_new_columns',
    post_hook        = "{{ f1_log_model_run() }}"
) }}

with base_results as (

    select
        r.race_result_surrogate_key                    as race_result_key,
        {{ surrogate_key(['r.race_surrogate_key', 'r.driver_surrogate_key']) }}
                                                       as race_driver_key,

        r.race_result_id,

        r.race_surrogate_key                           as race_key,
        r.driver_surrogate_key                         as driver_key,
        r.constructor_surrogate_key                    as constructor_key,
        r.race_status_surrogate_key                    as status_key,

        r.race_id,
        r.driver_id,
        r.constructor_id,
        r.race_status_id,

        r.car_number,
        r.race_grid_position,
        r.race_position,
        r.race_position_label,
        r.race_final_position_order,
        r.race_points,
        r.race_laps_completed,
        r.race_duration_milliseconds,

        r.race_fastest_lap,
        r.race_fastest_lap_rank,
        r.race_fastest_lap_time_milliseconds,
        r.race_fastest_lap_top_speed,
        r.is_inconsistent_fastest_lap,

        r.duplicate_race_result_count,
        r.ingestion_timestamp

    from {{ ref('stg_F1__results') }} r
    {% if var('f1_use_incremental', true) and is_incremental() %}
        where {{ f1_incremental_filter('ingestion_timestamp') }}
    {% endif %}
),

with_race_circuit_season as (

    select
        br.*,
        ra.season_year,
        ra.circuit_id,
        c.circuit_key,
        se.season_key

    from base_results br
    left join {{ ref('dim_F1__races') }} ra
        on br.race_key = ra.race_key
    left join {{ ref('dim_F1__circuits') }} c
        on ra.circuit_id = c.circuit_id
    left join {{ ref('dim_F1__seasons') }} se
        on ra.season_year = se.season_year
),

with_driver_constructor_status as (

    select
        wrcs.*,
        s.status_description,

        case
            -- 1) Terminó la carrera
            when upper(s.status_description) = 'FINISHED'
              or s.status_description like '+%Lap%'
            then 'FINISHED'

            -- 2) Descalificación / exclusión técnica
            when upper(s.status_description) in (
                    'DISQUALIFIED',
                    'EXCLUDED',
                    'UNDERWEIGHT'
                )
            then 'DISQUALIFIED'

            -- 3) No llegó a correr / no quedó clasificado
            when upper(s.status_description) in (
                    'DID NOT PREQUALIFY',
                    'DID NOT QUALIFY',
                    '107% RULE',
                    'WITHDREW',
                    'ILLNESS',
                    'INJURED',
                    'INJURY',
                    'DRIVER UNWELL',
                    'DRIVER UNWELL.',
                    'EYE INJURY',
                    'EYE PROBLEM',
                    'SAFETY',
                    'SAFETY CONCERNS'
                )
            then 'NOT_STARTED_OR_CLASSIFIED'

            -- 4)  Abandono en carrera (DNF)
            else 'DNF'
        end as status_outcome

    from with_race_circuit_season wrcs
    left join {{ ref('dim_F1__status') }} s
        on wrcs.status_key = s.status_key
),

with_lap_aggregates as (

    select
        wdcs.*,
        la.laps_from_lap_times,
        la.best_lap_time_milliseconds,
        la.avg_lap_time_milliseconds,
        la.total_lap_time_milliseconds,
        la.best_lap_position,
        la.worst_lap_position,
        la.avg_lap_position,
        la.lap_match_category,
        la.lap_data_mismatch_flag
    from with_driver_constructor_status wdcs
    left join {{ ref('int_F1__lap_times_aggregated') }} la
        on wdcs.race_key   = la.race_surrogate_key
       and wdcs.driver_key = la.driver_surrogate_key
),

with_pit_stops as (

    select
        wla.*,
        ps.pit_stop_count,
        ps.best_pit_stop_duration_milliseconds,
        ps.worst_pit_stop_duration_milliseconds,
        ps.avg_pit_stop_duration_milliseconds,
        ps.total_pit_stop_duration_milliseconds,
        ps.first_pit_lap_number,
        ps.last_pit_lap_number
    from with_lap_aggregates wla
    left join {{ ref('int_F1__pit_stops_aggregated') }} ps
        on wla.race_key   = ps.race_surrogate_key
       and wla.driver_key = ps.driver_surrogate_key
),

with_qualifying as (

    select
        wp.*,
        q.qualifying_position,
        q.best_qualifying_time_milliseconds,
        q.qualifying_sessions_entered,
        q.qualified_for_q2,
        q.qualified_for_q3
    from with_pit_stops wp
    left join {{ ref('int_F1__qualifying_aggregated') }} q
        on wp.race_key   = q.race_surrogate_key
       and wp.driver_key = q.driver_surrogate_key
)

select
    -- Claves fact
    race_result_key,
    race_driver_key,

    -- Claves a dimensiones
    race_key,
    driver_key,
    constructor_key,
    status_key,
    season_key,
    circuit_key,

    -- IDs de negocio
    race_id,
    driver_id,
    constructor_id,
    race_status_id,

    -- Métricas de resultado
    car_number,                       -- degenerate dim 
    race_grid_position,
    race_position,
    race_final_position_order,
    race_points,
    race_laps_completed,
    race_duration_milliseconds,

    race_fastest_lap,
    race_fastest_lap_rank,
    race_fastest_lap_time_milliseconds,
    race_fastest_lap_top_speed,

    -- Métricas / flags de calidad
    is_inconsistent_fastest_lap,
    duplicate_race_result_count,
    lap_match_category,
    lap_data_mismatch_flag,

    -- Métricas de laps
    laps_from_lap_times,
    best_lap_time_milliseconds,
    avg_lap_time_milliseconds,
    total_lap_time_milliseconds,
    best_lap_position,
    worst_lap_position,
    avg_lap_position,

    -- Métricas de pit stops
    pit_stop_count,
    best_pit_stop_duration_milliseconds,
    worst_pit_stop_duration_milliseconds,
    avg_pit_stop_duration_milliseconds,
    total_pit_stop_duration_milliseconds,
    first_pit_lap_number,
    last_pit_lap_number,

    -- Métricas de quali
    qualifying_position,
    best_qualifying_time_milliseconds,
    qualifying_sessions_entered,
    qualified_for_q2,
    qualified_for_q3,

    -- Dim degenerada
    status_outcome,

    -- Metadata
    ingestion_timestamp

from with_qualifying
