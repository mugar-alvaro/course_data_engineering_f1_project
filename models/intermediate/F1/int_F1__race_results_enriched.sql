-- PURPOSE:
-- Este modelo intermediate es la "tabla core" de resultados de carrera:
--  - Resuelve duplicados (múltiples filas para mismo race_id + driver_id).
--  - Pasa a un grano limpio: UNA FILA por PILOTO y CARRERA.
--  - Enriquecido con carrera, piloto, constructor, status, vueltas agregadas,
--    pits y qualifying.
-- Actúa como puente entre la capa Silver y la futura fact table en Gold.

{{ config(
    materialized = 'table',
    post_hook    = "{{ f1_log_model_run() }}"
) }}

with base_results as (
    select
        race_result_surrogate_key,
        race_result_id,
        race_surrogate_key,
        race_id,
        driver_surrogate_key,
        driver_id,
        constructor_surrogate_key,
        constructor_id,
        race_status_surrogate_key,
        race_status_id,
        car_number,
        race_grid_position,
        race_position,
        race_position_label,
        race_final_position_order,
        race_points,
        race_laps_completed,
        race_duration_milliseconds,
        race_fastest_lap,
        is_inconsistent_fastest_lap,
        race_fastest_lap_rank,
        race_fastest_lap_time_milliseconds,
        race_fastest_lap_top_speed,
        ingestion_timestamp
    from {{ ref('stg_F1__results') }}
),

dedup_results as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by race_id, driver_id
                order by
                    coalesce(race_points, 0) desc,
                    coalesce(race_laps_completed, 0) desc,
                    race_result_id
            ) as rn,
            count(*) over (
                partition by race_id, driver_id
            ) as duplicate_race_result_count
        from base_results
    )
    where rn = 1
),

races as (
    select *
    from {{ ref('int_F1__races_enriched') }}
),

-- OJO: ahora tiramos directamente del STG, no de INTs que ya no existen
drivers as (
    select *
    from {{ ref('stg_F1__drivers') }}
),

constructors as (
    select *
    from {{ ref('stg_F1__constructors') }}
),

status_dim as (
    select
        status_id,
        status_description
    from {{ ref('stg_F1__status') }}
),

laps_agg as (
    select *
    from {{ ref('int_F1__lap_times_aggregated') }}
),

pit_agg as (
    select *
    from {{ ref('int_F1__pit_stops_aggregated') }}
),

quali_agg as (
    select *
    from {{ ref('int_F1__qualifying_aggregated') }}
)

select
    -- keys
    r.race_result_surrogate_key,
    r.race_result_id,
    r.race_surrogate_key,
    r.driver_surrogate_key,
    r.constructor_surrogate_key,
    r.race_status_surrogate_key,

    -- natural ids
    r.race_id,
    r.driver_id,
    r.constructor_id,

    -- race
    ra.race_year,
    ra.race_round,
    ra.race_name,
    ra.race_date,
    ra.race_time_of_day,
    ra.circuit_id,
    ra.circuit_name,
    ra.city,
    ra.country,

    -- driver
    d.driver_reference,
    d.driver_number,
    d.driver_code,
    d.driver_forename,
    d.driver_surname,
    d.date_of_birth,
    d.driver_nationality,

    -- constructor
    c.constructor_referency,
    c.constructor_name,
    c.nationality as constructor_nationality,

    -- status
    s.status_description as race_status_description,

    -- race results
    r.car_number,
    r.race_grid_position,
    r.race_position,
    r.race_position_label,
    r.race_final_position_order,
    r.race_points,
    r.race_laps_completed,
    r.race_duration_milliseconds,
    r.race_fastest_lap,
    r.is_inconsistent_fastest_lap,
    r.race_fastest_lap_rank,
    r.race_fastest_lap_time_milliseconds,
    r.race_fastest_lap_top_speed,
    r.duplicate_race_result_count,

    -- lap aggregations
    la.laps_from_lap_times,
    la.laps_from_results,
    la.best_lap_time_milliseconds,
    la.avg_lap_time_milliseconds,
    la.total_lap_time_milliseconds,
    la.lap_match_category,
    la.lap_data_mismatch_flag,

    -- pit stops
    p.pit_stop_count,
    p.best_pit_stop_duration_milliseconds,
    p.worst_pit_stop_duration_milliseconds,
    p.avg_pit_stop_duration_milliseconds,
    p.total_pit_stop_duration_milliseconds,
    p.first_pit_lap_number,
    p.last_pit_lap_number,

    -- qualifying enriched
    q.qualifying_position,
    q.q1_time_milliseconds,
    q.is_anomalous_q1,
    q.q2_time_milliseconds,
    q.is_anomalous_q2,
    q.q3_time_milliseconds,
    q.is_anomalous_q3,
    q.best_qualifying_time_milliseconds,
    q.best_qualifying_session,
    q.qualifying_sessions_entered,
    q.qualified_for_q2,
    q.qualified_for_q3,

    r.ingestion_timestamp

from dedup_results r
left join races         ra on r.race_id   = ra.race_id
left join drivers       d  on r.driver_id = d.driver_id
left join constructors  c  on r.constructor_id = c.constructor_id
left join status_dim    s  on r.race_status_id = s.status_id
left join laps_agg      la on r.race_id   = la.race_id
                           and r.driver_id = la.driver_id
left join pit_agg       p  on r.race_id   = p.race_id
                           and r.driver_id = p.driver_id
left join quali_agg     q  on r.race_id   = q.race_id
                           and r.driver_id = q.driver_id;
