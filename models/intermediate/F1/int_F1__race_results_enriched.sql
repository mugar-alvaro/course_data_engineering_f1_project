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
        result_surrogate_key,
        race_surrogate_key,
        race_id,
        driver_surrogate_key,
        driver_id,
        constructor_surrogate_key,
        constructor_id,
        grid_position,
        final_position,
        final_position_text,
        final_position_order,
        points,
        race_laps_completed,
        time_milliseconds,
        fastest_lap_number,
        fastest_lap_rank,
        fastest_lap_time_ms,
        fastest_lap_speed_kph,
        status_id,
        ingestion_timestamp,
        result_id
    from {{ ref('stg_F1__results') }}
),

dedup_results as (
    -- Aquí atacamos el problema de duplicados: mismo race_id + driver_id varias veces.
    -- Nos quedamos con UNA fila por (race_id, driver_id), priorizando por result_id
    -- (o, si quisieras, podrías usar otra lógica).
    select *
    from (
        select
            *,
            row_number() over (
                partition by race_id, driver_id
                order by result_id
            ) as rn,
            count(*) over (
                partition by race_id, driver_id
            ) as duplicate_result_count
        from base_results
    )
    where rn = 1
),

races as (
    select *
    from {{ ref('int_F1__races_enriched') }}
),

drivers as (
    select *
    from {{ ref('int_F1__drivers_enriched') }}
),

constructors as (
    select *
    from {{ ref('int_F1__constructors_enriched') }}
),

status_dim as (
    select
        status_id,
        status as race_status_text
    from {{ ref('stg_F1__status') }}
),

lap_agg as (
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
    r.result_surrogate_key,
    r.race_surrogate_key,
    r.driver_surrogate_key,
    r.constructor_surrogate_key,
    r.race_id,
    r.driver_id,
    r.constructor_id,
    ra.season_year,
    ra.race_round,
    ra.race_name,
    ra.race_date,
    ra.race_time_utc,
    ra.circuit_name,
    ra.circuit_location,
    ra.circuit_country,
    d.driver_reference,
    d.driver_number,
    d.driver_code,
    d.driver_forename,
    d.driver_surname,
    d.date_of_birth,
    d.driver_nationality,
    d.driver_url,
    c.constructor_reference,
    c.constructor_name,
    c.constructor_nationality,
    c.constructor_url,
    r.grid_position,
    r.final_position,
    r.final_position_text,
    r.final_position_order,
    r.points,
    r.race_laps_completed,
    r.time_milliseconds         as race_time_ms,
    r.fastest_lap_number,
    r.fastest_lap_rank,
    r.fastest_lap_time_ms,
    r.fastest_lap_speed_kph,
    s.race_status_text,
    r.duplicate_result_count,
    l.laps_from_lap_times,
    l.laps_from_results,
    l.best_lap_time_ms          as best_lap_time_ms_from_laps,
    l.avg_lap_time_ms,
    l.total_lap_time_ms,
    l.lap_match_category,
    l.lap_data_mismatch_flag,
    p.pit_stop_count,
    p.best_pit_stop_seconds,
    p.worst_pit_stop_seconds,
    p.avg_pit_stop_seconds,
    p.total_pit_stop_seconds,
    p.first_pit_lap,
    p.last_pit_lap,
    q.grid_position             as quali_grid_position,
    q.best_quali_time_ms,
    r.ingestion_timestamp
from dedup_results r
left join races         ra on r.race_id         = ra.race_id
left join drivers       d  on r.driver_id       = d.driver_id
left join constructors  c  on r.constructor_id  = c.constructor_id
left join status_dim    s  on r.status_id       = s.status_id
left join lap_agg       l  on r.race_id         = l.race_id
                           and r.driver_id      = l.driver_id
left join pit_agg       p  on r.race_id         = p.race_id
                           and r.driver_id      = p.driver_id
left join quali_agg     q  on r.race_id         = q.race_id
                           and r.driver_id      = q.driver_id
