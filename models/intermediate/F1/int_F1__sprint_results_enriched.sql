-- PURPOSE:
-- Este modelo intermediate replica la lógica de resultados de carrera,
-- pero para las CARRERAS SPRINT. De nuevo:
--  - grano limpio race_id + driver_id
--  - enriquecido con carrera, piloto, constructor y status.
-- Servirá después para construir la fact table de resultados sprint en Gold.

{{ config(
    materialized = 'table',
    post_hook    = "{{ f1_log_model_run() }}"
) }}

with base_sprint as (
    select
        sprint_result_surrogate_key,
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
        status_id,
        ingestion_timestamp,
        sprint_result_id
    from {{ ref('stg_F1__sprint_results') }}
),

dedup_sprint as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by race_id, driver_id
                order by sprint_result_id
            ) as rn,
            count(*) over (
                partition by race_id, driver_id
            ) as duplicate_sprint_result_count
        from base_sprint
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
        status as sprint_status_text
    from {{ ref('stg_F1__status') }}
)

select
    s.sprint_result_surrogate_key,
    s.race_surrogate_key,
    s.driver_surrogate_key,
    s.constructor_surrogate_key,
    s.race_id,
    s.driver_id,
    s.constructor_id,
    ra.season_year,
    ra.race_round,
    ra.race_name,
    ra.race_date,
    ra.circuit_name,
    ra.circuit_country,
    d.driver_reference,
    d.driver_number,
    d.driver_code,
    d.driver_forename,
    d.driver_surname,
    d.driver_nationality,
    c.constructor_reference,
    c.constructor_name,
    c.constructor_nationality,
    s.grid_position,
    s.final_position,
    s.final_position_text,
    s.final_position_order,
    s.points,
    s.race_laps_completed,
    s.time_milliseconds       as sprint_time_ms,
    s.fastest_lap_number,
    s.fastest_lap_rank,
    s.fastest_lap_time_ms,
    st.sprint_status_text,
    s.duplicate_sprint_result_count,
    s.ingestion_timestamp
from dedup_sprint s
left join races        ra on s.race_id          = ra.race_id
left join drivers      d  on s.driver_id        = d.driver_id
left join constructors c  on s.constructor_id   = c.constructor_id
left join status_dim   st on s.status_id        = st.status_id
