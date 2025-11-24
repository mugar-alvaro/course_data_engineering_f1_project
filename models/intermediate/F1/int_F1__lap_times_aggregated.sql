-- PURPOSE:
-- Este modelo intermediate cambia el grano de "una fila por VUELTA" a
-- "una fila por PILOTO y CARRERA (race_id + driver_id)".
-- Además, compara el número de vueltas registradas en lap_times con las vueltas
-- reportadas en results, categorizando los mismatches (match, small_mismatch, etc.)
-- para tener un análisis claro de CALIDAD DE DATOS.

{{ config(
    materialized = 'table',
    post_hook    = "{{ f1_log_model_run() }}"
) }}

with lap_times as (
    select
        race_surrogate_key,
        race_id,
        driver_surrogate_key,
        driver_id,
        lap_number,
        milliseconds
    from {{ ref('stg_F1__lap_times') }}
),

aggregated_laps as (
    select
        race_surrogate_key,
        race_id,
        driver_surrogate_key,
        driver_id,
        count(*)          as laps_from_lap_times,
        min(milliseconds) as best_lap_time_ms,
        avg(milliseconds) as avg_lap_time_ms,
        sum(milliseconds) as total_lap_time_ms
    from lap_times
    group by 1,2,3,4
),

results as (
    select
        race_surrogate_key,
        race_id,
        driver_surrogate_key,
        driver_id,
        race_laps_completed   as laps_from_results
    from {{ ref('stg_F1__results') }}
),

joined as (

    select
        coalesce(a.race_surrogate_key, r.race_surrogate_key)       as race_surrogate_key,
        coalesce(a.race_id, r.race_id)                             as race_id,
        coalesce(a.driver_surrogate_key, r.driver_surrogate_key)   as driver_surrogate_key,
        coalesce(a.driver_id, r.driver_id)                         as driver_id,

        a.laps_from_lap_times,
        r.laps_from_results,
        a.best_lap_time_ms,
        a.avg_lap_time_ms,
        a.total_lap_time_ms,

        case
            when a.laps_from_lap_times is null and r.laps_from_results is not null then 'only_results'
            when a.laps_from_lap_times is not null and r.laps_from_results is null then 'only_lap_times'
            when a.laps_from_lap_times is null and r.laps_from_results is null then 'no_data'
            when a.laps_from_lap_times = r.laps_from_results then 'match'
            when abs(a.laps_from_lap_times - r.laps_from_results) <= 5 then 'small_mismatch'
            else 'large_mismatch'
        end as lap_match_category,

        case
            when a.laps_from_lap_times is not null
             and r.laps_from_results is not null
             and a.laps_from_lap_times != r.laps_from_results
            then true
            else false
        end as lap_data_mismatch_flag
    from aggregated_laps a
    full outer join results r
        on a.race_id   = r.race_id
       and a.driver_id = r.driver_id
)

select *
from joined
