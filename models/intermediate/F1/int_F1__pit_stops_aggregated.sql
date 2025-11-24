-- PURPOSE:
-- Este modelo intermediate cambia el grano de "una fila por PIT STOP"
-- a "una fila por PILOTO y CARRERA (race_id + driver_id)".
-- Así concentramos todas las métricas de paradas en boxes (nº de paradas,
-- mejor/peor/media, etc.) en un único sitio para luego enriquecer resultados.

{{ config(
    materialized = 'table',
    post_hook    = "{{ f1_log_model_run() }}"
) }}

with pit_stops as (
    select
        race_surrogate_key,
        race_id,
        driver_surrogate_key,
        driver_id,
        stop_number,
        lap_number,
        time_of_day_utc,
        duration_seconds
    from {{ ref('stg_F1__pit_stops') }}
),

aggregated_pit as (
    select
        race_surrogate_key,
        race_id,
        driver_surrogate_key,
        driver_id,
        count(*)              as pit_stop_count,
        min(duration_seconds) as best_pit_stop_seconds,
        max(duration_seconds) as worst_pit_stop_seconds,
        avg(duration_seconds) as avg_pit_stop_seconds,
        sum(duration_seconds) as total_pit_stop_seconds,
        min(lap_number)       as first_pit_lap,
        max(lap_number)       as last_pit_lap
    from pit_stops
    group by 1,2,3,4
)

select *
from aggregated_pit
