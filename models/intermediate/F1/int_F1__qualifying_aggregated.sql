-- PURPOSE:
-- Este modelo intermediate deja la clasificación (qualifying) a grano
-- "PILOTO y CARRERA (race_id + driver_id)" y calcula el mejor tiempo de quali
-- consolidando Q1/Q2/Q3. Se usa después para enriquecer resultados de carrera.

{{ config(
    materialized = 'table',
    post_hook    = "{{ f1_log_model_run() }}"
) }}

with q as (
    select
        race_surrogate_key,
        race_id,
        driver_surrogate_key,
        driver_id,
        constructor_surrogate_key,
        constructor_id,
        grid_position,
        q1_milliseconds,
        q2_milliseconds,
        q3_milliseconds
    from {{ ref('stg_F1__qualifying') }}
),

aggregated_q as (
    select
        race_surrogate_key,
        race_id,
        driver_surrogate_key,
        driver_id,
        constructor_surrogate_key,
        constructor_id,
        grid_position,
        least(
            nullif(q1_milliseconds, 0),
            nullif(q2_milliseconds, 0),
            nullif(q3_milliseconds, 0)
        ) as best_quali_time_ms
    from q
)

select *
from aggregated_q
