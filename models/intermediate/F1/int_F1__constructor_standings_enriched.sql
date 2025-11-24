-- PURPOSE:
-- Este modelo intermediate enriquece las clasificaciones de constructores (constructor standings)
-- a grano race_id + constructor_id, uniendo carrera y constructor.
-- Sirve como base para analizar la evolución del campeonato de constructores.

{{ config(
    materialized = 'table',
    post_hook    = "{{ f1_log_model_run() }}"
) }}

with standings_options as (
    select
        constructor_standings_surrogate_key,
        constructor_standings_id,
        race_surrogate_key,
        race_id,
        constructor_surrogate_key,
        constructor_id,
        points,
        position,
        position_text,
        wins,
        ingestion_timestamp
    from {{ ref('stg_F1__constructor_standings') }}
),

races as (
    select *
    from {{ ref('int_F1__races_enriched') }}
),

constructors as (
    select *
    from {{ ref('int_F1__constructors_enriched') }}
)

select
    cs.constructor_standings_surrogate_key,
    cs.constructor_standings_id,
    cs.race_surrogate_key,
    cs.race_id,
    cs.constructor_surrogate_key,
    cs.constructor_id,
    ra.season_year,
    ra.race_round,
    ra.race_name,
    ra.race_date,
    c.constructor_reference,
    c.constructor_name,
    c.constructor_nationality,
    cs.points,
    cs.position,
    cs.position_text,
    cs.wins,
    cs.ingestion_timestamp
from standings_options
left join races        ra on cs.race_id         = ra.race_id
left join constructors c  on cs.constructor_id  = c.constructor_id
