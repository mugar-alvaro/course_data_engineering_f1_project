-- PURPOSE:
-- Este modelo intermediate enriquece las clasificaciones de pilotos (driver standings)
-- a grano race_id + driver_id, uniendo carrera y piloto.
-- Es la base para analizar la evolución del campeonato piloto a piloto.

{{ config(
    materialized = 'table',
    post_hook    = "{{ f1_log_model_run() }}"
) }}

with standings_options as (
    select
        driver_standings_surrogate_key,
        driver_standings_id,
        race_surrogate_key,
        race_id,
        driver_surrogate_key,
        driver_id,
        points,
        position,
        position_text,
        wins,
        ingestion_timestamp
    from {{ ref('stg_F1__drivers_standings') }}
),

races as (
    select *
    from {{ ref('int_F1__races_enriched') }}
),

drivers as (
    select *
    from {{ ref('int_F1__drivers_enriched') }}
)

select
    ds.driver_standings_surrogate_key,
    ds.driver_standings_id,
    ds.race_surrogate_key,
    ds.race_id,
    ds.driver_surrogate_key,
    ds.driver_id,
    ra.season_year,
    ra.race_round,
    ra.race_name,
    ra.race_date,
    d.driver_reference,
    d.driver_number,
    d.driver_code,
    d.driver_forename,
    d.driver_surname,
    d.driver_nationality,
    ds.points,
    ds.position,
    ds.position_text,
    ds.wins,
    ds.ingestion_timestamp
from standings_options
left join races  ra on ds.race_id   = ra.race_id
left join drivers d on ds.driver_id = d.driver_id
