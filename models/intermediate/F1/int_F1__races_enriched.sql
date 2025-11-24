-- PURPOSE:
-- Este modelo intermediate existe para tener una "dimensión de carrera" ya enriquecida
-- con información de circuito y temporada a GRANO race_id.

{{ config(
    materialized = 'table',
    post_hook    = "{{ f1_log_model_run() }}"
) }}

with races as (
    select
        race_surrogate_key,
        race_id,
        season_year,
        race_round,
        race_name,
        race_date,
        race_time_utc,
        circuit_id,
        race_url,
        ingestion_timestamp
    from {{ ref('stg_F1__races') }}
),

circuits as (
    select
        circuit_surrogate_key,
        circuit_id,
        circuit_name,
        location,
        country,
        lat,
        lng,
        alt,
        circuit_url
    from {{ ref('stg_F1__circuits') }}
),

seasons as (
    select
        season_year,
        season_url
    from {{ ref('stg_F1__seasons') }}
)

select
    r.race_surrogate_key,
    r.race_id,
    r.season_year,
    r.race_round,
    r.race_name,
    r.race_date,
    r.race_time_utc,
    c.circuit_surrogate_key,
    c.circuit_name,
    c.location           as circuit_location,
    c.country            as circuit_country,
    c.lat,
    c.lng,
    c.alt,
    s.season_url,
    c.circuit_url,
    r.race_url,
    r.ingestion_timestamp
from races r
left join circuits c
    on r.circuit_id = c.circuit_id
left join seasons s
    on r.season_year = s.season_year
