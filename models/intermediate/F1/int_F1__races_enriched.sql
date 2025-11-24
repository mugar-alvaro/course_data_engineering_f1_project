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
        circuit_surrogate_key,
        circuit_id,
        race_year,
        race_round,
        race_name,
        race_date,
        race_time_of_day,
        fp1_session_date,
        fp1_session_time,
        fp2_session_date,
        fp2_session_time,
        fp3_session_date,
        fp3_session_time,
        quali_session_date,
        quali_session_time,
        sprint_session_date,
        sprint_session_time,
        ingestion_timestamp
    from {{ ref('stg_F1__races') }}
),

circuits as (
    select
        circuit_surrogate_key,
        circuit_id,
        circuit_referency,
        circuit_name,
        city,
        country,
        latitude,
        longitude,
        altitude_meters,
        ingestion_timestamp as circuit_ingestion_timestamp
    from {{ ref('stg_F1__circuits') }}
),

seasons as (
    select
        season_surrogate_key,
        season_year,
        ingestion_timestamp as season_ingestion_timestamp
    from {{ ref('stg_F1__seasons') }}
)

select
    r.race_surrogate_key,
    r.race_id,
    r.race_year,
    r.race_round,
    r.race_name,
    r.race_date,
    r.race_time_of_day,
    r.fp1_session_date,
    r.fp1_session_time,
    r.fp2_session_date,
    r.fp2_session_time,
    r.fp3_session_date,
    r.fp3_session_time,
    r.quali_session_date,
    r.quali_session_time,
    r.sprint_session_date,
    r.sprint_session_time,
    c.circuit_surrogate_key,
    c.circuit_id,
    c.circuit_referency,
    c.circuit_name,
    c.city,
    c.country,
    c.latitude,
    c.longitude,
    c.altitude_meters,
    s.season_surrogate_key,
    s.season_year,
    r.ingestion_timestamp
from races r
left join circuits c
    on r.circuit_id = c.circuit_id
left join seasons s
    on r.race_year = s.season_year