-- PURPOSE:
-- Este modelo intermediate existe para asegurar que tenemos UN SOLO REGISTRO por driver_id
-- (grano piloto) y un punto único donde, si quieres, puedes enriquecer con más atributos
-- externos (campeonatos, altura, etc.) sin tocar la capa staging.

{{ config(
    materialized = 'table',
    post_hook    = "{{ f1_log_model_run() }}"
) }}

with drivers as (
    select
        driver_surrogate_key,
        driver_id,
        driver_reference,
        driver_number,
        driver_code,
        driver_forename,
        driver_surname,
        date_of_birth,
        driver_nationality,
        driver_url,
        ingestion_timestamp
    from {{ ref('stg_F1__drivers') }}
),

deduplicated as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by driver_id
                order by ingestion_timestamp desc
            ) as rn
        from drivers
    )
    where rn = 1
)

select
    driver_surrogate_key,
    driver_id,
    driver_reference,
    driver_number,
    driver_code,
    driver_forename,
    driver_surname,
    date_of_birth,
    driver_nationality,
    driver_url,
    ingestion_timestamp
from deduplicated
