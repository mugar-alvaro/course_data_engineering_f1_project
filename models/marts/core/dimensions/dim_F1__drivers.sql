{{ config(
    materialized = 'table'
) }}

with source as (
    select
        driver_surrogate_key,
        driver_id,
        driver_reference,
        driver_number,
        driver_code,
        driver_forename,
        driver_surname,
        date_of_birth,
        driver_nationality
    from {{ ref('stg_F1__drivers') }}
),

business_enriched as (
    select
        driver_surrogate_key                                    as driver_key,
        driver_id,
        driver_reference,
        driver_number,
        driver_code,
        driver_forename,
        driver_surname,
        driver_forename || ' ' || driver_surname                as driver_full_name,
        substr(driver_forename, 1, 1) || '. ' || driver_surname as driver_display_name,
        date_of_birth,
        year(date_of_birth)                                     as birth_year,
        floor(year(date_of_birth) / 10) * 10                    as birth_decade,
        driver_nationality,
    from source
)

select *
from business_enriched
