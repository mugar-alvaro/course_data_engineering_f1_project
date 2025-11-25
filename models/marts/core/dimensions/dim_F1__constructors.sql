{{ config(
    materialized = 'table'
) }}

with source as (
    select
        constructor_surrogate_key,
        constructor_id,
        constructor_referency,
        constructor_name,
        nationality
    from {{ ref('stg_F1__constructors') }}
),

business_enriched as (
    select
        constructor_surrogate_key                       as constructor_key,
        constructor_id,
        constructor_referency                           as constructor_reference,
        constructor_name,
        nationality                                     as constructor_nationality,
        constructor_name || ' (' || nationality || ')'  as constructor_display_name
    from source
)

select *
from business_enriched
