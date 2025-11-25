{{ config(
    materialized = 'table'
) }}

with source as (
    select
        status_surrogate_key,
        status_id,
        status_description
    from {{ ref('stg_F1__status') }}
)

select
    status_surrogate_key as status_key,
    status_id,
    status_description
from source
