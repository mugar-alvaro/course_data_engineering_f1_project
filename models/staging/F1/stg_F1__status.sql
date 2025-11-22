{{ config(
    materialized = 'view'
) }}

with source as (
    select
        statusId,
        status,
        ingestion_timestamp
    from {{ source('F1', 'status') }}
),

cleaned as (
    select
        {{ surrogate_key(['statusId']) }}   as status_surrogate_key,
        statusId                            as status_id,
        upper(trim(status))                 as status_description,
        ingestion_timestamp
    from source
)

select * from cleaned
