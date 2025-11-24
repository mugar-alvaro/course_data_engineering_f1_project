-- PURPOSE:
-- Este modelo intermediate existe para tener un único registro por constructor_id
-- y unificar la información del constructor (nombre, nacionalidad, URL, etc.)
-- antes de usarlo en resultados, standings o dimensiones.

{{ config(
    materialized = 'table',
    post_hook    = "{{ f1_log_model_run() }}"
) }}

with constructors as (
    select
        constructor_surrogate_key,
        constructor_id,
        constructor_reference,
        constructor_name,
        constructor_nationality,
        constructor_url,
        ingestion_timestamp
    from {{ ref('stg_F1__constructors') }}
),

deduplicated as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by constructor_id
                order by ingestion_timestamp desc
            ) as rn
        from constructors
    )
    where rn = 1
)

select
    constructor_surrogate_key,
    constructor_id,
    constructor_reference,
    constructor_name,
    constructor_nationality,
    constructor_url,
    ingestion_timestamp
from deduplicated
