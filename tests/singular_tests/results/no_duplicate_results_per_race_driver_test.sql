{{ config(enabled = false) }}

select
    race_id,
    driver_id,
    count(*) as row_count
from {{ ref('stg_F1__results') }}
group by 1, 2
having count(*) > 1
