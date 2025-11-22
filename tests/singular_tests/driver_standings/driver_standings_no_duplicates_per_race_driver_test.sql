select
    race_id,
    driver_id,
    count(*) as row_count
from {{ ref('stg_F1__driver_standings') }}
group by 1, 2
having count(*) > 1
