select
    race_id,
    driver_id,
    count(*) as row_count
from {{ ref('stg_F1__sprint_results') }}
group by 1, 2
having count(*) > 1
