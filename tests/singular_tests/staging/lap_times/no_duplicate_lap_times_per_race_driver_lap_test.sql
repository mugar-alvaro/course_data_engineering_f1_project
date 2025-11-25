select
    race_id,
    driver_id,
    lap_number,
    count(*) as row_count
from {{ ref('stg_F1__lap_times') }}
group by 1, 2, 3
having count(*) > 1
