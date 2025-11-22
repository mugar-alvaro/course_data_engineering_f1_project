select
    race_id,
    driver_id,
    stop_number,
    count(*) as row_count
from {{ ref('stg_F1__pit_stops') }}
group by 1, 2, 3
having count(*) > 1
