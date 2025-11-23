select race_id, driver_id, count(*)
from {{ ref('stg_F1__lap_times') }}
group by 1,2
having count(*) < 1
