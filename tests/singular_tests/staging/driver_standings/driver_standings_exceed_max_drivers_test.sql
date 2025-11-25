select
    race_id,
    count(*) as drivers_in_standings
from {{ ref('stg_F1__driver_standings') }}
group by 1
having count(*) > 30
