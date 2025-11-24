select
    race_id,
    driver_id,
    count(*) as row_count
from {{ ref('int_F1__qualifying_aggregated') }}
group by
    race_id,
    driver_id
having count(*) > 1
