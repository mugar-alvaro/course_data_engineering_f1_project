select
    race_id,
    count(*) as classified_drivers
from {{ ref('stg_F1__sprint_results') }}
where sprint_position is not null
  and sprint_position <> ''
group by 1
having count(*) > 22
