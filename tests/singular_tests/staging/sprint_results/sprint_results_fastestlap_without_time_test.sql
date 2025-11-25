select *
from {{ ref('stg_F1__sprint_results') }}
where sprint_fastest_lap is not null
  and sprint_fastest_lap_time is null
