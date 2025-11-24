select
    race_id,
    driver_id,
    points,
    position
from {{ ref('int_F1__driver_standings_enriched') }}
where points is not null
  and points > 0
  and position is null